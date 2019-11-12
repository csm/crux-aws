(ns crux.tx.hitchhiker-tree
  (:require crux.tx.hitchhiker-tree.async
            b64
            [clojure.core.async :as async]
            [clojure.spec.alpha :as s]
            [clojure.string :as string]
            [cognitect.anomalies :as anomalies]
            [cognitect.aws.client.api :as aws]
            [cognitect.aws.client.api.async :as aws-async]
            [crux.aws.util :refer :all]
            [crux.db :as db]
            [crux.index :as idx]
            [hitchhiker.tree.core :as hh]
            [hitchhiker.tree.s3 :as hhs3]
            [taoensso.nippy :as nippy]
            [crux.tx :as tx]
            [crux.codec :as c]
            [crux.tx.consumer :as consumer]
            [clojure.tools.logging :as log]
            [clojure.core.cache :as cache]
            [crux.node :as n])
  (:import [java.io Closeable OutputStream DataOutputStream]
           [java.nio ByteBuffer]
           [java.security SecureRandom]
           [java.time ZonedDateTime ZoneOffset]
           [java.time.format DateTimeFormatter]
           [java.util Date UUID]
           [com.google.common.io ByteStreams]
           [crux.tx.consumer Message]))

; Ideas:
; Write a tx log to dynamodb, after it gets to a certain size, flush
; this to the hh-tree.
; Hitchhiker tree stored on S3; a catalog node in DynamoDB is atomically
; updated for the new tree as items are added to the tree.
; Brief sketch:
; Insert op (insert/evict) into dynamodb tx log; if item count > limit:
;   Take dynamodb txlog and merge it into the current hh-tree
;   Swap root pointers in DynamoDB to the new hh-tree

; TODOS:
; delete tx-log items that have been merged into the tree in S3.
;   possibly put a TTL column on items, so they are deleted after
;   some timeout, to give clients a chance to catch up.
;   UPDATE: added clean-txlog! which adds expires fields to all
;   txlog entries that are already in the HH tree. Just need to
;   figure out when to call; after write-back!? Manually?
; need to GC old HH-tree nodes from S3.
; clients poll SQS for S3 writes, and reload their trees from there?
; or just poll root item, and reload if it changes.
; note we aren't using HH-tree write buffers, just our own tx-log
; our usage of HH-trees here doesn't require the whole tree in memory,
; right?
; what's a good tx-log size? data node size? index node size?

(def random (SecureRandom.))

(nippy/extend-freeze crux.tx.consumer.Message
  :crux.tx.consumer/Message
  [this output]
  (nippy/freeze-to-out! output (.-body this))
  (nippy/freeze-to-out! output (.-topic this))
  (nippy/freeze-to-out! output (.-message-id this))
  (nippy/freeze-to-out! output (.-message-time this))
  (nippy/freeze-to-out! output (.-key this))
  (nippy/freeze-to-out! output (.-headers this)))

(nippy/extend-thaw :crux.tx.consumer/Message
  [input]
  (let [body (nippy/thaw-from-in! input)
        topic (nippy/thaw-from-in! input)
        message-id (nippy/thaw-from-in! input)
        message-time (nippy/thaw-from-in! input)
        key (nippy/thaw-from-in! input)
        headers (nippy/thaw-from-in! input)]
    (crux.tx.consumer/->Message body topic message-id message-time key headers)))

(defn merge-tx-log
  [tree txes]
  (hh/go-try
    (loop [txes txes
           tree tree]
      (if-let [tx (first txes)]
        (let [{:keys [tx-id tx-date tx-data]} tx
              merged (loop [tx-data tx-data
                            tree tree]
                       (if-let [tx-doc (first tx-data)]
                         (recur (rest tx-data)
                                (hh/<? (hh/insert tree tx-id (consumer/->Message (:doc tx-doc)
                                                                                 nil
                                                                                 tx-id
                                                                                 tx-date
                                                                                 (:doc-id tx-doc)
                                                                                 {::tx/sub-topic (some-> tx-doc :topic keyword)}))))
                         tree))]
          (recur (rest txes) merged))
        tree))))

; TODO I can't really get any better than doing an atomic
; increment on a row in dynamodb. It would be nice if we could
; have ingest clients just throw values into dynamodb with
; pseudo-ordered keys, and then apply tx ordering at read time.
; maybe we can figure out something where docs just get thrown
; in with pseudo-order, but the tx data itself is more strict
; We kind-of do that below, since we generate all the tx-ids
; when submit-tx happens. I need to learn more about how all of
; this works.

(defn generate-tx-ids
  "Generate n transaction IDs, atomically updating the dynamodb
  table with the new, latest tx-ids."
  [ddb-client table-name n]
  (async/go-loop [delay 500]
    (let [get-item (async/<! (aws-async/invoke ddb-client {:op :GetItem
                                                           :request {:TableName table-name
                                                                     :Key {"topic" {:S "metadata"}
                                                                           "id"    {:S "tx-id"}}
                                                                     :ConsistentRead true}}))]
      (cond (throttle? get-item)
            (do (async/<! (async/timeout delay))
                (recur (min 60000 (* delay 2))))

            (anomaly? get-item)
            (ex-info "failed to read tx-id" {:error get-item})

            :else
            (let [last-txid (some-> get-item :Item :txid :N (Long/parseLong))
                  result (async/<! (if (some? last-txid)
                                     (aws-async/invoke ddb-client {:op :UpdateItem
                                                                   :request {:TableName table-name
                                                                             :Key {"topic" {:S "metadata"}
                                                                                   "id" {:S "tx-id"}}
                                                                             :UpdateExpression "SET #txid = #txid + :num"
                                                                             :ConditionExpression "#txid = :lastTxid"
                                                                             :ExpressionAttributeNames {"#txid" "txid"}
                                                                             :ExpressionAttributeValues {":num" {:N (str n)}
                                                                                                         ":lastTxid" {:N (str last-txid)}}}})
                                     (aws-async/invoke ddb-client {:op :PutItem
                                                                   :request {:TableName table-name
                                                                             :Item {"topic" {:S "metadata"}
                                                                                    "id" {:S "tx-id"}
                                                                                    "txid" {:N (str n)}}
                                                                             :ConditionExpression "attribute_not_exists(topic) AND attribute_not_exists(id)"}})))]
              (cond
                (conditional-failed? result)
                (recur 500)

                (throttle? result)
                (do (async/<! (async/timeout delay))
                    (recur (min 60000 (* 2 delay))))

                (anomaly? result)
                (ex-info "failed to update txid" {:error result})

                :else
                (range (or last-txid 0) (+ (or last-txid 0) n))))))))

(defn encode-tx-id
  [id]
  (let [b (byte-array 8)
        buf (ByteBuffer/wrap b)]
    (.putLong buf id)
    (b64/b64-encode b)))

(defn decode-tx-id
  [s]
  (let [b (b64/b64-decode s)
        buf (ByteBuffer/wrap b)]
    (.getLong buf)))

(defn insert-doc! [ddb-client table-name tx-id id doc topic]
  (let [encoded-doc (nippy/freeze doc)
        encoded-tx-id (encode-tx-id tx-id)
        tx-date (ZonedDateTime/now ZoneOffset/UTC)
        item (as-> {"topic"     {:S "tx-log"}
                    "id"        {:S encoded-tx-id}
                    "tx-date"   {:S (.format tx-date DateTimeFormatter/ISO_OFFSET_DATE_TIME)}
                    "tx-data"   {:B encoded-doc}
                    "sub-topic" {:S (str topic)}
                    "op"        {:S "insert"}} item
                   (if (some? id) (assoc item "doc-id" {:S id}) item))]
    (log/debug :task ::insert-doc! :phase :inserting :item item)
    (loop [delay 1000]
      (let [result (aws/invoke ddb-client {:op      :PutItem
                                           :request {:TableName table-name
                                                     :Item item}})]
        (cond (throttle? result)
              (do
                (Thread/sleep delay)
                (recur (min 60000 (* 2 delay))))

              (anomaly? result)
              (throw (ex-info "failed to write to tx-log" {:error result}))

              :else {:index tx-id
                     :date  (-> tx-date
                                (.toInstant)
                                (.toEpochMilli)
                                (Date.))})))))

(defn txlog-insert!
  [ddb-client table-name tx-id data]
  (hh/go-try
    (let [encoded-data (nippy/freeze data)
          encoded-tx-id (encode-tx-id tx-id)
          tx-date (ZonedDateTime/now ZoneOffset/UTC)
          item {"topic"   {:S "tx-log"}
                "id"      {:S encoded-tx-id}
                "tx-date" {:S (.format tx-date DateTimeFormatter/ISO_OFFSET_DATE_TIME)}
                "tx-data" {:B encoded-data}}]
      (loop [delay 500]
        (let [result (async/<! (aws-async/invoke ddb-client {:op :PutItem
                                                             :request {:TableName table-name
                                                                       :Item item}}))]
          (cond (throttle? result)
                (do
                  (Thread/sleep delay)
                  (recur (min 60000 (* 2 delay))))

                (anomaly? result)
                (throw (ex-info "failed to write to tx-log" {:error result}))

                :else {:index tx-id
                       :date  (-> tx-date (.toInstant) (.toEpochMilli) (Date.))}))))))

(defn evict-doc!
  [ddb-client table-name id]
  (log/warn "TODO doc eviction"))

(defprotocol ITxLogReload
  (reload! [this]
    "Reloads the tx-log. This will (1) fetch the tree metadata from DynamoDB;
    (2) load the tree referenced by this metadata; (3) swap the new tree root
    and metadata into the current state.

    Returns a promise channel that yields a value when complete."))

(defprotocol ITxLogWriteBack
  (write-back! [this]
    "Flush the current tx-log state back to durable storage; this will (1)
    merge the current DynamoDB tx-log into the current tree; (2) write the
    new tree to S3; (3) swap the new metadata config back to dynamodb.

    Returns a promise channel that yields a value when complete."))

(defprotocol ITxLogCleaner
  (clean-txlog! [this]
    "Clean up old tx-log entries. This will scan the tx-log for entries
    that have already been written to the tree, and will add an expires
    field to them, with the timestamp when the row should be deleted.
    Returns a channel that returns when the records are all marked."))

(defrecord HitchhikerTreeTxLogConsumerContext [tree]
  Closeable
  (close [_]))

(defn possibly-flush-txlog
  [tx-log last-tx-id]
  (let [last-tree-id (hh/last-key (-> tx-log :state deref :root))]
    (when (> (- last-tx-id (or last-tree-id 0)) 1024)
      (log/debug :task ::possibly-flush-txlog :phase :flushing-now :last-tx-id last-tx-id :last-tree-id last-tree-id)
      (hh/<?? (write-back! tx-log)))))

(defn estimate-frozen-size
  [v]
  (let [size (volatile! 0)
        output-stream (proxy [OutputStream] []
                        (write
                          ([b]
                           (cond (integer? b) (vswap! size inc)
                                 (bytes? b) (vswap! size + (alength b))))
                          ([_ _ l] (vswap! size + l))))
        data-output (DataOutputStream. output-stream)]
    (nippy/freeze-to-out! data-output v)
    (+ @size 4)))

; todo do dynamodb limits count bytes or base64 bytes?
; just set this limit to 256kiB for now, might be able to go higher.
(def txlog-limit (* 256 1024))

(defn package-tx-data
  [docs]
  (let [estimated-size (estimate-frozen-size docs)]
    (cond (< estimated-size txlog-limit)
          [docs]

          (< 1 (count docs))
          (let [pivot (long (/ (count docs) 2))] ; todo split on size instead?
            (vec (concat (package-tx-data (take pivot docs))
                         (package-tx-data (drop pivot docs)))))

          :else
          (throw (ex-info "overflow; single doc would overflow size limit" {:size estimated-size})))))

(defrecord HitchhikerTreeTxLog [node-id state backend ddb-client table-name]
  db/TxLog
  (submit-doc [this content-hash doc]
    (let [tx-id (first (hh/<?? (generate-tx-ids ddb-client table-name 1)))]
      (if (idx/evicted-doc? doc)
        (evict-doc! ddb-client table-name (str content-hash))
        (txlog-insert! ddb-client table-name tx-id [{:doc-id (str content-hash)
                                                     :doc doc
                                                     :topic "docs"}]))
      (possibly-flush-txlog this tx-id)))

  (submit-tx [this tx-ops]
    (let [docs (mapv #(hash-map :doc-id (str (c/new-id %)) :doc % :topic "docs")
                     (tx/tx-ops->docs tx-ops))
          tx-events [{:doc (tx/tx-ops->tx-events tx-ops) :topic "txs"}]
          tx-data (package-tx-data (into docs tx-events))
          tx-ids (hh/<?? (generate-tx-ids ddb-client table-name (count tx-data)))
          tx (loop [txes (map vector tx-ids tx-data)
                    result nil]
               (if-let [[id data] (first txes)]
                 (let [rez (txlog-insert! ddb-client table-name id data)]
                   (recur (rest txes) rez))
                 result))]
      (possibly-flush-txlog this (last tx-ids))
      (delay {:crux.tx/tx-id (:index tx)
              :crux.tx/tx-time (:date tx)})))

  (new-tx-log-context [_]
    (->HitchhikerTreeTxLogConsumerContext (:root @state)))

  (tx-log [this context from-tx-id]
    ((fn step [from-tx-id]
       (lazy-seq
         (when-let [batch (not-empty (consumer/next-events this context from-tx-id))]
           (concat batch (step (.-message-id (last batch)))))))
     from-tx-id))

  ITxLogReload
  (reload! [_]
    (hh/go-try
      (loop [delay 500]
        (let [item (async/<! (aws-async/invoke ddb-client {:op :GetItem
                                                           :request {:TableName table-name
                                                                     :Key {"topic" {:S "metadata"}
                                                                           "id"    {:S "root"}}
                                                                     :ConsistentRead true}}))]
          (cond (throttle? item)
                (do (async/<! (async/timeout delay))
                    (recur (min 60000 (* 2 delay))))

                (anomaly? item)
                (throw (ex-info "failed to read tree metadata" {:error item}))

                :else (let [metadata {:root-address (-> item :Item :root-address :S)
                                      :last-txlog-id (-> item :Item :last-txlog-id :S)}
                             root (if-let [addr (:root-address metadata)]
                                    (hh/<? (hhs3/create-tree-from-root-key backend addr))
                                    (hh/<? (hh/b-tree (hh/->Config 2048 8 2))))]
                        (swap! state assoc :metadata metadata :root root)))))))

  ITxLogWriteBack
  (write-back! [this]
    (hh/go-try
      (loop [delay 500]
        (let [writer-state (async/<! (aws-async/invoke ddb-client {:op :GetItem
                                                                   :request {:TableName table-name
                                                                             :Key {"topic" {:S "metadata"}
                                                                                   "id"    {:S "writer-state"}}
                                                                             :ConsistentRead true}}))]
          (log/debug :task ::write-back! :phase :got-writer-state :writer-state writer-state)
          (cond
            (throttle? writer-state)
            (do (async/<! (async/timeout delay))
                (recur (min 60000 (* 2 delay))))

            (anomaly? writer-state)
            (throw (ex-info "failed to read writer state" {:error writer-state}))

            :else
            (do
              (when-not (empty? writer-state)
                (async/<! (async/timeout 1000)))
              (let [token (.nextLong random)
                    ; claim the writer lock
                    claim-result (async/<!
                                   (if (empty? writer-state)
                                     (aws-async/invoke ddb-client {:op :PutItem
                                                                   :request {:TableName table-name
                                                                             :Item {"topic" {:S "metadata"}
                                                                                    "id"    {:S "writer-state"}
                                                                                    "owner" {:S node-id}
                                                                                    "token" {:N (str token)}}
                                                                             :ConditionExpression "attribute_not_exists(#topic) and attribute_not_exists(#id)"
                                                                             :ExpressionAttributeNames {"#topic" "topic"
                                                                                                        "#id" "id"}}})
                                     (aws-async/invoke ddb-client {:op :UpdateItem
                                                                   :request {:TableName table-name
                                                                             :Key {"topic" {:S "metadata"}
                                                                                   "id" {:S "writer-state"}}
                                                                             :UpdateExpression "SET #owner = :newOwner, #token = :newToken"
                                                                             :ConditionExpression "#owner = :oldOwner and #token = :oldToken"
                                                                             :ExpressionAttributeNames {"#owner" "owner"
                                                                                                        "#token" "token"}
                                                                             :ExpressionAttributeValues {":oldOwner" (-> writer-state :Item :owner)
                                                                                                         ":oldToken" (-> writer-state :Item :token)
                                                                                                         ":newOwner" {:S node-id}
                                                                                                         ":newToken" {:N (str token)}}}})))]
                (log/debug :task ::write-back! :phase :claim-attempt :result claim-result)
                (cond
                  (throttle? claim-result)
                  (do
                    (async/<! (async/timeout delay))
                    (recur (min 60000 (* 2 delay))))

                  (anomaly? claim-result)
                  (throw (ex-info "could not claim writer lock" {:error claim-result}))

                  ; unrolled async logic as a simple state machine loop.
                  ; This is so we can inject updates to our writer-lock
                  ; while we are performing the merge.
                  :else
                  (loop [timeout 500
                         phase :reload-tree
                         root nil
                         metadata nil
                         token token
                         partial-results []
                         op-chan (reload! this)
                         prev-aws-call nil
                         delay 500]
                    (log/debug :task ::write-back! :phase phase
                               :timeout timeout
                               ;:root root
                               :metadata metadata
                               :token token
                               :partial-results partial-results)
                    (let [begin (System/currentTimeMillis)
                          result (async/alt! op-chan ([v] v)
                                             (async/timeout timeout) ::timeout)]
                      (log/debug :task ::write-back! :phase phase :result result)
                      (if (= result ::timeout)
                        (let [new-token (unchecked-inc token)
                              update-state (async/<! (aws-async/invoke ddb-client {:op :UpdateItem
                                                                                   :request {:TableName table-name
                                                                                             :Key {"topic" {:S "metadata"}
                                                                                                   "id" {:S "writer-state"}}
                                                                                             :UpdateExpression "SET #token = :newToken"
                                                                                             :ConditionExpression "#owner = :me AND #token = :oldToken"
                                                                                             :ExpressionAttributeNames {"#owner" "owner"
                                                                                                                        "#token" "token"}
                                                                                             :ExpressionAttributeValues {":oldToken" {:N (str token)}
                                                                                                                         ":me" {:S node-id}
                                                                                                                         ":newToken" {:N (str new-token)}}}}))]
                          (cond
                            (throttle? update-state)
                            (do
                              (async/<! (async/timeout delay))
                              (recur 0 phase root metadata token partial-results op-chan prev-aws-call (min 60000 (* 2 delay))))

                            (anomaly? update-state)
                            (throw (ex-info "failed to update writer state" {:error update-state}))

                            :else
                            (recur 500 phase root metadata new-token partial-results op-chan prev-aws-call 500)))
                        (let [elapsed (- (System/currentTimeMillis) begin)
                              next-timeout (max 0 (- 500 elapsed))]
                          (case phase
                            :reload-tree
                            (if (instance? Throwable result)
                              (throw result)
                              (let [aws-call {:op :Scan
                                              :request {:TableName table-name
                                                        :ScanFilter {"topic" {:AttributeValueList [{:S "tx-log"}]
                                                                              :ComparisonOperator "EQ"}}
                                                        :ExclusiveStartKey {"topic" {:S "tx-log"}
                                                                            "id"    {:S (or (:last-txlog-id (:metadata result)) "0")}}}}]
                                (recur next-timeout :scanning-txlog (:root result) (:metadata result) token []
                                       (aws-async/invoke ddb-client aws-call) aws-call 500)))

                            :scanning-txlog
                            (cond
                              (throttle? result)
                              (do (async/<! (async/timeout delay))
                                  (recur next-timeout phase root metadata token partial-results
                                         (aws-async/invoke ddb-client prev-aws-call) prev-aws-call
                                         (min 60000 (* 2 delay))))

                              (anomaly? result)
                              (throw (ex-info "scanning tx-log failed" {:error result}))

                              :else
                              (let [messages (->> (:Items result)
                                                  (map (fn [{:keys [id tx-data tx-date]}]
                                                         (let [id (decode-tx-id (:S id))
                                                               tx-date (-> tx-date :S
                                                                           (ZonedDateTime/parse DateTimeFormatter/ISO_OFFSET_DATE_TIME)
                                                                           (.toInstant)
                                                                           (.toEpochMilli)
                                                                           (Date.))]
                                                           {:tx-id   id
                                                            :tx-date tx-date
                                                            :tx-data (->> tx-data
                                                                          :B
                                                                          (ByteStreams/toByteArray)
                                                                          (nippy/thaw))}))))
                                    new-partial-results (into partial-results messages)]
                                (if (and (:LastEvaluatedKey result)
                                         (< (count new-partial-results) 4096))
                                  (let [aws-call {:op :Scan
                                                  :request {:TableName table-name
                                                            :ScanFilter {"topic" {:AttributeValueList [{:S "tx-log"}]
                                                                                  :ComparisonOperator "EQ"}}
                                                            :ExclusiveStartKey (:LastEvaluatedKey result)}}]
                                    (recur next-timeout :scanning-txlog root metadata token new-partial-results
                                           (aws-async/invoke ddb-client aws-call) aws-call 500))
                                  (recur next-timeout :merging-tree root metadata token (-> new-partial-results last :tx-id)
                                         (merge-tx-log root new-partial-results) nil 500))))

                            :merging-tree
                            (if (instance? Throwable result)
                              (throw result)
                              (recur next-timeout :flushing-tree result metadata token partial-results
                                     (hh/flush-tree result backend) nil 500))

                            :flushing-tree
                            (if (instance? Throwable result)
                              (throw result)
                              (let [storage-addr (hh/<? (:storage-addr root))
                                    new-metadata {:root-address (str (:guid storage-addr))
                                                  :last-txlog-id partial-results}
                                    aws-call (if (nil? (:root-address metadata))
                                               {:op :PutItem
                                                :request {:TableName table-name
                                                          :Item {"topic" {:S "metadata"}
                                                                 "id" {:S "root"}
                                                                 "root-address" {:S (str (:guid storage-addr))}
                                                                 "last-txlog-id" {:S partial-results}}
                                                          :ConditionExpression "attribute_not_exists(topic) AND attribute_not_exists(id)"}}
                                               {:op :UpdateItem
                                                :request {:TableName table-name
                                                          :Key {"topic" {:S "metadata"}
                                                                "id" {:S "root"}}
                                                          :UpdateExpression "SET #root = :newRoot, #lastTxid = :lastTxId"
                                                          :ConditionExpression "#root = :curRoot"
                                                          :ExpressionAttributeNames {"#root"     "root-address"
                                                                                     "#lastTxid" "last-txlog-id"}
                                                          :ExpressionAttributeValues {":newRoot"  {:S (str (:guid (hh/<? (:storage-addr root))))}
                                                                                      ":curRoot"  {:S (str (:root-address metadata))}
                                                                                      ":lastTxId" {:S partial-results}}}})]
                                (recur next-timeout :committing-metadata root new-metadata token []
                                       (aws-async/invoke ddb-client aws-call) aws-call 500)))

                            :committing-metadata
                            (cond
                              (throttle? result)
                              (do
                                (async/<! (async/timeout delay))
                                (recur next-timeout phase root metadata token []
                                       (aws-async/invoke ddb-client prev-aws-call) prev-aws-call
                                       (min 60000 (* 2 delay))))

                              (anomaly? result)
                              (do
                                (async/>! (:anchor-chan backend) ::hhs3/abort)
                                (throw (ex-info "failed to commit tree metadata" {:error result})))

                              :else
                              (do
                                (async/>! (:anchor-chan backend) ::hhs3/commit)
                                (let [new-state (swap! state assoc :metadata metadata)]
                                  (recur next-timeout :cleaning-txlog (:root new-state) (:metadata new-state) token []
                                         (clean-txlog! this) nil 500))))

                            :cleaning-txlog
                            (when (instance? Throwable result)
                              (log/warn result :task ::write-back! :phase :cleaning-txlog)))))))))))))))

  consumer/PolledEventLog
  (new-event-log-context [_]
    (->HitchhikerTreeTxLogConsumerContext (:root @state)))

  (next-events [_ context next-offset]
    (log/debug :task ::consumer/next-events :phase :begin
               :next-offset next-offset)
    (let [last-key (or (hh/last-key (:tree context)) -1)
          path (hh/<?? (hh/lookup-path (:tree context) next-offset))
          events (if (<= next-offset last-key)
                   (loop [path path
                          items []]
                     (if (and path (< (count items) 1024))
                       (let [start-node (peek path)
                             _ (assert (hh/data-node? start-node))
                             elements (-> start-node
                                          :children
                                          (subseq >= next-offset))
                             new-items (loop [elements elements
                                              items items]
                                         (if (> (count items) 1024)
                                           items
                                           (if-let [batch (first elements)]
                                             (recur (rest elements) (into items (val batch)))
                                             items)))]
                         (recur (hh/<?? (hh/right-successor (pop path))) new-items))
                       items))
                   [])
          remaining (when (< (count events) 1024)
                      (loop [delay 500]
                        (let [next-offset (+ next-offset (count events))
                              txlog-key (if (pos? next-offset)
                                          (encode-tx-id (dec next-offset))
                                          "0")
                              result (aws/invoke ddb-client {:op :Scan
                                                             :request {:TableName table-name
                                                                       :ExclusiveStartKey {"topic" {:S "tx-log"}
                                                                                           "id"    {:S txlog-key}}
                                                                       :ScanFilter {"topic" {:AttributeValueList [{:S "tx-log"}]
                                                                                             :ComparisonOperator "EQ"}}}})]
                          (log/debug :task ::consumer/next-events :phase :scanned-ddb :result result)
                          (cond (throttle? result)
                                (do (async/<!! (async/timeout delay))
                                    (recur (min 60000 (* 2 delay))))

                                (anomaly? result) ; todo handle throttles
                                (throw (ex-info "failed to scan tx-log" {:error result}))

                                :else
                                (->> (:Items result)
                                     (mapcat (fn [{:keys [id tx-date tx-data]}]
                                               (let [id (decode-tx-id (:S id))
                                                     tx-date (-> tx-date :S
                                                                 (ZonedDateTime/parse DateTimeFormatter/ISO_OFFSET_DATE_TIME)
                                                                 (.toInstant)
                                                                 (.toEpochMilli)
                                                                 (Date.))]
                                                 (->> tx-data :B (ByteStreams/toByteArray) nippy/thaw
                                                      (map (fn [{:keys [doc-id doc topic]}]
                                                             (consumer/->Message doc
                                                                                 nil
                                                                                 id
                                                                                 tx-date
                                                                                 doc-id
                                                                                 {::tx/sub-topic (some-> topic keyword)}))))))))))))]
      (into events remaining)))

  (end-offset [_]
    (loop [delay 500]
      (let [query-result (aws/invoke ddb-client {:op :Query
                                                 :request {:TableName table-name
                                                           :KeyConditionExpression "#topic = :topic"
                                                           :ExpressionAttributeNames {"#topic" "topic"}
                                                           :ExpressionAttributeValues {":topic" {:S "tx-log"}}
                                                           :ScanIndexForward false
                                                           :Limit 1}})]
        (cond (and (s/valid? ::anomalies/anomaly query-result)
                   (some? (:__type query-result))
                   (string/includes? (:__type query-result) "ProvisionedThroughputExceeded"))
              (do (Thread/sleep delay)
                  (recur (min 60000 (* delay 2))))

              (s/valid? ::anomalies/anomaly query-result)
              (throw (ex-info "could not query tx-log" {:error query-result}))

              :else
              (if-let [id (some-> query-result :Items first :id :S decode-tx-id)]
                id
                (or (hh/last-key (-> state deref :root)) 0))))))

  ITxLogCleaner
  (clean-txlog! [_]
    (hh/go-try
      (when-let [end-txid (hh/last-key (:root @state))]
        (loop [start-key {"topic" {:S "tx-log"}
                          "id" {:S "0"}}
               delay 500]
          (let [scan-result (async/<! (aws-async/invoke ddb-client {:op :Scan
                                                                    :request {:TableName table-name
                                                                              :ScanFilter {"topic" {:AttributeValueList [{:S "tx-log"}] :ComparisonOperator "EQ"}
                                                                                           "id" {:AttributeValueList [{:S (encode-tx-id end-txid)}] :ComparisonOperator "LE"}}
                                                                              :ExclusiveStartKey start-key
                                                                              :AttributesToGet ["expires" "id"]}}))]
            (cond (throttle? scan-result)
                  (do (async/<! (async/timeout delay))
                      (recur start-key (min 60000 (* 2 delay))))

                  (anomaly? scan-result)
                  (ex-info "failed to scan tx-log" {:error scan-result})

                  :else
                  ; todo hard-code expire time to 1 hour, possibly make this configurable
                  (let [expires (str (long (+ (/ (System/currentTimeMillis) 1000) 3600)))]
                    (loop [items (:Items scan-result)
                           delay 500]
                      (when-let [item (first items)]
                        (let [result (when (nil? (:expires item))
                                       (async/<! (aws-async/invoke ddb-client {:op :UpdateItem
                                                                               :request {:TableName table-name
                                                                                         :Key {"topic" {:S "tx-log"}
                                                                                               "id" (:id item)}
                                                                                         :UpdateExpression "SET #expires = :expires"
                                                                                         :ExpressionAttributeNames {"#expires" "expires"}
                                                                                         :ExpressionAttributeValues {":expires" {:N expires}}}})))]
                          (cond (throttle? result)
                                (do (async/<! (async/timeout delay))
                                    (recur items (min 60000 (* 2 delay))))

                                (anomaly? result)
                                (do
                                  (log/warn :task ::clean-txlog! :phase :updating-item-expires
                                              :id (:S (:id item)) :error result)
                                  (recur (rest items) 500))

                                :else
                                (do
                                  (log/debug :task ::clean-txlog! :phase :updating-item-expires
                                             :id (:S (:id item)) :expires expires)
                                  (recur (rest items) 500))))))
                    (when-let [next-start-key (:LastEvaluatedKey scan-result)]
                      (recur next-start-key 500))))))))))

(defn start-s3-client
  [_ {s3-client ::s3-client
      region ::region
      creds ::creds}]
  (or s3-client
      (aws/client {:api :s3 :region region :credentials-provider creds})))

(defn start-ddb-client
  [_ {ddb-client ::ddb-client
      region ::region
      creds ::creds}]
  (or ddb-client
      (aws/client {:api :dynamodb :region region :credentials-provider creds})))

(defn start-tx-log
  [{:keys [s3-client ddb-client object-cache]} {node-id ::node-id
                                                bucket ::bucket
                                                table-name ::table-name}]
  (log/debug :task ::start-tx-log :phase :begin
             :node-id node-id :bucket bucket :table-name table-name)
  (let [anchor-chan (async/chan)
        anchor-mult (async/mult anchor-chan)
        tx-log (->HitchhikerTreeTxLog (or node-id (str (UUID/randomUUID)))
                                      (atom {})
                                      (hhs3/->S3Backend s3-client bucket object-cache anchor-chan anchor-mult)
                                      ddb-client
                                      table-name)]
    (hh/<?? (reload! tx-log))
    tx-log))

;(def polling-consumer #'crux.tx.polling/polling-consumer)
(defn- polling-consumer
  [running? indexer event-log-consumer sqs-client queue-name idle-sleep-ms]
  (log/debug :task ::polling-consumer :phase :begin :idle-sleep-ms idle-sleep-ms :queue-name queue-name)
  (let [queue-url (some-> sqs-client (aws/invoke {:op :GetQueueUrl :request {:QueueName queue-name}}) :QueueUrl)]
    (when-not (db/read-index-meta indexer :crux.tx-log/consumer-state)
      (db/store-index-meta
        indexer
        :crux.tx-log/consumer-state {:crux.tx/event-log {:lag 0
                                                         :next-offset 0
                                                         :time nil}}))
    (while @running?
      (let [idle? (with-open [context (consumer/new-event-log-context event-log-consumer)]
                    (let [next-offset (get-in (db/read-index-meta indexer :crux.tx-log/consumer-state) [:crux.tx/event-log :next-offset])]
                      (if-let [^Message last-message (reduce (fn [last-message ^Message m]
                                                               (log/debug :task ::polling-consumer :phase :process-message :message m)
                                                               (case (get (.headers m) :crux.tx/sub-topic)
                                                                 :docs
                                                                 (db/index-doc indexer (.key m) (.body m))
                                                                 :txs
                                                                 (db/index-tx indexer
                                                                              (.body m)
                                                                              (.message-time m)
                                                                              (.message-id m)))
                                                               m)
                                                             nil
                                                             (consumer/next-events event-log-consumer context next-offset))]
                        (let [end-offset (consumer/end-offset event-log-consumer)
                              next-offset (inc (long (.message-id last-message)))
                              lag (- end-offset next-offset)
                              _ (when (pos? lag)
                                  (log/debug "Falling behind" ::event-log "at:" next-offset "end:" end-offset))
                              consumer-state {:crux.tx/event-log
                                              {:lag lag
                                               :next-offset next-offset
                                               :time (.message-time last-message)}}]
                          (log/debug "Event log consumer state:" (pr-str consumer-state))
                          (db/store-index-meta indexer :crux.tx-log/consumer-state consumer-state)
                          false)
                        true)))]
        (log/debug :task ::polling-consumer :phase :events-processed :idle? idle?)
        (when idle?
          (if (some? queue-url)
            (let [messages (aws/invoke sqs-client {:op :ReceiveMessage
                                                   :request {:QueueUrl queue-url
                                                             :WaitTimeSeconds 20
                                                             :MaxNumberOfMessages 10}})]
              (log/debug "sqs messages:" messages)
              (doseq [m (:Messages messages)]
                (aws/invoke sqs-client {:op :DeleteMessage
                                        :request {:QueueUrl queue-url
                                                  :ReceiptHandle (:ReceiptHandle m)}})))
            (Thread/sleep idle-sleep-ms)))))))

(defn start-event-log-consumer
  [{indexer ::n/indexer tx-log ::n/tx-log} {consumer-type ::consumer-type
                                            idle-sleep-ms ::idle-sleep-ms
                                            sqs-client    ::sqs-client
                                            region        ::region
                                            creds         ::creds
                                            queue-name    ::queue-name
                                            :or {idle-sleep-ms 1000
                                                 consumer-type :polling}}]
  (log/debug :task ::start-event-log-consumer :phase :begin
             :indexer indexer :tx-log tx-log)
  (when (and (= :sqs consumer-type) (string/blank? queue-name))
    (throw (IllegalArgumentException. "missing queue-name for SQS consumer")))
  (let [running? (atom true)
        sqs-client (or sqs-client (when (= consumer-type :sqs)
                                    (aws/client {:api :sqs :region region :credentials-provider creds})))
        worker-thread (doto (Thread. #(try
                                        ; this impl copied here to use a longer idle sleep time
                                        ; every 10ms might be too much for dynamodb
                                        (polling-consumer running? indexer tx-log sqs-client queue-name idle-sleep-ms)
                                        (catch Throwable t
                                          (log/fatal t "Event log consumer threw exception, consumption has stopped:")))
                                     "crux.tx.event-log-consumer-thread")
                        (.start))]
    (reify Closeable
      (close [_]
        (reset! running? false)
        (.join worker-thread)))))

(defn make-object-cache
  [_ {threshold ::cache-threshold :or {threshold 32}}]
  (atom (cache/lru-cache-factory {} :threshold threshold)))

(s/def ::s3-client (s/nilable #(satisfies? cognitect.aws.client/ClientSPI %)))
(s/def ::region string?)
(s/def ::creds (s/nilable #(satisfies? cognitect.aws.credentials/CredentialsProvider %)))
(s/def ::s3-client-args (s/keys :opt-un [::s3-client ::region ::creds]))

(s/def ::ddb-client (s/nilable #(satisfies? cognitect.aws.client/ClientSPI %)))
(s/def ::ddb-client-args (s/keys :opt-un [::ddb-client ::region ::creds]))

(s/def ::bucket string?)
(s/def ::table-name string?)
(s/def ::queue-name string?)
(s/def ::idle-time-ms pos-int?)
(s/def ::tx-log-args (s/keys :req-un [::bucket ::table-name]))
(s/def ::event-log-consumer-args (s/keys :req-un [::idle-time-ms]))

(s/def ::threshold pos-int?)
(s/def ::cache-args (s/keys :opt-un [::threshold]))

(def topology (merge n/base-topology
                     {:object-cache {:start-fn make-object-cache
                                     :args {::cache-threshold
                                            {:doc "Max size for internal LRU object cache"
                                             :default 32
                                             :crux.config/type :crux.config/int}}}
                      :s3-client {:start-fn start-s3-client
                                  :args {::s3-client
                                         {:doc "The explicit S3 client instance"
                                          :crux.config/type [(fn [x] (s/valid? ::s3-client x))
                                                             identity]}
                                         ::region
                                         {:doc "The AWS region"
                                          :crux.config/type :crux.config/string}
                                         ::creds
                                         {:doc "AWS credentials provider"
                                          :crux.config/type [(fn [x] (s/valid? ::creds x))
                                                             identity]}}}
                      :ddb-client {:start-fn start-ddb-client
                                   :args {::ddb-client
                                          {:doc "The explicit DynamoDB client instance"
                                           :crux.config/type [(fn [x] (s/valid? ::ddb-client x))
                                                              identity]}
                                          ::region
                                          {:doc "The AWS region"
                                           :crux.config/type :crux.config/string}
                                          ::creds
                                          {:doc "AWS credentials provider"
                                           :crux.config/type [(fn [x] (s/valid? ::creds x))
                                                              identity]}}}
                      ::n/tx-log {:start-fn start-tx-log
                                  :deps [:object-cache
                                         :s3-client
                                         :ddb-client]
                                  :args {::bucket
                                         {:doc "The S3 bucket"
                                          :crux.config/required? true
                                          :crux.config/type :crux.config/string}
                                         ::table-name
                                         {:doc "The DynamoDB table name"
                                          :crux.config/required? true
                                          :crux.config/type :crux.config/string}
                                         ::node-id
                                         {:doc "This node's unique ID"
                                          :crux.config/type :crux.config/string}}}
                      ::event-log-consumer {:start-fn start-event-log-consumer
                                            :deps [::n/indexer ::n/tx-log]
                                            :args {::consumer-type
                                                   {:doc "The consumer type. Valid types are :polling, :sqs"
                                                    :crux.config/type [(fn [x] (#{:polling :sqs} x))
                                                                       identity]
                                                    :crux.config/default :polling}
                                                   ::sqs-client
                                                   {:doc "An explicit SQS client for :sqs consumers"
                                                    :crux.config/type [(fn [x] (s/valid? ::sqs-client x))
                                                                       identity]}
                                                   ::region
                                                   {:doc "The AWS region"
                                                    :crux.config/type :crux.config/string}
                                                   ::creds
                                                   {:doc "AWS credentials provider"
                                                    :crux.config/type [(fn [x] (s/valid? ::creds x))
                                                                       identity]}
                                                   ::queue-name
                                                   {:doc "SQS queue name, for :sqs consumers"
                                                    :crux.config/type :crux.config/string}
                                                   ::idle-sleep-ms
                                                   {:doc "Time to sleep between polls of DynamoDB, for :polling consumers"
                                                    :crux.config/type :crux.config/nat-int
                                                    :crux.config/default 1000}}}}))