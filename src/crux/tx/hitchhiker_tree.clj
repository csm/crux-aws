(ns crux.tx.hitchhiker-tree
  (:require crux.tx.hitchhiker-tree.async
            b64
            [clojure.core.async :as async]
            [clojure.spec.alpha :as s]
            [clojure.string :as string]
            [cognitect.anomalies :as anomalies]
            [cognitect.aws.client.api :as aws]
            [cognitect.aws.client.api.async :as aws-async]
            [crux.db :as db]
            [crux.index :as idx]
            [crux.tx.polling :as polling]
            [hitchhiker.tree.core :as hh]
            [hitchhiker.tree.s3 :as hhs3]
            [taoensso.nippy :as nippy]
            [crux.tx :as tx]
            [crux.codec :as c]
            [crux.tx.consumer :as consumer]
            [clojure.tools.logging :as log]
            [clojure.core.cache :as cache]
            [crux.node :as n])
  (:import [java.io Closeable]
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

(defn anomaly?
  [result]
  (s/valid? ::anomalies/anomaly result))

(defn throttle?
  [result]
  (and (anomaly? result)
       (string? (:__type result))
       (string/includes? (:__type result) "ProvisionedThroughputExceeded")))

(defn conditional-failed?
  [result]
  (and (anomaly? result)
       (string? (:message result))
       (= "The conditional request failed" (:message result))))

(defprotocol IContext
  (open! [_ from-tx-id]
    "Open the transaction log context."))

(defn forward-iterator
  [iter-ch tree start-key]
  (async/go
    (try
      (let [path (hh/<? (hh/lookup-path tree start-key))]
        (loop [path path]
          (if path
            (let  [start-node (peek path)
                   _ (assert (hh/data-node? start-node))
                   elements (-> start-node
                                :children ; Get the indices of it
                                (subseq >= start-key))]
              (hh/<? (async/onto-chan iter-ch elements false))
              (recur (hh/<? (hh/right-successor (pop path)))))
            (async/close! iter-ch))))
      (catch Throwable t
        (async/put! iter-ch t
                    (fn [_]
                      (async/close! iter-ch)))))))

(defrecord HitchhikerTreeTxLogContext [chan tree]
  IContext
  (open! [_ from-tx-id]
    (let [ch (async/chan 64)]
      (if (= ch (swap! chan (fn [ch2]
                              (if (nil? ch2)
                                ch
                                ch2))))
        (forward-iterator ch tree from-tx-id)
        (throw (IllegalStateException. "context already opened")))))

  Closeable
  (close [_]
    (when-let [ch @chan]
      (when (not= ch ::closed)
        (when (compare-and-set! chan ch ::closed)
          (async/close! ch))))))

(defn merge-tx-log
  [tree txes]
  (hh/go-try
    (let [last-key (hh/last-key tree)]
      (loop [k (inc (or last-key -1))
             txes txes
             tree tree]
        (if-let [tx (first txes)]
          (let [{:keys [op tx-data tx-date doc-id sub-topic]} tx]
            (case op
              :insert
              (recur (inc k) (rest txes)
                     (hh/<? (hh/insert tree k (consumer/->Message tx-data
                                                                  nil
                                                                  k
                                                                  tx-date
                                                                  doc-id
                                                                  {::tx/sub-topic (some-> sub-topic keyword)}))))))
          tree)))))

(defn ->tx-id
  []
  (let [bytes (byte-array 16)
        buf (ByteBuffer/wrap bytes)]
    (.putLong buf (System/currentTimeMillis))
    (.putLong buf (.nextLong random))
    (b64/b64-encode bytes)))

; TODO I can't really get any better than doing an atomic
; increment on a row in dynamodb. It would be nice if we could
;

(defn generate-tx-ids
  "Generate n transaction IDs, atomically updating the dynamodb
  table with the new, latest tx-ids."
  [ddb-client table-name n]
  (async/go-loop [delay 500]
    (let [get-item (async/<! (aws-async/invoke ddb-client {:op :GetItem
                                                           :request {:TableName table-name
                                                                     :Key {"topic" {:S "metadata"}
                                                                           "id" {:S "tx-id"}}
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

(defrecord HitchhikerTreeTxLogConsumerContext [tree]
  Closeable
  (close [_]))

(defn possibly-flush-txlog
  [tx-log last-tx-id]
  (let [last-tree-id (hh/last-key (-> tx-log :state deref :root))]
    (when (> (- last-tx-id (or last-tree-id 0)) 1024)
      (log/debug :task ::possibly-flush-txlog :phase :flushing-now :last-tx-id last-tx-id :last-tree-id last-tree-id)
      (hh/<?? (write-back! tx-log)))))

(defrecord HitchhikerTreeTxLog [node-id state backend ddb-client table-name]
  db/TxLog
  (submit-doc [this content-hash doc]
    (let [tx-id (first (hh/<?? (generate-tx-ids ddb-client table-name 1)))]
      (if (idx/evicted-doc? doc)
        (evict-doc! ddb-client table-name (str content-hash))
        (insert-doc! ddb-client table-name tx-id (str content-hash) doc "docs"))
      (possibly-flush-txlog this tx-id)))

  (submit-tx [this tx-ops]
    (let [docs (tx/tx-ops->docs tx-ops)
          tx-ids (hh/<?? (generate-tx-ids ddb-client table-name (inc (count docs))))]
      (doseq [[doc tx-id] (map vector docs (butlast tx-ids))]
        (insert-doc! ddb-client table-name tx-id (str (c/new-id doc)) doc "docs"))
      (let [tx-events (tx/tx-ops->tx-events tx-ops)
            tx (insert-doc! ddb-client table-name (last tx-ids) nil tx-events "txs")]
        (possibly-flush-txlog this (last tx-ids))
        (delay {:crux.tx/tx-id   (:index tx)
                :crux.tx/tx-time (:date tx)}))))

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
                                                                           "id" {:S "root"}}
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
                                    (hh/<? (hh/b-tree (hh/->Config 1024 2048 16))))]
                        (swap! state assoc :metadata metadata :root root)))))))

  ITxLogWriteBack
  (write-back! [this]
    (hh/go-try
      (loop [delay 500]
        (let [writer-state (async/<! (aws-async/invoke ddb-client {:op :GetItem
                                                                   :request {:TableName table-name
                                                                             :Key {"topic" {:S "metadata"}
                                                                                   "id" {:S "writer-state"}}
                                                                             :ConsistentRead true}}))]
          (log/debug :task ::write-back! :phase :got-writer-state :writer-state writer-state)
          (cond
            (throttle? writer-state)
            (do (async/<! (async/timeout delay))
                (recur (min 60000 (* 2 delay))))

            (anomaly? writer-state) ;todo handle throttling
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
                                                                                    "id" {:S "writer-state"}
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
                            (do (async/<! (async/timeout delay))
                                (recur 0 phase root metadata new-token partial-results op-chan prev-aws-call (min 60000 (* 2 delay))))

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
                                                        :ScanFilter {"topic" {:AttributeValueList [{:S "tx-log"}] :ComparisonOperator "EQ"}}
                                                        :ExclusiveStartKey {"topic" {:S "tx-log"}
                                                                            "id" {:S (or (-> state deref :metadata :last-txlog-id)
                                                                                         "00--")}}}}]
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
                                                  (map (fn [{:keys [op tx-data tx-date doc-id sub-topic]}]
                                                         {:op (-> op :S keyword)
                                                          :tx-data (-> tx-data :B (ByteStreams/toByteArray) nippy/thaw)
                                                          :tx-date (-> tx-date :S
                                                                       (ZonedDateTime/parse DateTimeFormatter/ISO_OFFSET_DATE_TIME)
                                                                       (.toInstant)
                                                                       (.toEpochMilli)
                                                                       (Date.))
                                                          :doc-id (:S doc-id)
                                                          :sub-topic (some-> (:S sub-topic) keyword)})))
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
                                  (recur next-timeout :merging-tree root metadata token (-> result :Items last :id :S)
                                         (merge-tx-log root new-partial-results) nil 500))))

                            :merging-tree
                            (if (instance? Throwable result)
                              (throw result)
                              (recur next-timeout :flushing-tree result metadata token partial-results
                                     (hh/flush-tree result backend) nil 500))

                            :flushing-tree
                            (if (instance? Throwable result)
                              (throw result)
                              (let [aws-call (if (nil? (:root-address metadata))
                                               {:op :PutItem
                                                :request {:TableName table-name
                                                          :Item {"topic" {:S "metadata"}
                                                                 "id" {:S "root"}
                                                                 "root-address" {:S (str (:guid (hh/<? (:storage-addr root))))}
                                                                 "last-txlog-id" {:S partial-results}}
                                                          :ConditionExpression "attribute_not_exists(topic) AND attribute_not_exists(id)"}}
                                               {:op :UpdateItem
                                                :request {:TableName table-name
                                                          :Key {"topic" {:S "metadata"}
                                                                "id" {:S "root"}}
                                                          :UpdateExpression "SET #root = :newRoot, #lastTxid = :lastTxId"
                                                          :ConditionExpression "#root = :curRoot"
                                                          :ExpressionAttributeNames {"#root" "root-address"
                                                                                     "#lastTxid" "last-txlog-id"}
                                                          :ExpressionAttributeValues {":newRoot" {:S (str (:guid (hh/<? (:storage-addr root))))}
                                                                                      ":curRoot" {:S (:root-address metadata)}
                                                                                      ":lastTxId" {:S partial-results}}}})]
                                (recur next-timeout :committing-metadata root metadata token []
                                       (aws-async/invoke ddb-client aws-call) aws-call 500)))

                            :committing-metadata
                            (cond
                              (throttle? result)
                              (do (async/<! (async/timeout delay))
                                  (recur next-timeout phase root metadata token []
                                         (aws-async/invoke ddb-client prev-aws-call) prev-aws-call
                                         (min 60000 (* 2 delay))))

                              (anomaly? result)
                              (throw (ex-info "failed to commit tree metadata" {:error result}))))))))))))))))

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
                                          (subseq >= next-offset))]
                         (recur (hh/<?? (hh/right-successor (pop path)))
                                (into items (take (- 1024 (count items))
                                                  (map val elements)))))
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
                                                                                           "id" {:S txlog-key}}
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
                                     (map (fn [{:keys [id tx-date tx-data sub-topic doc-id]}]
                                            (consumer/->Message (nippy/thaw (ByteStreams/toByteArray (:B tx-data)))
                                                                nil
                                                                (decode-tx-id (:S id))
                                                                (-> (:S tx-date)
                                                                    (ZonedDateTime/parse DateTimeFormatter/ISO_OFFSET_DATE_TIME)
                                                                    (.toInstant)
                                                                    (.toEpochMilli)
                                                                    (Date.))
                                                                (:S doc-id)
                                                                {::tx/sub-topic (some-> (:S sub-topic) keyword)}))))))))]
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
                (or (hh/last-key (-> state deref :root)) 0)))))))

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
  (let [tx-log (->HitchhikerTreeTxLog (or node-id (str (UUID/randomUUID)))
                                      (atom {})
                                      (hhs3/->S3Backend s3-client bucket object-cache)
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

(def object-cache [make-object-cache [] ::cache-args])
(def s3-client [start-s3-client [] ::s3-client-args])
(def ddb-client [start-ddb-client [] ::ddb-client-args])
(def tx-log [start-tx-log [:s3-client :ddb-client :cache] ::tx-log-args])
(def event-log-consumer [start-event-log-consumer [:indexer :tx-log] ::event-log-consumer-args])

(def node-config {:cache object-cache
                  :s3-client s3-client
                  :ddb-client ddb-client
                  :tx-log tx-log
                  :event-log-consumer event-log-consumer})

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