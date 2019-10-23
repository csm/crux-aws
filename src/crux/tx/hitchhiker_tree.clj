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
            [hitchhiker.tree.core :as hh]
            [hitchhiker.tree.s3 :as hhs3]
            [taoensso.nippy :as nippy]
            [crux.tx :as tx]
            [crux.codec :as c]
            [crux.tx.consumer :as consumer])
  (:import [java.io Closeable]
           [java.nio ByteBuffer]
           [java.security SecureRandom]
           [java.time ZonedDateTime ZoneOffset]
           [java.time.format DateTimeFormatter]
           [java.util Date]
           [crux.tx.hitchhiker_tree ITxLogWriteBack]))

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
  (let [last-key (hh/last-key tree)]
    (loop [k (inc last-key)
           txes txes
           tree tree]
      (if-let [tx (first txes)]
        (let [{:keys [op tx-data tx-date doc-id sub-topic]} tx]
          (case op
            :insert
            (recur (inc k) (rest txes)
                   (hh/insert tree k (consumer/->Message tx-data
                                                         nil
                                                         k
                                                         tx-date
                                                         doc-id
                                                         {::tx/sub-topic sub-topic})))))
        tree))))

(defn ->tx-id
  []
  (let [bytes (byte-array 16)
        buf (ByteBuffer/wrap bytes)]
    (.putLong buf (System/currentTimeMillis))
    (.putLong buf (.nextLong random))
    (b64/b64-encode bytes)))

(defn insert-doc! [ddb-client table-name id doc topic]
  (let [encoded-doc (nippy/freeze doc)
        tx-id (->tx-id)
        tx-date (ZonedDateTime/now ZoneOffset/UTC)
        item (as-> {"topic"     {:S "tx-log"}
                    "id"        {:S tx-id}
                    "tx-date"   {:S (.format tx-date DateTimeFormatter/ISO_OFFSET_DATE_TIME)}
                    "tx-data"   {:B encoded-doc}
                    "sub-topic" {:S topic}
                    "op"        {:S "insert"}} item
                   (if (some? id) (assoc item "doc-id" {:S id}) item))]
    (loop [delay 1000]
      (let [result (aws/invoke ddb-client {:op      :PutItem
                                           :request {:TableName table-name
                                                     :Item item}})]
        (cond (and (s/valid? ::anomalies/anomaly result)
                   (some? (:__type result))
                   (string/includes? (:__type result) "ProvisionedThrougputExceeded"))
              (Thread/sleep delay)
              (recur (min 60000 (* 2 delay)))

              (s/valid? ::anomalies/anomaly result)
              (throw (ex-info "failed to write to tx-log" {:error result}))

              :else {:index tx-id
                     :date  (-> tx-date
                                (.toInstant)
                                (.toEpochMilli)
                                (Date.))})))))

(defn evict-doc!
  [ddb-client table-name id]
  "todo, how to evict?")

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

(defrecord HitchhikerTreeTxLog [node-id state backend ddb-client table-name]
  db/TxLog
  (submit-doc [_ content-hash doc]
    (if (idx/evicted-doc? doc)
      (evict-doc! ddb-client table-name (str content-hash))
      (insert-doc! ddb-client table-name (str content-hash) doc "docs")))

  (submit-tx [this tx-ops]
    (doseq [doc (tx/tx-ops->docs tx-ops)]
      (db/submit-doc this (str (c/new-id doc)) doc))
    (let [tx-events (tx/tx-ops->tx-events tx-ops)
          tx (insert-doc! ddb-client table-name nil tx-events "txs")]
      (delay {:crux.tx/tx-id   (:index tx)
              :crux.tx/tx-time (:date tx)})))

  (new-tx-log-context [_]
    (->HitchhikerTreeTxLogContext (atom nil) (:root @state)))

  (tx-log [_ context from-tx-id]
    (open! context from-tx-id)
    ((fn step [prev-tx-id prev-txlog-id]
       (lazy-seq
         (if-let [x (async/alt!! @(:chan context)     ([v] v)
                                 (async/timeout 5000) ::timeout)]
           (cond
             (instance? Throwable x)
             (throw x)

             (not= ::timeout x)
             (cons x (step (.-key x) nil)))
           (let [txlog-id (or prev-txlog-id (-> @state :metadata :last-txlog-id) "00--")
                 result (aws/invoke ddb-client {:op :Scan
                                                :request {:TableName table-name
                                                          :ExclusiveStartKey {"topic" {:S "tx-log"}
                                                                              "id" {:S txlog-id}}
                                                          :Limit 1}})]
             ; todo there are better ways to handle this, avoid a network call per item.
             (cond (s/valid? ::anomalies/anomaly result) ; todo handle throttles here
                   (throw (ex-info "failed to scan tx-log" {:error result}))

                   (empty? result)
                   nil

                   :else
                   (let [body (nippy/thaw (-> result :tx-data :B))
                         tx-date (-> result :tx-date :S
                                      (ZonedDateTime/parse (DateTimeFormatter/ISO_OFFSET_DATE_TIME))
                                      (.toInstant)
                                      (.toEpochMilli)
                                      (Date.))]
                     (cons (consumer/->Message body
                                               nil
                                               (inc prev-tx-id)
                                               tx-date
                                               (-> result :doc-id :S)
                                               {::tx/sub-topic (-> result :sub-topic :S)})
                           (step (-> result :id :S)))))))))))

  ITxLogReload
  (reload! [_]
    (hh/go-try
      (let [item (async/<! (aws-async/invoke ddb-client {:op :GetItem
                                                         :request {:TableName table-name
                                                                   :Key {"topic" {:S "metadata"}
                                                                         "id" {:S "root"}}
                                                                   :ConsistentRead true}}))
            metadata (cond (s/valid? ::anomalies/anomaly item) ; todo throttling
                           (throw (ex-info "failed to read tree metadata" {:error item}))

                           :else
                           {:root-address (-> item :root-address :S)
                            :last-txlog-id (-> item :last-txlog-id :S)})
            root (hh/<? (hhs3/create-tree-from-root-key (:s3-bucket backend)
                                                        (:bucket backend)
                                                        (:root-address metadata)))]
        (swap! state assoc :metadata metadata :root root))))

  ITxLogWriteBack
  (write-back! [this]
    (hh/go-try
      (let [writer-state (async/<! (aws-async/invoke ddb-client {:op :GetItem
                                                                 :request {:TableName table-name
                                                                           :Key {"topic" {:S "write-back"}
                                                                                 "id" {:S "writer-state"}}
                                                                           :ConsistentRead true}}))]
        (if (s/valid? ::anomalies/anomaly writer-state) ;todo handle throttling
          (throw (ex-info "failed to read writer state" {:error writer-state}))
          (do
            (async/<! (async/timeout 1000))
            (let [token (.nextLong random)
                  ; claim the writer lock
                  claim-result (async/<!
                                 (if (empty? writer-state)
                                   (aws-async/invoke ddb-client {:op :PutItem
                                                                 :request {:TableName table-name
                                                                           :Item {"topic" {:S "write-back"}
                                                                                  "id" {:S "writer-state"}
                                                                                  "owner" {:S node-id}
                                                                                  "token" {:N (str token)}}
                                                                           :ConditionExpression "attribute_not_exists(topic) and attribute_not_exists(id)"}})
                                   (aws-async/invoke ddb-client {:op :UpdateItem
                                                                 :request {:TableName table-name
                                                                           :Key {"topic" {:S "write-back"}
                                                                                 "id" {:S "writer-state"}}
                                                                           :UpdateExpression "SET #owner = :newOwner, #token = :newToken"
                                                                           :ConditionExpression "#owner = :oldOwner and #token = :oldToken"
                                                                           :ExpressionAttributeNames {"#owner" "owner"
                                                                                                      "#token" "token"}
                                                                           :ExpressionAttributeValues {":oldOwner" (:owner writer-state)
                                                                                                       ":oldToken" (:token writer-state)
                                                                                                       ":newOwner" {:S node-id}
                                                                                                       ":newToken" {:N (str token)}}}})))]
              (if (s/valid? ::anomalies/anomaly claim-result)
                (throw (ex-info "could not claim writer lock" {:error claim-result}))
                ; unrolled async logic as a simple state machine loop.
                ; This is so we can inject updates to our writer-lock
                ; while we are performing the merge.
                (loop [timeout 500
                       phase :reload-tree
                       root nil
                       metadata nil
                       token token
                       partial-results []
                       op-chan (reload! this)]
                  (let [begin (System/currentTimeMillis)
                        result (async/alt! op-chan ([v] v)
                                           (async/timeout timeout) ::timeout)]
                    (if (= result ::timeout)
                      (let [new-token (unchecked-inc token)
                            update-state (async/<! (aws-async/invoke ddb-client {:op :UpdateItem
                                                                                 :request {:TableName table-name
                                                                                           :Key {"topic" {:S "write-back"}
                                                                                                 "id" {:S "writer-state"}}
                                                                                           :UpdateExpression "SET #token = :newToken"
                                                                                           :ConditionExpression "#owner = :me AND #token = :oldToken"
                                                                                           :ExpressionAttributeNames {"#owner" "owner"
                                                                                                                      "#token" "token"}
                                                                                           :ExpressionAttributeValues {":oldToken" {:N (str token)}
                                                                                                                       ":me" {:S node-id}
                                                                                                                       ":newToken" {:N (str new-token)}}}}))]
                        (if (s/valid? ::anomalies/anomaly update-state)
                          (throw (ex-info "failed to update writer state" {:error update-state}))
                          (recur 500 phase new-token partial-results op-chan)))
                      (let [elapsed (- (System/currentTimeMillis) begin)
                            next-timeout (max 0 (- 500 elapsed))]
                        (case phase
                          :reload-tree
                          (if (instance? Throwable result)
                            (throw result)
                            (recur next-timeout :scanning-txlog (:root result) (:metadata result) token []
                                   (aws-async/invoke ddb-client {:op :Scan
                                                                 :request {:TableName table-name
                                                                           :ExclusiveStartKey {"topic" {:S "tx-log"}
                                                                                               "id" (or (-> state deref :metadata :last-txlog-id)
                                                                                                        "00--")}}})))

                          :scanning-txlog
                          (if (s/valid? ::anomalies/anomaly result)
                            (throw (ex-info "scanning tx-log failed" {:error result}))
                            (let [messages (->> (:Items result)
                                                (map (fn [{:keys [op tx-data tx-date doc-id sub-topic]}]
                                                       {:op (-> op :S keyword)
                                                        :tx-data (-> tx-data :B nippy/thaw)
                                                        :tx-date (-> tx-date
                                                                     (ZonedDateTime/parse DateTimeFormatter/ISO_OFFSET_DATE_TIME)
                                                                     (.toInstant)
                                                                     (.toEpochMilli)
                                                                     (Date.))
                                                        :doc-id doc-id
                                                        :sub-topic sub-topic})))
                                  new-partial-results (into partial-results messages)]
                              (if (and (:LastEvaluatedKey result)
                                       (< (count new-partial-results) 4096))
                                (recur next-timeout :scanning-txlog token new-partial-results
                                       (aws-async/invoke ddb-client {:op :Scan
                                                                     :request {:TableName table-name
                                                                               :ExclusiveStartKey (:LastEvaluatedKey result)}}))
                                (recur next-timeout :merging-tree root metadata token (-> result :Items last :id :S)
                                       (async/thread
                                         (try
                                             (merge-tx-log root new-partial-results)
                                           (catch Throwable t t)))))))

                          :merging-tree
                          (if (instance? Throwable result)
                            (throw result)
                            (recur next-timeout :flushing-tree result metadata token partial-results
                                   (hh/flush-tree result backend)))

                          :flushing-tree
                          (if (instance? Throwable result)
                            (throw result)
                            (recur next-timeout :committing-metadata root metadata token []
                                   (aws-async/invoke ddb-client {:op :UpdateItem
                                                                 :request {:TableName table-name
                                                                           :Key {"topic" {:S "metadata"}
                                                                                 "id" {:S "root"}}
                                                                           :UpdateExpression "SET #root = :newRoot, #last-txid = :lastTxId"
                                                                           :ConditionExpression "#root = :curRoot"
                                                                           :ExpressionAttributeNames {"#root" "root-address"
                                                                                                      "#last-txid" "last-txlog-id"}
                                                                           :ExpressionAttributeValues {":newRoot" (hh/<? (:storage-addr root))
                                                                                                       ":curRoot" (:root-address metadata)
                                                                                                       ":lastTxId" partial-results}}})))
                          :committing-metadata
                          (when (instance? Throwable result)
                            (throw result)))))))))))))))

(defrecord HitchhikerTreeTxLogConsumerContext [tree]
  Closeable
  (close [_]))

(defrecord HitchhikerTreeTxLogConsumer [tx-log]
  consumer/PolledEventLog
  (new-event-log-context [this]
    (->HitchhikerTreeTxLogConsumerContext @(-> tx-log :state deref :root)))

  (next-events [_ context next-offset]
    (let [path (hh/<?? (hh/lookup-path (:tree context) next-offset))
          events (loop [path path
                        items []]
                   (if (and path (< (count items) 1024))
                     (let [start-node (peek path)
                           _ (assert (hh/data-node? start-node))
                           elements (-> start-node
                                        :children
                                        (subseq >= next-offset))]
                       (recur (hh/<?? (hh/right-successor (pop path)))
                              (into items (take (- 100 (count items)) elements))))
                     items))
          last-txid (or (some-> events last (.-key)) -1)
          remaining (when (< (count events) 1024)
                      (let [txlog-key (or (-> tx-log :state deref :metadata :last-txlog-id) "00--")
                            result (aws/invoke (:ddb-client tx-log) {:op :Scan
                                                                     :request {:TableName (:table-name tx-log)
                                                                               :ExclusiveStartKey {"topic" {:S "tx-log"}
                                                                                                   "id" {:S txlog-key}}}})]
                        (cond (s/valid? ::anomalies/anomaly result) ; todo handle throttles
                              (throw (ex-info "failed to scan tx-log" {:error result}))

                              :else
                              (->> (:Items result)
                                   (map-indexed (fn [i {:keys [tx-date tx-data sub-topic doc-id]}]
                                                  (consumer/->Message (nippy/thaw (:B tx-data))
                                                                      nil
                                                                      (+ i last-txid)
                                                                      (-> (:S tx-date)
                                                                          (ZonedDateTime/parse DateTimeFormatter/ISO_OFFSET_DATE_TIME)
                                                                          (.toInstant)
                                                                          (.toEpochMilli)
                                                                          (Date.))
                                                                      (:S doc-id)
                                                                      {::tx/sub-topic (:S sub-topic)})))))))]
      (into events remaining)))

  (end-offset [_]
    (let [tree-key (hh/last-key @(-> tx-log :state deref :root))]
      (loop [ddb-delay 1000]
        (let [txlog-key (or (-> tx-log :state deref :metadata :last-txlog-id) "00--")
              scan-result (aws/invoke (:ddb-client tx-log) {:op :Scan
                                                            :Select "COUNT"
                                                            :ExclusiveStartKey {"topic" {:S "tx-log"}
                                                                                "id" {:S txlog-key}}})]
          (cond (and (s/valid? ::anomalies/anomaly scan-result)
                     (some? (:__type scan-result))
                     (string/includes? (:__type scan-result) "ProvisionedThroughputExceeded"))
                (do (Thread/sleep ddb-delay)
                    (recur (min 60000 (* ddb-delay 2))))

                (s/valid? ::anomalies/anomaly scan-result)
                (throw (ex-info "could not scan tx-log" {:error scan-result}))

                :else
                (+ tree-key (get scan-result :Count 0))))))))