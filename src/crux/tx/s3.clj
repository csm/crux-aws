(ns crux.tx.s3
  (:require [clojure.core.async :as async]
            [clojure.core.cache :as cache]
            [clojure.spec.alpha :as s]
            [clojure.string :as string]
            [cognitect.anomalies :as anomalies]
            [cognitect.aws.client.api :as aws]
            [cognitect.aws.client.api.async :as aws-async]
            [crux.db :as db]
            [crux.tx :as tx]
            [crux.tx.sqs :as tx-sqs]
            [crux.codec :as c]
            [taoensso.nippy :as nippy]
            [clojure.tools.logging :as log])
  (:import [java.util Date Base64]
           [java.io Closeable]
           [com.google.common.io ByteStreams]
           [java.security MessageDigest]))

; DynamoDB -- key gid -- atomic counter for event IDs
; S3 --
;    l/ prefix, all events in insertion order
;    k/ prefix, all event keys
;      k/${SHA256(event-key)/ prefix, event IDs referenced by event-key
;        each object under that prefix a (empty) object with key == event ID.

(defn- hash-event-key
  [event-key]
  (let [hash (.digest (doto (MessageDigest/getInstance "SHA-256")
                        (.update (.getBytes event-key "UTF-8"))))]
    (.encodeToString (Base64/getUrlEncoder) hash)))

(defn insert-event!
  [s3-client bucket ddb-client table-name cache event-key v topic]
  (let [key (loop []
              (let [existing-id (aws/invoke ddb-client {:op :GetItem
                                                        :request {:TableName table-name
                                                                  :Key {"Id" {:S "gid"}}}})
                    cur-id (when-not (empty? existing-id)
                             (BigInteger. ^String (-> existing-id :Item :Index :N)))
                    id (if (nil? cur-id) BigInteger/ZERO (.add cur-id BigInteger/ONE))
                    result (if (empty? existing-id)
                             (aws/invoke ddb-client {:op :PutItem
                                                     :request {:TableName table-name
                                                               :Item {"Id" {:S "gid"}
                                                                      "Index" {:N (str id)}}
                                                               :ConditionExpression "attribute_not_exists(Id)"}})
                             (aws/invoke ddb-client {:op :UpdateItem
                                                     :request {:TableName table-name
                                                               :Key {"Id" {:S "gid"}}
                                                               :UpdateExpression "SET #Index = :n"
                                                               :ConditionExpression "#Index = :c"
                                                               :ExpressionAttributeValues {":n" {:N (str id)}
                                                                                           ":c" {:N (str cur-id)}}
                                                               :ExpressionAttributeNames {"#Index" "Index"}}}))]
                (cond (and (s/valid? ::anomalies/anomaly result)
                           (= "The conditional request failed" (:message result)))
                      (recur)

                      (s/valid? ::anomalies/anomaly result)
                      (throw (ex-info "failed to generate new ID" {:error result}))

                      :else id)))]
       (let [object-key (format "k/%s/%032x" (hash-event-key event-key) key)
             result (aws/invoke s3-client {:op :PutObject
                                           :request {:Bucket bucket
                                                     :Key object-key}})]
         (if (s/valid? ::anomalies/anomaly result)
           (throw (ex-info "failed to insert event marker" {:error result}))
           (let [object-key (format "l/%032x" key)
                 event-date (Date.)
                 v {:k event-key :v v :d event-date :t topic}
                 b (nippy/freeze v)
                 result (aws/invoke s3-client {:op      :PutObject
                                               :request {:Bucket bucket
                                                         :Key    object-key
                                                         :Body   b}})]
             (if (s/valid? ::anomalies/anomaly result)
               (throw (ex-info "failed to insert event data" {:error result}))
               (do
                 (swap! cache cache/miss object-key v)
                 {:date  event-date
                  :index key})))))))

(defn evict-docs!
  [s3-client bucket cache event-key]
  (loop [continuation-token nil]
    (let [result (aws/invoke s3-client {:op :ListObjectsV2
                                        :request {:Bucket bucket
                                                  :Prefix (str "k/" (hash-event-key event-key) \/)
                                                  :ContinuationToken continuation-token}})]
      (if (s/valid? ::anomalies/anomaly result)
        (throw (ex-info "failed to list event markers" {:error result}))
        (let [response (aws/invoke s3-client {:op :DeleteObjects
                                              :request {:Bucket bucket
                                                        :Delete {:Objects (->> (:Contents result)
                                                                               (map #(select-keys % [:Key])))
                                                                 :Quiet true}}})]
          (if (s/valid? ::anomalies/anomaly response)
            (throw (ex-info "failed to delete event markers" {:error result}))
            (let [response (aws/invoke s3-client {:op      :DeleteObjects
                                                  :request {:Bucket bucket
                                                            :Delete {:Objects (->> (:Contents result)
                                                                                   (map (fn [object]
                                                                                          {:Key (str "l/" (last (string/split (:Key object) #"/")))})))
                                                                     :Quiet true}}})]
              (when (s/valid? ::anomalies/anomaly response)
                (throw (ex-info "failed to delete log entries" {:error response}))
                (swap! cache (fn [c]
                               (loop [keys (->> (:Contents result)
                                                (map (fn [object]
                                                       (str "l/" (last (string/split (:Key object) #"/"))))))
                                      cache c]
                                 (if-let [k (first keys)]
                                   (recur (rest keys) (if (cache/has? cache k)
                                                        (cache/evict cache k)
                                                        cache))
                                   cache))))))))))))

(defrecord S3LogQueryContext [running?]
  Closeable
  (close [_] (reset! running? false)))

(defrecord S3TxLog [s3-client bucket ddb-client table-name cache]
  db/TxLog
  (submit-doc [_ content-hash doc]
    (let [id (str content-hash)]
      (if (tx/evicted-doc? doc)
        (evict-docs! s3-client bucket cache id)
        (insert-event! s3-client bucket ddb-client table-name cache id doc "docs"))))

  (submit-tx [this tx-ops]
    (doseq [doc (tx/tx-ops->docs tx-ops)]
      (db/submit-doc this (str (c/new-id doc)) doc))
    (let [tx-events (tx/tx-ops->tx-events tx-ops)
          tx (insert-event! s3-client bucket ddb-client table-name cache "txs" tx-events "txs")]
      (delay {:crux.tx/tx-id   (:index tx)
              :crux.tx/tx-time (:date tx)})))

  (new-tx-log-context [_]
    (S3LogQueryContext. (atom true)))

  (tx-log [_ tx-log-context from-tx-id]
    (let [chan (async/chan 1000)
          running? (:running? tx-log-context)]
      (async/go-loop [start-after (when (and (some? from-tx-id) (pos? from-tx-id))
                                    (->> from-tx-id dec (format "l/%032x")))
                      continuation-token nil]
        (let [response (async/<!! (aws-async/invoke s3-client {:op :ListObjectsV2
                                                               :request {:Bucket bucket
                                                                         :Prefix "l/"
                                                                         :Delimiter "/"
                                                                         :StartAfter start-after
                                                                         :ContinuationToken continuation-token}}))]
          (if (s/valid? ::anomalies/anomaly response)
            (do (async/>! chan (ex-info "failed to list objects" {:error response}))
                (async/close! chan))
            (let [result (loop [contents (:Contents response)]
                           (when-let [item (first contents)]
                             (let [index (BigInteger. ^String (last (string/split (:Key item) #"/")) 16)
                                   value (or (cache/lookup @cache (:Key item))
                                             (async/<! (aws-async/invoke s3-client {:op :GetObject
                                                                                    :request {:Bucket bucket
                                                                                              :Key (:Key item)
                                                                                              :Prefix "txs/"}})))]
                               (if (s/valid? ::anomalies/anomaly value)
                                 (let [ex (ex-info "failed to read object" {:error value})]
                                   (async/>! chan ex)
                                   (async/close! chan)
                                   ex)
                                 (let [bytes (ByteStreams/toByteArray (:Body value))
                                       value (nippy/thaw bytes)]
                                   (swap! cache cache/miss (:Key item) value)
                                   (async/>! chan {:crux.tx/tx-id index
                                                   :crux.tx/tx-time (:d value)
                                                   :crux.tx.event/tx-events (:v value)})
                                   (recur (rest contents)))))))]
              (when (and @running? (:IsTruncated response) (not (instance? Throwable result)))
                (recur nil (:NextContinuationToken response)))))))
      ((fn step []
         (lazy-seq
           (when-let [x (async/alt!! chan ([v] v)
                                     (async/timeout 5000) ::timeout)]
             (cond
               (instance? Throwable x)
               (throw x)

               (not= ::timeout x)
               (cons x (step))))))))))

(defrecord S3EventLogConsumer [s3-client bucket ddb-client table-name cache]
  crux.tx.consumer/PolledEventLog
  (new-event-log-context
    [_]
    (reify Closeable
      (close [_])))

  (next-events [_this _context next-offset]
    (let [response (aws/invoke s3-client {:op :ListObjectsV2
                                          :request {:Bucket bucket
                                                    :Prefix "l/"
                                                    :Delimiter "/"
                                                    :StartAfter (when (and (some? next-offset) (pos? next-offset))
                                                                  (format "l/%032x" (dec next-offset)))
                                                    :MaxKeys 100}})]
      (if (s/valid? ::anomalies/anomaly response)
        (throw (ex-info "failed to list objects" {:error response}))
        (loop [results []
               contents (:Contents response)]
          (if-let [entry (first contents)]
            (let [value (get (swap! cache cache/through-cache (:Key entry)
                                    #(let [object (aws/invoke s3-client {:op :GetObject
                                                                         :request {:Bucket bucket
                                                                                   :Key %}})]
                                       (if (s/valid? ::anomalies/anomaly object)
                                         (throw (ex-info "failed to get object" {:key % :error object}))
                                         (let [bytes (ByteStreams/toByteArray (:Body object))]
                                           (nippy/thaw bytes)))))
                             (:Key entry))
                  _ (log/debug :task :crux.tx.consumer/next-events :phase :read-value :value value)
                  msg (crux.tx.consumer/->Message (:v value)
                                                  nil
                                                  (BigInteger. ^String (last (string/split (:Key entry) #"/")) 16)
                                                  (:d value)
                                                  (:k value)
                                                  {::tx/sub-topic (keyword (:t value))})]
                (recur (conj results msg) (rest contents)))
            results)))))

  (end-offset [_]
    (let [response (aws/invoke ddb-client {:op :GetItem
                                           :request {:TableName table-name
                                                     :Key {"Id" {:S "gid"}}}})]
      (log/debug :task :crux.tx.consumer/end-offset :phase :got-gid :response response)
      (if (s/valid? ::anomalies/anomaly response)
        (throw (ex-info "failed to read object ID" {:error response}))
        (or (some-> response :Item :Index :N (Integer/parseInt) (inc))
            0)))))

(defn start-s3-client
  [_ {:keys [s3-client region creds]}]
  (or s3-client
      (aws/client {:api :s3 :region region :credentials-provider creds})))

(defn start-ddb-client
  [_ {:keys [ddb-client region creds]}]
  (or ddb-client
      (aws/client {:api :dynamodb :region region :credentials-provider creds})))

(defn start-sqs-client
  [_ {:keys [sqs-client region creds]}]
  (or sqs-client
      (aws/client {:api :sqs :region region :credentials-provider creds})))

(defn start-tx-log
  [x y]
  (map->S3TxLog (merge x y)))

(defn start-event-log-consumer
  [{:keys [indexer sqs-client] :as c} {:keys [queue-name] :as o}]
  (tx-sqs/start-event-log-consumer indexer (map->S3EventLogConsumer (merge c o)) nil queue-name :sqs-client sqs-client))

(defn make-object-cache
  [_ {:keys [threshold] :or {threshold 32}}]
  (atom (cache/lru-cache-factory {} :threshold threshold)))

(s/def ::s3-client (s/nilable #(satisfies? cognitect.aws.client/ClientSPI %)))
(s/def ::region string?)
(s/def ::creds (s/nilable #(satisfies? cognitect.aws.credentials/CredentialsProvider %)))
(s/def ::s3-client-args (s/keys :opt-un [::s3-client ::region ::creds]))

(s/def ::ddb-client (s/nilable #(satisfies? cognitect.aws.client/ClientSPI %)))
(s/def ::ddb-client-args (s/keys :opt-un [::ddb-client ::region ::creds]))

(s/def ::sqs-client-args (s/keys :opt-un [::sqs-client ::region ::creds]))

(s/def ::bucket string?)
(s/def ::table-name string?)
(s/def ::queue-name string?)
(s/def ::tx-log-args (s/keys :req-un [::bucket ::table-name]))
(s/def ::event-log-consumer-args (s/keys :req-un [::bucket ::table-name ::queue-name]))

(s/def ::threshold pos-int?)
(s/def ::cache-args (s/keys :opt-un [::threshold]))

(def object-cache [make-object-cache [] ::cache-args])
(def s3-client [start-s3-client [] ::s3-client-args])
(def ddb-client [start-ddb-client [] ::ddb-client-args])
(def sqs-client [start-sqs-client [] ::sqs-client-args])
(def tx-log [start-tx-log [:s3-client :ddb-client :cache] ::tx-log-args])
(def event-log-consumer [start-event-log-consumer [:indexer :s3-client :ddb-client :sqs-client :cache] ::event-log-consumer-args])

(def node-config {:cache object-cache
                  :s3-client s3-client
                  :ddb-client ddb-client
                  :sqs-client sqs-client
                  :tx-log tx-log
                  :event-log-consumer event-log-consumer})