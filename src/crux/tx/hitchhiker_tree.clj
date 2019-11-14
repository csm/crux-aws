(ns crux.tx.hitchhiker-tree
  (:require [clojure.core.async :as async]
            [clojure.core.cache :as cache]
            [clojure.spec.alpha :as s]
            [clojure.string :as string]
            [clojure.tools.logging :as log]
            [cognitect.aws.client.api :as aws]
            [crux.codec :as c]
            [crux.db :as db]
            [crux.index :as idx]
            [crux.node :as n]
            [crux.tx :as tx]
            [crux.tx.consumer :as consumer]
            [konserve.cache :as kc]
            [konserve.core :as k]
            [konserve.filestore :as kf]
            [konserve.memory :as km]
            [hitchhiker.tree :as hh]
            [hitchhiker.tree.bootstrap.konserve :as hhkons]
            [hitchhiker.tree.messaging :as hmsg]
            [hitchhiker.tree.node :as hn]
            [hitchhiker.tree.utils.async :as ha]
            [superv.async :as sv])
  (:import [crux.tx.consumer Message]
           [java.io Closeable]
           [java.time Clock Duration]
           [java.util Date UUID]))

(defn map->Message
  [{:keys [body topic message-id message-time key headers]}]
  (consumer/->Message body topic message-id message-time key headers))

; wrap message as a record so incognito/fressian serialization works
(defrecord CruxMessage [body topic message-id message-time key headers])

(def read-handlers
  {'crux.tx.hitchhiker_tree.CruxMessage
   map->CruxMessage})

(def write-handlers
  {'crux.tx.hitchhiker_tree.CruxMessage
   identity})

(extend-protocol hn/IEDNOrderable
  (type (byte-array 0))
  (-order-on-edn-types [_] (int \b)))

(defn- compare-bytes
  [b1 b2]
  (let [cmp (map #(compare (bit-and %1 0xFF) (bit-and %2 0xFF)) b1 b2)]
    (if (every? zero? cmp)
      (cond
        (> (alength b1) (alength b2))
        1
        (< (alength b1) (alength b2))
        -1
        :else
        0)
      (->> cmp (remove zero?) first))))

(extend-protocol hitchhiker.tree.key-compare/IKeyCompare
  (type (byte-array 0))
  (-compare [k1 k2]
    (if (= (hn/-order-on-edn-types k1) (hn/-order-on-edn-types k2))
      (compare-bytes k1 k2)
      (- (hn/-order-on-edn-types k2) (hn/-order-on-edn-types k1)))))

; TODO I can't really get any better than doing an atomic
; increment on a row in dynamodb. It would be nice if we could
; have ingest clients just throw values into dynamodb with
; pseudo-ordered keys, and then apply tx ordering at read time.
; maybe we can figure out something where docs just get thrown
; in with pseudo-order, but the tx data itself is more strict
; We kind-of do that below, since we generate all the tx-ids
; when submit-tx happens. I need to learn more about how all of
; this works.

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

(defn nano-clock
  ([] (nano-clock (Clock/systemUTC)))
  ([^Clock base-clock]
   (let [base-ns (System/nanoTime)
         base-instant (.instant base-clock)]
     (proxy [Clock] []
       (getZone [] (.getZone base-clock))
       (withZone [zone] (nano-clock (.withZone base-clock zone)))
       (instant []
         (.plusNanos base-instant (- (System/nanoTime) base-ns)))))))

(def ^:dynamic *clock* (nano-clock))

(defn now
  ([] (.instant *clock*)))

(defn ms
  ([start] (ms *clock* start))
  ([clock start]
   (-> (Duration/between start (.instant clock))
       (.toNanos)
       (double)
       (/ 1000000.0))))

(defn insert-doc
  [{:keys [tx-log doc-index] :as db} doc-id doc topic]
  (let [last-key (hn/-last-key tx-log)
        next-key (if (some? last-key) (inc last-key) 0)
        doc (->CruxMessage doc nil next-key (Date.) doc-id {::tx/sub-topic (keyword topic)})
        tx-log (ha/<?? (hmsg/insert tx-log next-key doc))
        doc-index (if doc-id
                    (ha/<?? (hmsg/insert doc-index [doc-id next-key] nil))
                    doc-index)]
    (assoc db :tx-log tx-log :doc-index doc-index)))

(defn evict-doc
  [{:keys [tx-log doc-index] :as db} doc-id]
  (let [iterator (map first (hmsg/lookup-fwd-iter doc-index [doc-id]))
        ids (map second (take-while #(= doc-id (first %)) iterator))
        tx-log (reduce (fn [t id]
                         (hmsg/delete t id))
                       tx-log ids)
        doc-index (reduce (fn [t id]
                            (hmsg/delete t [doc-id id]))
                          doc-index ids)]
    (assoc db :tx-log tx-log :doc-index doc-index)))

(defn flush-trees!
  [{:keys [tx-log doc-index]} konserve]
  (log/info {:task :flush-trees! :phase :begin})
  (let [backend (hhkons/->KonserveBackend konserve)
        begin (now)
        tx-log-flushed (:tree (hh/flush-tree-without-root tx-log backend))
        doc-index-flushed (:tree (hh/flush-tree-without-root doc-index backend))]
    (log/info {:task :flush-trees! :phase :end :ms (ms begin)})
    {:tx-log tx-log-flushed
     :doc-index doc-index-flushed}))

(defn flush-root!
  [db konserve]
  (log/debug {:task ::flush-root! :phase :begin :db (pr-str db)})
  (let [begin (now)
        result (sv/<?? sv/S (k/assoc-in konserve [:tx] db))]
    (log/debug {:task ::flush-root! :phase :end :ms (ms begin)})
    result))

; Trees we store:
; tx-log: map of tx-id (integer) to docs
; doc-index: map of doc-id to tx-ids

(defrecord HitchhikerTreeTxLog [node-id db konserve]
  db/TxLog
  (submit-doc [_ content-hash doc]
    (locking db
      (let [new-db (vswap! db #(let [new-db (if (idx/evicted-doc? doc)
                                              (evict-doc % (str content-hash))
                                              (insert-doc % (str content-hash) doc "docs"))]
                                 (flush-trees! new-db konserve)))]
        (flush-root! new-db konserve)
        nil)))

  (submit-tx [_ tx-ops]
    (locking db
      (let [docs (tx/tx-ops->docs tx-ops)
            tx-events (tx/tx-ops->tx-events tx-ops)
            docs (conj (mapv #(vector (str (c/new-id %)) % "docs") docs)
                       [nil tx-events "txs"])
            new-db (vswap! db #(let [new-db (reduce (fn [db [id msg topic]]
                                                      (insert-doc db id msg topic))
                                                    % docs)]
                                 (flush-trees! new-db konserve)))
            result {:crux.tx/tx-id (hn/-last-key (:tx-log new-db))
                    :crux.tx/tx-time (Date.)}]
        (flush-root! new-db konserve)
        (delay result))))

  (new-tx-log-context [_]
    (->HitchhikerTreeTxLogConsumerContext @db))

  (tx-log [this context from-tx-id]
    ((fn step [from-tx-id]
       (lazy-seq
         (when-let [batch (not-empty (consumer/next-events this context from-tx-id))]
           (concat batch (step (.-message-id (last batch)))))))
     from-tx-id))

  ITxLogReload
  (reload! [_]
    (ha/go-try
      (locking db
        (let [current-db-chan (k/get-in konserve [:tx])
              current-db (ha/if-async?
                           (ha/<? current-db-chan)
                           (ha/throw-if-exception (async/<!! current-db-chan)))]
          (vreset! db (or current-db
                          {:tx-log (ha/<?? (hh/b-tree (hh/->Config 16 256 256)))
                           :doc-index (ha/<?? (hh/b-tree (hh/->Config 16 256 256)))}))))))

  consumer/PolledEventLog
  (new-event-log-context [_]
    (->HitchhikerTreeTxLogConsumerContext @db))

  (next-events [_ context next-offset]
    (let [tx-log (get-in context [:tree :tx-log])
          iter (ha/<?? (hmsg/lookup-fwd-iter tx-log next-offset))]
      (map (comp map->Message val) (take 100 iter))))

  (end-offset [_]
    (hn/-last-key (:tx-log @db))))

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

(defn start-konserve
  [{ddb-client :crux.hitchhiker.tree/ddb-client
    s3-client :crux.hitchhiker.tree/s3-client
    object-cache :crux.hitchhiker.tree/object-cache}
   {konserve-backend ::konserve-backend
    db-path ::db-path
    jdbc-url ::jdbc-url
    table-name ::table-name
    bucket-name ::bucket-name}]
  (kc/ensure-cache
    (hhkons/add-hitchhiker-tree-handlers
      (sv/<?? sv/S
        (case konserve-backend
          :mem    (km/new-mem-store)
          :file   (->> (kf/new-fs-store db-path :read-handlers (atom read-handlers) :write-handlers (atom write-handlers)))
          :level  (let [ctor (requiring-resolve 'konserve-leveldb.core/new-leveldb-store)]
                    (ctor db-path :read-handlers (atom read-handlers) :write-handlers (atom write-handlers)))
          :pg     (let [ctor (requiring-resolve 'konserve-pg.core/new-pg-store)]
                    (ctor jdbc-url :read-handlers (atom read-handlers) :write-handlers (atom write-handlers)))
          :ddb+s3 (let [ctor (requiring-resolve 'konserve-ddb-s3.core/connect-store)
                        args {:ddb-client ddb-client
                              :s3-client s3-client
                              :table table-name
                              :bucket bucket-name
                              :database :crux
                              :read-handlers (atom read-handlers)
                              :write-handlers (atom write-handlers)
                              :consistent-key #{:kv :tx}}]
                    (log/debug {:task ::start-konserve :phase :start-konserve-ddb-s3 :args args})
                    (ctor args)))))
    object-cache))

(defn start-tx-log
  [{konserve :crux.hitchhiker.tree/konserve}
   {node-id ::node-id}]
  (let [tx-log (->HitchhikerTreeTxLog (or node-id (str (UUID/randomUUID)))
                                      (volatile! {})
                                      konserve)]
    (ha/<?? (reload! tx-log))
    tx-log))

(defn- polling-consumer
  [running? indexer event-log-consumer sqs-client queue-name idle-sleep-ms]
  (log/debug :task ::polling-consumer :phase :begin :idle-sleep-ms idle-sleep-ms :queue-name queue-name)
  (let [queue-url (some-> sqs-client (aws/invoke {:op :GetQueueUrl :request {:QueueName queue-name}}) :QueueUrl)
        idle-sleep-ms (or idle-sleep-ms 1000)]
    (when-not (db/read-index-meta indexer :crux.tx-log/consumer-state)
      (db/store-index-meta
        indexer
        :crux.tx-log/consumer-state {:crux.tx/event-log {:lag 0
                                                         :next-offset 0
                                                         :time nil}}))
    (while @running?
      (let [idle? (with-open [context (consumer/new-event-log-context event-log-consumer)]
                    (let [next-offset (get-in (db/read-index-meta indexer :crux.tx-log/consumer-state) [:crux.tx/event-log :next-offset])]
                      (if-let [^Message last-message (reduce (fn [_last-message ^Message m]
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
        ;(log/debug :task ::polling-consumer :phase :events-processed :idle? idle?)
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
  ;(log/debug :task ::start-event-log-consumer :phase :begin
  ;           :indexer indexer :tx-log tx-log]
  (when (and (= :sqs consumer-type) (string/blank? queue-name))
    (throw (IllegalArgumentException. "missing queue-name for SQS consumer")))
  (let [running? (atom true)
        sqs-client (or sqs-client (when (= consumer-type :sqs)
                                    (aws/client {:api :sqs :region region :credentials-provider creds})))
        worker-thread (doto (Thread. #(try
                                        ; this impl copied here to use a longer idle sleep time
                                        ; every 10ms might be too much for dynamodb
                                        (log/debug {:task ::start-event-log-consumer :phase :starting-consumer
                                                    :args {:indexer indexer :tx-log tx-log :sqs-client sqs-client
                                                           :queue-name queue-name :idle-sleep-ms idle-sleep-ms}})
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

(s/def ::konserve-backend #{:mem :file :pg :level :ddb+s3})

(def topology (merge n/base-topology
                     {:crux.hitchhiker.tree/object-cache {:start-fn make-object-cache
                                                          :args {::cache-threshold
                                                                 {:doc "Max size for internal LRU object cache"
                                                                  :default 32
                                                                  :crux.config/type :crux.config/int}}}
                      :crux.hitchhiker.tree/s3-client {:start-fn start-s3-client
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
                      :crux.hitchhiker.tree/ddb-client {:start-fn start-ddb-client
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
                      :crux.hitchhiker.tree/konserve {:start-fn start-konserve
                                                      :deps [:crux.hitchhiker.tree/s3-client
                                                             :crux.hitchhiker.tree/ddb-client
                                                             :crux.hitchhiker.tree/object-cache]
                                                      :args {::konserve-backend
                                                             {:doc "The type of konserve backend."
                                                              :crux.config/required? true
                                                              :crux.config/type [(fn [x] (s/valid? ::konserve-backend x))
                                                                                 identity]}
                                                             ::db-path
                                                             {:doc "The db-path for file backends."
                                                              :crux.config/type :crux.config/string}
                                                             ::jdbc-url
                                                             {:doc "The JDBC datasource for pg backends."
                                                              :crux.config/type :crux.config/string}
                                                             ::table-name
                                                             {:doc "The DynamoDB table name for ddb+s3 backends."
                                                              :crux.config/type :crux.config/string}
                                                             ::bucket-name
                                                             {:doc "The S3 bucket name for ddb+s3 backends."
                                                              :crux.config/type :crux.config/string}}}
                      ::n/tx-log {:start-fn start-tx-log
                                  :deps [:crux.hitchhiker.tree/konserve]
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