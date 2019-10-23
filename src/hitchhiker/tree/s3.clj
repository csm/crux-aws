(ns hitchhiker.tree.s3
  (:require [clojure.core.async :as async]
            [clojure.spec.alpha :as s]
            [cognitect.anomalies :as anomalies]
            [cognitect.aws.client.api.async :as aws]
            [hitchhiker.tree.core :as core]
            [clojure.core.cache :as cache]
            [taoensso.nippy :as nippy]
            [clojure.string :as string])
  (:import [com.google.common.io ByteStreams]
           [java.util UUID]
           [java.security SecureRandom]))

(def ^:dynamic *context* {})

(let [random (SecureRandom.)]
  (defn squuid
    []
    (let [hi (bit-or (bit-shift-left (long (/ (System/currentTimeMillis) 1000)) 32)
                     0x4000
                     (bit-and (.nextInt random) 0xffff0fff))
          lo (bit-or (bit-and (.nextLong random)
                              (bit-not (bit-shift-left 0xc0 56)))
                     (bit-shift-left 1 63))]
      (UUID. hi lo))))

(defrecord S3Address [context guid last-key]
  core/IResolve
  (index? [_] false)
  (dirty? [_] false)
  (last-key [_] last-key)
  (resolve [_]
    (core/go-try
      (if-let [value (cache/lookup @(:cache context) guid)]
        (do
          (swap! (:cache context) cache/hit guid)
          (assoc value :storage-addr (doto (async/promise-chan) (async/put! guid))))
        (let [result (async/<! (aws/invoke (:s3-client context) {:op :GetObject
                                                                 :request {:Bucket (:bucket context)
                                                                           :Key (str guid)}}))]
          (cond (s/valid? ::anomalies/anomaly result)
                (ex-info "failed to get value from S3" {:error result})

                :else
                (binding [*context* context]
                  (let [value (nippy/thaw (ByteStreams/toByteArray (:Body result)))]
                    (swap! (:cache context) cache/miss guid value)
                    (assoc value :storage-addr (doto (async/promise-chan) (async/put! guid)))))))))))

(defmethod print-method S3Address
  [this writer]
  (.write writer (str "#hitchhiker.tree.s3.S3Address{:guid #uuid\"" (:guid this)
                      "\" :last-key " (:last-key this) \})))

(defn node->value [node]
  (if (core/index-node? node)
    (-> (assoc node :storage-addr nil)
        (update :children (fn [cs] (mapv #(assoc % :store nil
                                                  :storage-addr nil) cs))))
    (assoc node :storage-addr nil)))

(defrecord S3Backend [s3-client bucket cache]
  core/IBackend
  (new-session [_]
    (atom {:writes 0 :deletes 0}))

  (write-node [this node session]
    (core/go-try
      (swap! session update :writes inc)
      (let [pnode (node->value node)
            guid (squuid)
            value (nippy/freeze pnode)
            address (let [result (async/<! (aws/invoke s3-client {:op :PutObject
                                                                  :request {:Bucket bucket
                                                                            :Key (str guid)
                                                                            :Body value}}))]
                      (if (s/valid? ::anomalies/anomaly result)
                        (throw (ex-info "failed to write node to S3" {:error result}))
                        (->S3Address this guid (core/last-key node))))]
        (swap! cache cache/miss guid pnode)
        address)))

  (anchor-root [this node]
    (async/go
      (async/<! (async/timeout 5000))
      (when-let [addr (some-> (:storage-addr node) async/<!)]
        (let [rez (core/delete-addr this addr (atom {:deletes 0}))]
          (when-let [ch (some-> rez meta :channel)]
            (async/<! ch)))))
    node)

  (delete-addr [_ addr session]
    (if (some? addr)
      (let [delete-chan (core/go-try
                          (let [result (async/<! (aws/invoke s3-client {:op :DeleteObject
                                                                        :request {:Bucket bucket
                                                                                  :Key (str addr)}}))]
                            (cond (s/valid? ::anomalies/anomaly result)
                                  (ex-info "failed to delete object" {:key (str addr) :bucket bucket
                                                                      :error result})

                                  :else
                                  nil)))]
        (with-meta (swap! session update :deletes inc) {:channel delete-chan}))
      @session)))

(nippy/extend-freeze S3Address :b-tree/s3-addr
  [{:keys [guid last-key]} data-output]
  (nippy/freeze-to-out! data-output guid)
  (nippy/freeze-to-out! data-output last-key))

(nippy/extend-thaw :b-tree/s3-addr
  [in]
  (let [guid (nippy/thaw-from-in! in)
        last-key (nippy/thaw-from-in! in)]
    (map->S3Address {:context *context*
                     :guid guid
                     :last-key last-key})))

(defn create-tree-from-root-key
  [s3-client bucket cache root-key]
  (core/go-try
    (let [address (->S3Address {:s3-client s3-client :bucket bucket :cache cache} root-key nil)]
      (core/<? (core/resolve address)))))