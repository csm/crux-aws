(ns crux.kv.hitchhiker-tree
  (:require [clojure.core.async :as async]
            [clojure.core.cache :as cache]
            [cognitect.aws.client.api :as aws]
            [cognitect.aws.client.api.async :as aws-async]
            [crux.aws.util :refer :all]
            [crux.kv :as kv]
            [hitchhiker.tree.s3 :as hhs3]
            [hitchhiker.tree.core :as hh])
  (:import [java.io Closeable]))

(defn load-tree
  [backend ddb-client table-name]
  (hh/go-try
    (loop [delay 500]
      (let [get-item (async/<! (aws-async/invoke ddb-client {:op :GetItem
                                                             :request {:TableName table-name
                                                                       :Key {"topic" {:S "kv"}
                                                                             "id" {:S "root"}}}}))]
        (cond (throttle? get-item)
              (do
                (async/<! (async/timeout delay))
                (recur (min 60000 (* 2 delay))))

              (anomaly? get-item)
              (throw (ex-info "failed to read KV root" {:error get-item}))

              :else
              (if-let [root-address (-> get-item :Item :root-address :S)]
                (hh/<? (hhs3/create-tree-from-root-key backend root-address))
                (hh/<? (hh/b-tree (hh/->Config 32 64 8)))))))))

(defrecord HitchhikerTreeKvSnapshot [root]
  Closeable
  (close [_]))

(defrecord HitchhikerTreeKv [state backend ddb-client table-name]
  kv/KvStore
  (open [this options]
    (let [ddb-client (or (::ddb-client options) (aws/client {:api :dynamodb
                                                             :region (::region options)
                                                             :credentials-provider (::creds options)}))
          s3-client (or (::s3-client options) (aws/client {:api :s3
                                                           :region (::region options)
                                                           :credentials-provider (::creds options)}))
          anchor-chan (async/chan)
          anchor-mult (async/mult anchor-chan)
          backend (hhs3/->S3Backend s3-client (::bucket options) (atom (cache/lru-cache-factory {})) anchor-chan anchor-mult)
          root (hh/<?? (load-tree backend ddb-client (::table-name options)))]
      (assoc this :backend backend
                  :ddb-client ddb-client
                  :table-name (::table-name options)
                  :state (atom {:root root}))))

  (new-snapshot ^Closeable
    [this]
    (->HitchhikerTreeKvSnapshot (:root @state)))

  (store [this kvs])

  (delete [this ks])

  (fsync [this])

  (backup [this dir])

  (count-keys [this])

  (db-dir [this])

  (kv-name [this]))
