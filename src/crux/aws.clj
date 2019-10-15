(ns crux.aws
  (:require [clojure.spec.alpha :as s]
            [cognitect.aws.client :as aws-client]
            crux.bootstrap
            crux.standalone
            [crux.moberg :as moberg]
            [crux.tx.sqs :as sqs]
            [crux.bootstrap :as b])
  (:import [crux.api ICruxAPI]))

(defn start-event-log-consumer
  [{:keys [event-log-kv indexer]} {:keys [region sqs-queue sqs-client]}]
  (when event-log-kv
    (sqs/start-event-log-consumer indexer
                                  (moberg/map->MobergEventLogConsumer {:event-log-kv event-log-kv
                                                                       :batch-size 100})
                                  region sqs-queue
                                  :sqs-client sqs-client)))

(s/def :sqs-event-log-consumer/region string?)
(s/def :sqs-event-log-consumer/sqs-queue string?)
(s/def :sqs-event-log-consumer/sqs-client (s/nilable #(satisfies? aws-client/ClientSPI %)))
(s/def ::sqs-event-log-consumer (s/keys :opt-un [:sqs-event-log-consumer/region
                                                 :sqs-event-log-consumer/sqs-queue
                                                 :sqs-event-log-consumer/sqs-client]))

(def event-log-consumer [start-event-log-consumer
                         [:event-log-kv :indexer]
                         ::sqs-event-log-consumer])

(s/def ::event-log-dir string?)
(s/def ::event-log-kv-backend :crux.kv/kv-backend)
(s/def ::s3-client (s/nilable #(satisfies? aws-client/ClientSPI %)))
(s/def ::event-log-kv-opts (s/keys :req-un [:crux.kv/db-dir
                                            ::event-log-dir]
                                   :opt-un [:crux.kv/kv-backend
                                            :crux.kv/sync?]
                                   :opt [::event-log-sync-interval-ms
                                         ::event-log-kv-backend
                                         ::s3-client]))

(defn- start-event-log-kv [_ {:keys [crux.aws/event-log-kv-backend
                                     crux.aws/event-log-sync-interval-ms
                                     crux.aws/s3-client
                                     event-log-dir kv-backend sync?]}]
  (let [event-log-sync? (boolean (or sync? (not event-log-sync-interval-ms)))]
    (b/start-kv-store
      {:db-dir event-log-dir
       :kv-backend (or event-log-kv-backend kv-backend)
       :sync? event-log-sync?
       :crux.index/check-and-store-index-version false
       :crux.aws/s3-client s3-client})))

(def event-log-kv [start-event-log-kv [] ::event-log-kv-opts])

(def node-config (dissoc
                   (assoc crux.standalone/node-config
                     :event-log-consumer event-log-consumer
                     :event-log-kv event-log-kv)
                   :event-log-sync))

(defn start-aws-node
  ^ICruxAPI [config]
  (crux.bootstrap/start-node node-config
                             (assoc config :kv-backend "crux.kv.rocksdb.RocksKv"
                                           :crux.aws/event-log-kv-backend "crux.kv.s3.S3Kv")))

; lots of todos here still; it's silly to say 'event-log-dir' is
; your S3 bucket; you also have to manually create an SNS topic,
; have S3 push events to it, and create SQS queues and subscribe
; them to the topic.

(comment
  (start-aws-node {:region "us-west-2"
                   :event-log-dir "your-event-log-s3-bucket"
                   :sqs-queue "your-event-log-sqs-queue"}))

