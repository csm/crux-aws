(ns crux.aws
  (:require [clojure.spec.alpha :as s]
            crux.bootstrap
            crux.standalone
            [crux.moberg :as moberg]
            [crux.tx.sqs :as sqs])
  (:import [crux.api ICruxAPI]))

(defn start-event-log-consumer
  [{:keys [event-log-kv indexer]} {:keys [region sqs-queue]}]
  (when event-log-kv
    (sqs/start-event-log-consumer indexer
                                  (moberg/map->MobergEventLogConsumer {:event-log-kv event-log-kv
                                                                       :batch-size 100})
                                  region sqs-queue)))

(s/def :sqs-event-log-consumer/region string?)
(s/def :sqs-event-log-consumer/sqs-queue string?)
(s/def ::sqs-event-log-consumer (s/keys :opt-un [:sqs-event-log-consumer/region
                                                 :sqs-event-log-consumer/sqs-queue]))

(def event-log-consumer [start-event-log-consumer
                         [:event-log-kv :indexer]
                         ::sqs-event-log-consumer])

(def node-config (dissoc
                   (assoc crux.standalone/node-config
                     :event-log-consumer event-log-consumer)
                   :event-log-sync))

(defn start-aws-node
  ^ICruxAPI [config]
  (crux.bootstrap/start-node node-config
                             (assoc config :kv-backend "crux.kv.rocksdb.RocksKv"
                                           :crux.standalone/event-log-kv-backend "crux.kv.s3.S3Kv")))

; lots of todos here still; it's silly to say 'event-log-dir' is
; your S3 bucket; you also have to manually create an SNS topic,
; have S3 push events to it, and create SQS queues and subscribe
; them to the topic.

(comment
  (start-aws-node {:kv-backend "crux.kv.rocksdb.RocksKv"
                   :crux.standalone/event-log-kv-backend "crux.kv.s3.S3Kv"
                   :region "us-west-2"
                   :event-log-dir "your-event-log-s3-bucket"
                   :sqs-queue "your-event-log-sqs-queue"}))

