(ns crux.tx.sqs
  (:require [clojure.spec.alpha :as s]
            [clojure.tools.logging :as log]
            [cognitect.anomalies :as anomalies]
            [cognitect.aws.client.api :as aws]
            [crux.db :as db]
            [crux.tx.consumer :as consumer])
  (:import [crux.tx.consumer Message]
           [java.io Closeable]))

(defn- sqs-polling-consumer
  [running? indexer event-log-consumer sqs-client sqs-queue]
  (when-not (db/read-index-meta indexer :crux.tx-log/consumer-state)
    (db/store-index-meta
      indexer
      :crux.tx-log/consumer-state {:crux.tx/event-log {:lag 0
                                                       :next-offset 0
                                                       :time nil}}))
  (let [queue-url (aws/invoke sqs-client {:op :GetQueueUrl
                                          :request {:QueueName sqs-queue}})]
    (if (s/valid? ::anomalies/anomaly queue-url)
      (throw (ex-info "failed to read queue URL" {:error queue-url}))
      (while running?
        (let [idle? (with-open [context (consumer/new-event-log-context event-log-consumer)]
                      (let [next-offset (get-in (db/read-index-meta indexer :crux.tx-log/consumer-state) [:crux.tx/event-log :next-offset])]
                        (if-let [^Message last-message (reduce (fn [last-message ^Message m]
                                                                 (log/debug "message:" m)
                                                                 (case (get (.headers m) :crux.tx/sub-topic)
                                                                   :docs
                                                                   (db/index-doc indexer (.key m) (.body m))
                                                                   :txs
                                                                   (db/index-tx indexer
                                                                                (.body m)
                                                                                (.message-time m)
                                                                                (.message-id m))
                                                                   nil)
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
                            (zero? lag))
                          true)))]
          (when idle?
            (log/debug :task ::sqs-polling-consumer :phase :polling-sqs :queue queue-url)
            (let [reply (aws/invoke sqs-client {:op :ReceiveMessage
                                                :request (assoc queue-url
                                                           :WaitTimeSeconds 20
                                                           :MaxNumberOfMessages 10)})]
              (log/debug :task ::sqs-polling-consumer :phase :polled-sqs :result reply)
              (if (s/valid? ::anomalies/anomaly reply)
                (log/warn :sqs-error reply)
                (doseq [message (:Messages reply)]
                  (aws/invoke sqs-client {:op :DeleteMessage
                                          :request (assoc queue-url
                                                     :ReceiptHandle (:ReceiptHandle message))}))))))))))

(defn start-event-log-consumer
  ^Closeable
  [indexer event-log-consumer region sqs-queue & {:keys [sqs-client]}]
  (let [running? (atom true)
        sqs-client (or sqs-client (aws/client {:api :sqs :region region}))
        worker-thread (doto (Thread. #(try
                                        (sqs-polling-consumer running? indexer event-log-consumer sqs-client sqs-queue)
                                        (catch Throwable t
                                          (log/fatal t "Event log consumer threw exception, consumption has stopped:")))
                                     "crux.tx.event-log-consumer-thread")
                        (.start))]
    (reify Closeable
      (close [_]
        (reset! running? false)
        (.join worker-thread)))))