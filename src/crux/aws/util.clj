(ns crux.aws.util
  (:require [clojure.spec.alpha :as s]
            [clojure.string :as string]
            [cognitect.anomalies :as anomalies]))

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

