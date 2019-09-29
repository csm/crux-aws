(defproject crux-aws "0.1.0-SNAPSHOT"
  :description "Crux atop S3 and SQS. Experimental."
  :url "https://github.com/csm/crux-aws"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [juxt/crux-core "19.09-1.4.0-alpha"]
                 [com.cognitect.aws/api "0.8.352"]
                 [com.cognitect.aws/endpoints "1.1.11.632"]
                 [com.cognitect.aws/s3 "726.2.488.0"]
                 [com.cognitect.aws/sqs "747.2.533.0"]
                 [com.cognitect/anomalies "0.1.12"]]
  :repl-options {:init-ns crux.aws.repl})
