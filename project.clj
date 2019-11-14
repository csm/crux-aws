(defproject crux-aws "0.1.0-SNAPSHOT"
  :description "Crux atop S3 and SQS. Experimental."
  :url "https://github.com/csm/crux-aws"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [org.clojure/core.cache "0.7.2"]
                 [org.clojure/core.memoize "0.7.2"]
                 [juxt/crux-core "19.09-1.5.0-alpha"]
                 [com.cognitect.aws/api "0.8.378"]
                 [com.cognitect.aws/endpoints "1.1.11.655"]
                 [com.cognitect.aws/s3 "747.2.533.0"]
                 [com.cognitect.aws/sqs "747.2.533.0"]
                 [com.cognitect.aws/dynamodb "746.2.533.0"]
                 [com.cognitect.aws/streams-dynamodb "747.2.533.0"]
                 [com.cognitect/anomalies "0.1.12"]
                 [io.replikativ/hitchhiker-tree "0.1.5-SNAPSHOT"]
                 [com.github.csm/konserve-ddb-s3 "0.1.1" :scope "provided"]
                 [io.replikativ/konserve-leveldb "0.1.2" :scope "provided"]
                 [org.clojars.mihaelkonjevic/konserve-pg "0.1.2" :scope "provided"]]
  :repl-options {:init-ns crux.aws.repl})
