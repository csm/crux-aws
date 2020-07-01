(defproject crux-aws "0.1.0-SNAPSHOT"
  :description "Crux atop DynamoDB and S3. Experimental."
  :url "https://github.com/csm/crux-aws"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [org.clojure/core.cache "0.7.2"]
                 [org.clojure/core.memoize "0.7.2"]
                 [juxt/crux-core "20.06-1.9.1-beta"]
                 [juxt/crux-s3 "20.06-1.9.1-beta" :exclusions [software.amazon.awssdk/s3]]
                 [com.github.csm/crux-dynamodb "0.1.1"]
                 [com.github.csm/konserve-ddb-s3 "0.1.4"]
                 [com.github.csm/crux-hitchhiker-tree "0.1.0"]
                 [ch.qos.logback/logback-classic "1.2.3"]
                 [ch.qos.logback/logback-core "1.2.3"]]
  :repl-options {:init-ns crux.aws.repl}
  :profiles {:repl {:source-paths ["examples"]}})
