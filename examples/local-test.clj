(in-ns 'crux.aws.repl)

; An example for getting crux-aws running locally, with mocked S3, SQS, and DynamoDB,
; so you don't have to pay AWS for testing this out.

; Add something like this to profiles.clj to get started:
'{:repl {:dependencies [[ch.qos.logback/logback-classic "1.1.8"]
                        [ch.qos.logback/logback-core "1.1.8"]
                        [juxt/crux-rocksdb "19.09-1.4.0-alpha"]
                        [juxt/crux-kafka "19.09-1.4.0-alpha"]
                        [juxt/crux-jdbc "19.09-1.4.0-alpha"]
                        [io.replikativ/konserve "0.5.1"]
                        [io.replikativ/konserve-leveldb "0.1.2" :exclusions [manifold]]
                        [s4 "0.1.8"]
                        [org.clojure/data.csv "0.1.4"]
                        [itc4j/itc4j-core "0.7.0"]
                        [org.eclipse.jetty/jetty-server "9.4.15.v20190215"]
                        [com.amazonaws/DynamoDBLocal "1.11.477"
                         :exclusions
                         [com.fasterxml.jackson.core/jackson-core
                          org.eclipse.jetty/jetty-client
                          com.google.guava/guava
                          commons-logging]]
                        [com.almworks.sqlite4java/libsqlite4java-osx "1.0.392" :extension "dylib"]]
         :repositories [["aws-dynamodb-local" {:url "https://s3-us-west-2.amazonaws.com/dynamodb-local/release"}]]
         :source-paths ["scripts" "examples"]
         :jvm-opts ["-Dsqlite4java.library.path=/Users/YOUR_USERNAME_HERE/.m2/repository/com/almworks/sqlite4java/libsqlite4java-osx/1.0.392/"]}}

(import [com.amazonaws.services.dynamodbv2.local.server LocalDynamoDBRequestHandler
         LocalDynamoDBServerHandler
         DynamoDBProxyServer])
(require '[clojure.java.io :as io])

(defn start-dynamodb-local
  []
  (let [ddb-handler (LocalDynamoDBRequestHandler. 0 false (-> (doto (io/file "data/ddb")
                                                                (.mkdir))
                                                              (.getAbsolutePath))
                                                  false false)
        server-handler (LocalDynamoDBServerHandler. ddb-handler nil)
        server (doto (DynamoDBProxyServer. 0 server-handler) (.start))
        server-field (doto (.getDeclaredField DynamoDBProxyServer "server")
                       (.setAccessible true))
        jetty-server (.get server-field server)
        port (-> jetty-server (.getConnectors) first (.getLocalPort))]
    {:server server
     :port port}))

(require '[s4.core :as s4])
(require '[konserve-leveldb.core :as kldb])
(require '[clojure.core.async :as async])
(require '[konserve.serializers :as ser])
(require '[superv.async :as sv])

(defn start-s3-local
  []
  (.mkdir (io/file "data/s3"))
  (let [konserve (sv/<?? sv/S (kldb/new-leveldb-store "data/s3" :serializer (ser/fressian-serializer @s4/read-handlers
                                                                                                     @s4/write-handlers)))]
    (s4/make-server! {:konserve konserve :enable-sqs? true})))

(.setLevel (org.slf4j.LoggerFactory/getLogger "s4") ch.qos.logback.classic.Level/INFO)
(.setLevel (org.slf4j.LoggerFactory/getLogger "org.eclipse") ch.qos.logback.classic.Level/INFO)

(def ddb (start-dynamodb-local))
(def s3 (start-s3-local))

(swap! (-> @s3 :auth-store :access-keys) assoc "ACCESSKEY" "SECRETACCESSKEY")

(require '[cognitect.aws.client.api :as aws])
(require '[cognitect.aws.credentials :as creds])

(def ddb-client (aws/client {:api :dynamodb
                             :credentials-provider (creds/basic-credentials-provider {:access-key-id "ACCESSKEY"
                                                                                      :secret-access-key "SECRETACCESSKEY"})
                             :endpoint-override {:protocol "http"
                                                 :hostname "localhost"
                                                 :port (:port ddb)}}))
(def s3-client (aws/client {:api :s3
                            :credentials-provider (creds/basic-credentials-provider {:access-key-id "ACCESSKEY"
                                                                                     :secret-access-key "SECRETACCESSKEY"})
                            :endpoint-override {:protocol "http"
                                                :hostname "localhost"
                                                :port (-> @s3 :bind-address (.getPort))}}))
(def sqs-client (aws/client {:api :sqs
                             :credentials-provider (creds/basic-credentials-provider {:access-key-id "ACCESSKEY"
                                                                                      :secret-access-key "SECRETACCESSKEY"})
                             :endpoint-override {:protocol "http"
                                                 :hostname "localhost"
                                                 :port (-> @s3 :sqs-server deref :bind-address (.getPort))}}))

; If you restart and still have your data on disk, skip the following four commands
(aws/invoke ddb-client {:op :CreateTable
                        :request {:TableName "crux-aws"
                                  :AttributeDefinitions [{:AttributeName "Id"
                                                          :AttributeType "S"}]
                                  :KeySchema [{:AttributeName "Id"
                                               :KeyType "HASH"}]
                                  :ProvisionedThroughput {:ReadCapacityUnits 5
                                                          :WriteCapacityUnits 5}}})

(aws/invoke s3-client {:op :CreateBucket :request {:Bucket "crux-aws"}})
(aws/invoke sqs-client {:op :CreateQueue :request {:QueueName "crux-aws"}})
(aws/invoke s3-client {:op :PutBucketNotificationConfiguration
                       :request {:Bucket "crux-aws"
                                 :NotificationConfiguration {:QueueConfigurations [{:Id "objects-created"
                                                                                    :QueueArn "crux-aws"
                                                                                    :Events ["s3:ObjectCreated:*"]}]}}})

(require 'crux.aws)

(def crux (crux.aws/start-aws-node {:s3-client s3-client
                                    :ddb-client ddb-client
                                    :sqs-client sqs-client
                                    :bucket "crux-aws"
                                    :table-name "crux-aws"
                                    :queue-name "crux-aws"
                                    :db-dir "data/index"}))

(require 'crux.api)
(import 'java.util.UUID)
(import 'java.util.Date)

(crux.api/submit-tx crux [[:crux.tx/put {:crux.db/id (UUID/randomUUID) :test "test"} (Date.)]])
(crux.api/q (crux.api/db crux) '{:find [e t] :where [[e :test t]]})