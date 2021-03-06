(in-ns 'crux.aws.repl)

(.setLevel (org.slf4j.LoggerFactory/getLogger "software") ch.qos.logback.classic.Level/INFO)
(.setLevel (org.slf4j.LoggerFactory/getLogger "io.netty") ch.qos.logback.classic.Level/INFO)
(.setLevel (org.slf4j.LoggerFactory/getLogger "crux") ch.qos.logback.classic.Level/INFO)
(require 'crux.aws
         '[crux.api :as crux]
         'crux.kv.hitchhiker-tree.konserve.ddb-s3)

(def node (crux/start-node {:crux.node/topology crux.aws/topology
                            :crux.dynamodb/table-name "csm-crux-test"
                            :crux.s3/bucket "csm-crux-test"
                            :crux.kv.hitchhiker-tree/konserve crux.kv.hitchhiker-tree.konserve.ddb-s3/ddb-s3-backend
                            :crux.kv.hitchhiker-tree.konserve.ddb-s3/bucket "csm-crux-kv-test"
                            :crux.kv.hitchhiker-tree.konserve.ddb-s3/table "csm-crux-kv-test"
                            :crux.kv.hitchhiker-tree.konserve.ddb-s3/region "us-west-2"}))

(def node2 (crux/start-node {:crux.node/topology crux.aws/topology-ddb-s3
                             :crux.ddb-s3/table "csm-crux-tx-test"
                             :crux.ddb-s3/bucket "csm-crux-tx-test"
                             :crux.s3/bucket "csm-crux-tx-test"
                             :crux.s3/prefix "doc/"}))
                             ; hitchhiker-tree konserve serialization currently broken
                             ;:crux.kv.hitchhiker-tree/konserve crux.kv.hitchhiker-tree.konserve.ddb-s3/ddb-s3-backend
                             ;:crux.kv.hitchhiker-tree.konserve.ddb-s3/bucket "csm-crux-kv-test"
                             ;:crux.kv.hitchhiker-tree.konserve.ddb-s3/table "csm-crux-kv-test"
                             ;:crux.kv.hitchhiker-tree.konserve.ddb-s3/region "us-west-2"}))