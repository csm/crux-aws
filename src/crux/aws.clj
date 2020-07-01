(ns crux.aws
  (:require [crux.node :as n]))

(def topology
  ['crux.node/base-topology
   'crux.dynamodb/dynamodb-tx-log
   'crux.s3/s3-doc-store
   'crux.kv.hitchhiker-tree/kv])