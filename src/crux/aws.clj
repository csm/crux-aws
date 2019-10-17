(ns crux.aws
  (:require [crux.bootstrap :as b]
            [crux.tx.s3 :as s3])
  (:import [crux.api ICruxAPI]))

(defn start-aws-node
  ^ICruxAPI [config]
  (let [config (if (nil? (:kv-backend config))
                 (assoc config :kv-backend "crux.kv.rocksdb.RocksKv")
                 config)]
    (b/start-node s3/node-config config)))

(comment
  "You'll want an S3 bucket, DynamoDB table, and SQS queue.

  The bucket should be a vanilla bucket -- versioning not needed.

  The DynamoDB table should have:
    AttributeDefinition Name \"Id\" Type \"S\".
    KeySchema Name \"Id\" KeyType \"HASH\".

  DynamoDB is only used to track an atomic counter, so it should
  not consume much storage space at all; the read and write capacity
  should be equal, since it always does a read-then-conditional-update.

  The SQS queue should be configured to receive s3:ObjectCreated:*
  events, either directly or via an SNS topic."

  (start-aws-node {:region     "us-west-2"
                   :table-name "your-dynamodb-table"
                   :bucket     "your-s3-bucket"
                   :sqs-queue  "your-sqs-queue"}))

