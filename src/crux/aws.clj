(ns crux.aws
  (:require [crux.node :as n]
            crux.kv.hitchhiker-tree
            [crux.tx.hitchhiker-tree :as crux-hh])
  (:import [crux.api ICruxAPI]))

(comment ; todo rewrite for new DI stuff
  (defn start-aws-node
    ^ICruxAPI [config]
    (let [config (if (nil? (:kv-backend config))
                   (assoc config :kv-backend "crux.kv.rocksdb.RocksKv")
                   config)]
      (b/start-node s3/node-config config))))

(defn start-hh-node
  ^ICruxAPI [config]
  (n/start
    (let [topology crux-hh/topology
          topology (if (= :crux.kv/hitchhiker-tree (::kv-backend config))
                     (assoc topology ::n/kv-store crux.kv.hitchhiker-tree/kv)
                     topology)]
      (assoc (dissoc config ::kv-backend) :crux.node/topology topology))))

(comment
  "You'll want an S3 bucket and DynamoDB table at minimum.

  The bucket should be a vanilla bucket -- versioning not needed.

  The DynamoDB table should have table spec:

    Attribute definitions:
    topic, type S
    id, type S

    Key schema:
    topic, key type HASH
    id, key type RANGE

  Also configure TTL on your table on the 'expires' attribute.

  For doing real-time indexing as the tx-log is added to, you can
  also create a DynamoDB stream that publishes to a lambda, which
  pushes to an SNS topic, and finally subscribe an SQS queue to
  that topic. The alternative just polls DynamoDB for new
  transactions.

  Example lambda code:

  var AWS = require('aws-sdk');
  exports.handler = async (event, context) => {
    var sns = new AWS.SNS();
    var logAltered = false;
    for (const record of event.Records) {
      if (record.dynamodb['Keys']['topic']['S'] === 'tx-log') {
        logAltered = true;
        break;
      }
    }
    if (logAltered) {
      var result = await sns.publish({TopicArn: 'topic-arn-here',
                                      Message: JSON.stringify({Cause: \"tx-log\"})}).promise();
      console.log(`sns result: ${result}`);
    }
    return `Triggered tx-log event? ${logAltered}`;
  };"

  (start-aws-node {:crux.tx.hitchhiker-tree/region     "us-west-2"
                   :crux.tx.hitchhiker-tree/table-name "your-dynamodb-table"
                   :crux.tx.hitchhiker-tree/bucket     "your-s3-bucket"
                   :crux.tx.hitchhiker-tree/queue-name "your-sqs-queue"}))

