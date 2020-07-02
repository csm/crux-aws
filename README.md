# crux-aws

[crux](https://juxt.pro/crux/) atop S3 and DynamoDB.

Experimental.

## Quick Start:

For a full AWS experience:

```clojure
(def node (crux.api/start-node {:crux.node/topology crux.aws/topology
                                :crux.dynamodb/table-name "your-dynamodb-tx-table"
                                :crux.s3/bucket "your-s3-doc-bucket"
                                :crux.kv.hitchhiker-tree/konserve crux.kv.hitchhiker-tree.konserve.ddb-s3/ddb-s3-backend
                                :crux.kv.hitchhiker-tree.konserve.ddb-s3/bucket "your-dynamodb-kv-table"
                                :crux.kv.hitchhiker-tree.konserve.ddb-s3/table "your-s3-kv-bucket"
                                :crux.kv.hitchhiker-tree.konserve.ddb-s3/region "us-west-2"}))
```

**NOTE** the hitchhiker-tree KV store with the konserve-ddb-s3 backend is
currently broken; use a different KV store for now.

## TODO

The KV store needs to handle concurrent writes better than it does.

The KV store should take updates from the remote store periodically (or on changes
to dynamodb) so it keeps in sync.

The transaction log could wait for changes triggered