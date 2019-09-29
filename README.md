# crux-aws

[crux](https://juxt.pro/crux/) atop S3 and SQS.

Experimental.

Quick start:

1. Create an S3 bucket with versioning enabled.
2. Create an SNS topic, and push S3 create events to it.
3. Create an SQS queue, and subscribe it to the SNS topic.
4. Launch crux via `crux.aws/start-aws-node` (the idea is
   to use a local store for the index, but use S3/SNS for
   the tx-log).

Primarily an investigation on using crux as a data store
atop a very cheap storage solution. It may not work at all.