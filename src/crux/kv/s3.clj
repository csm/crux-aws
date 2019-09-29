(ns crux.kv.s3
  (:require [crux.kv :as kv]
            [crux.kv.s3.impl :refer :all]
            [cognitect.aws.client.api :as aws])
  (:import [java.io Closeable]))

(defrecord S3KvIterator [state snapshot]
  kv/KvIterator
  (seek [this k] (s3kv-iterator-seek this k))
  (next [this] (s3kv-iterator-next this))
  (prev [this] (s3kv-iterator-prev this))
  (value [this] (s3kv-iterator-value this))
  Closeable
  (close [_]))

(defrecord S3KvSnapshot [client bucket timestamp]
  kv/KvSnapshot
  (new-iterator [this] (s3kv-snapshot-new-iterator this))
  (get-value [this k] (s3kv-snapshot-get-value this k))
  Closeable
  (close [_]))

(defrecord S3Kv [client bucket]
  kv/KvStore
  (open [this options] (s3kv-open this options))
  (new-snapshot [this] (s3kv-new-snapshot this))
  (store [this kvs] (s3kv-store this kvs))
  (delete [this ks] (s3kv-delete this ks))
  (fsync [_])
  (backup [_ v] (throw (UnsupportedOperationException. "not implemented")))
  (count-keys [this] (s3kv-count-keys this))
  (db-dir [_] [])
  (kv-name [_] "s3")

  Closeable
  (close [{:keys [client]}] (aws/stop client)))

