(ns crux.kv.s3.impl
  (:require [clojure.java.io :as io]
            [clojure.spec.alpha :as s]
            [clojure.string :as string]
            [clojure.tools.logging :as log]
            [cognitect.anomalies :as anomalies]
            [cognitect.aws.client.api :as aws]
            [crux.memory :as mem])
  (:import [java.util Date Collections]
           [java.io ByteArrayOutputStream]
           [org.agrona ExpandableDirectByteBuffer]
           [org.agrona.io DirectBufferInputStream]))

; hex converters until crux cuts a release with https://github.com/juxt/crux/commit/b06335ffd3c717839b3ccfcabe36d18792c50df2
(let [hex->nibble (into {} (map (fn [i] [(.charAt (format "%x" i) 0) i]) (range 0x10)))]
  (defn hex->buffer
    [^String b]
    (if (zero? (mod (.length b) 2))
      (let [b (string/lower-case b)
            length (.length b)
            buffer (ExpandableDirectByteBuffer. (bit-shift-right length 1))]
        (->> (partition 2 b)
             (map-indexed (fn [i [a b]]
                            (let [a (hex->nibble a)
                                  b (hex->nibble b)
                                  bt (.byteValue (bit-or (bit-shift-left a 4) b))]
                              (.putByte buffer i bt))))
             (doall))
        buffer)
      (recur (str "0" b)))))

; convert to hex seems correct
(def buffer->hex mem/buffer->hex)

;; lazy-seq's from version listings

(def ^:dynamic *page-size* 1000)

(defn versions-seq
  ([client bucket] (versions-seq client bucket {}))
  ([client bucket {:keys [Prefix KeyMarker VersionIdMarker]}]
   (let [page (aws/invoke client {:op :ListObjectVersions
                                  :request {:Bucket bucket
                                            :Prefix Prefix
                                            :KeyMarker KeyMarker
                                            :VersionIdMarker VersionIdMarker
                                            :MaxKeys *page-size*}})]
     (if (s/valid? ::anomalies/anomaly page)
       (throw (ex-info "failed to list object versions" {:error page}))
       (let [page (->> (concat (map #(assoc % :IsDeleteMarker true) (:DeleteMarkers page))
                               (:Versions page))
                       (sort-by #(vector (:Key %) (- (.getTime ^Date (:LastModified %))))))]
         (lazy-cat page
                   (when (:IsTruncated page)
                     (versions-seq client bucket {:Prefix Prefix
                                                  :KeyMarker (:NextKeyMarker page)
                                                  :VersionIdMarker (:NextVersionIdMarker page)}))))))))

(defn filter-keys-and-date
  "Do a partial reduction collapsing multiple records for a single
  :Key into one, and taking only the first that does not have a
  :LastModified date newer than the given date, also dropping keys
  if the most recent version before date is deleted."
  ([date]
   (fn [rf]
     (let [pv (volatile! nil)]
       (fn
         ([] (rf))
         ([result] (rf result))
         ([result input]
          (let [prior @pv]
            (if (not (pos? (compare (:LastModified input) date)))
              (if (or (nil? prior) (not= (:Key prior) (:Key input)))
                (do
                  (vreset! pv input)
                  (if (:IsDeleteMarker input)
                    result
                    (rf result input)))
                result)
              result)))))))
  ([date coll] (sequence (filter-keys-and-date date) coll)))

;; iterators

(defn s3kv-iterator-read-page
  [client timestamp args]
  (loop [key-marker nil
         version-marker nil]
    (let [args (if (some? key-marker) (assoc args :KeyMarker key-marker) args)
          args (if (some? version-marker) (assoc args :VersionIdMarker version-marker) args)
          result (aws/invoke client {:op :ListObjectVersions
                                     :request (assoc args :MaxKeys *page-size*)})]
      (if (s/valid? ::anomalies/anomaly result)
        (throw (ex-info "failed to read object versions" {:error result}))
        (let [result (dissoc
                       (assoc result :FilteredKeys (vec (filter-keys-and-date timestamp
                                                                              (->> (concat (map #(assoc % :IsDeleteMarker true) (:DeleteMarkers result))
                                                                                           (:Versions result))
                                                                                   (sort-by #(vector (:Key %) (- (.getTime ^Date (:LastModified %)))))
                                                                                   (vec)))))
                       :DeleteMarkers :Versions)]
          (if (and (:IsTruncated result) (empty? (:FilteredKeys result)))
            (recur (:NextKeyMarker result) (:NextVersionIdMarker result))
            result))))))

(defn s3kv-iterator-page-forward
  [{:keys [state snapshot]}]
  (let [{:keys [client bucket timestamp]} snapshot
        {:keys [page index]} @state]
    (log/debug :task ::s3kv-iterator-page-forward
               :page (update page :FilteredKeys #(map :Key %))
               :index index
               :phase :begin)
    (if (or (nil? index)
            (:IsTruncated page))
      (let [next-page (s3kv-iterator-read-page client timestamp
                                               {:Bucket bucket
                                                :KeyMarker (:NextKeyMarker page)
                                                :VersionIdMarker (:NextVersionIdMarker page)})]
        (cond
          (empty? (:FilteredKeys next-page))
          (do
            (swap! state (fn [{:keys [page] :as state}]
                           (assoc state :index (dec (count (:FilteredKeys page))))))
            nil)

          :else
          (swap! state assoc
            :page next-page
            :index 0)))
      (do
        (reset! state {:end true})
        nil))))

(defn s3kv-iterator-do-seek
  [{:keys [state snapshot] :as this} prev-state k]
  (let [{:keys [page index]} @state
        {:keys [client bucket timestamp]} snapshot]
    (log/debug :task ::s3kv-iterator-seek
               :k k
               :phase :begin)
    (if (nil? index)
      (do (s3kv-iterator-page-forward this)
          (recur this nil k))
      (let [found (Collections/binarySearch (map :Key (:FilteredKeys page)) k)]
        (log/debug :task ::s3kv-iterator-seek :found found :phase :probed-current-page)
        (if (neg? found)
          (cond
            ; key is in a page before the current page
            ; rewind to the beginning and scan forward again
            (= -1 found)
            (let [new-page (s3kv-iterator-read-page client timestamp
                                                    {:Bucket bucket})]
              (if (or (empty? (:FilteredKeys new-page))
                      (= -1 (Collections/binarySearch (map :Key (:FilteredKeys new-page)) k)))
                (do
                  (swap! state assoc
                    :page new-page
                    :index 0)
                  (some-> (:FilteredKeys new-page) first :Key hex->buffer))
                (do
                  (swap! state assoc
                    :page new-page
                    :index 0)
                  (recur this nil k))))

            ; key is in a page after the current page
            (= (- (inc (count (:FilteredKeys page)))) found)
            ; key would lie between this page and the next, but we already came from there
            (if (and (some? prev-state)
                     (neg? (compare (:Key (last (:FilteredKeys page))) k))
                     (neg? (compare k (:Key (first (:FilteredKeys (:page prev-state)))))))
                (let [new-state (swap! state assoc
                                       :index (dec (count (:FilteredKeys page))))]
                  (hex->buffer (:Key (nth (:FilteredKeys page) (:index new-state)))))
              (when (s3kv-iterator-page-forward this)
                (recur this state k)))

            ; key's location is in this page, but not exactly in this page
            :else
            (let [new-state (swap! state assoc :index (dec (- found)))]
              (hex->buffer (:Key (nth (:FilteredKeys page) (:index new-state))))))
          (do
            (swap! state assoc :index found)
            (hex->buffer (:Key (nth (:FilteredKeys page) found)))))))))

(defn s3kv-iterator-seek
  [this k]
  (s3kv-iterator-do-seek this nil (buffer->hex (mem/as-buffer k))))

(defn s3kv-iterator-next
  [{:keys [state] :as this}]
  (let [{:keys [index page end]} @state]
    (log/debug :task ::s3kv-iterator-next :index index :phase :begin)
    (cond
      end
      nil

      (nil? index)
      (when-let [new-state (s3kv-iterator-page-forward this)]
        (hex->buffer (:Key (nth (:FilteredKeys (:page new-state)) (:index new-state)))))

      (>= (inc index) (count (:FilteredKeys page)))
      (when-let [new-state (s3kv-iterator-page-forward this)]
        (hex->buffer (:Key (nth (:FilteredKeys (:page new-state)) (:index new-state)))))

      :else
      (let [new-index (:index (swap! state update :index inc))]
        (hex->buffer (:Key (nth (:FilteredKeys page) new-index)))))))

(defn s3kv-iterator-prev
  [{:keys [state snapshot]}]
  (let [{:keys [index page]} @state
        {:keys [client bucket timestamp]} snapshot]
    (log/debug :task ::s3kv-iterator-prev :index index :phase :begin)
    (if (zero? index)
      ; roll back to the start, scan until we are on page before (or containing) current key
      (let [first-key (:Key (first (:FilteredKeys page)))
            first-page (s3kv-iterator-read-page client timestamp {:Bucket bucket})]
        (log/debug :task ::s3kv-iterator-prev :first-key first-key :first-page (map :Key (:FilteredKeys first-page))
                   :phase :rewound-to-start)
        (loop [prev-page first-page]
          (let [found (Collections/binarySearch (map :Key (:FilteredKeys prev-page)) first-key)]
            (log/debug :task ::s3kv-iterator-prev :first-key first-key :prev-page (map :Key (:FilteredKeys prev-page))
                       :found found :phase :starting-loop)
            (cond
              (or (= -1 found) (zero? found))
              (if (identical? first-page prev-page)
                (do (swap! state assoc :index 0)
                    nil)
                (throw (ex-info "BUG: should not happen; somehow iterated past our target key"
                                {:prev-page prev-page
                                 :first-key first-key})))

              (= (- (inc (count (:FilteredKeys prev-page)))) found)
              (if-let [next-page (s3kv-iterator-read-page client timestamp {:Bucket bucket
                                                                            :KeyMarker (:NextKeyMarker prev-page)
                                                                            :VersionIdMarker (:NextVersionIdMarker prev-page)})]
                (let [found2 (Collections/binarySearch (map :Key (:FilteredKeys next-page)) first-key)]
                  (cond
                    (or (= -1 found2) (zero? found2))
                    (let [new-index (dec (count (:FilteredKeys prev-page)))]
                      (swap! state assoc
                        :page prev-page
                        :index new-index)
                      (hex->buffer (:Key (nth (:FilteredKeys prev-page) new-index))))

                    (= (- (inc (count (:FilteredKeys next-page)))) found2)
                    (recur next-page)

                    :else
                    (let [next-index (if (neg? found2)
                                       (dec (dec (- found2)))
                                       (dec found2))]
                      (swap! state assoc
                        :page next-page
                        :index next-index)
                      (hex->buffer (:Key (nth (:FilteredKeys next-page) next-index)))))))

              :else
              (let [new-index (if (neg? found)
                                (dec (dec (- found)))
                                (dec found))]
                (swap! state assoc
                       :page prev-page
                       :index new-index)
                (hex->buffer (:Key (nth (:FilteredKeys prev-page) new-index))))))))
      (let [new-index (:index (swap! state update :index dec))]
        (hex->buffer (:Key (nth (:FilteredKeys page) new-index)))))))

(defn s3kv-iterator-value
  [{:keys [state snapshot]}]
  (let [{:keys [index page]} @state
        {:keys [client bucket]} snapshot]
    (log/debug :task ::s3kv-iterator-value :index index :phase :begin)
    (when (and (some? index)
               (< index (count (:FilteredKeys page))))
      (let [target (nth (:FilteredKeys page) index)
            object (aws/invoke client {:op :GetObject
                                       :request {:Bucket bucket
                                                 :Key (:Key target)
                                                 :VersionId (:VersionId target)}})]
        (if (s/valid? ::anomalies/anomaly object)
          (ex-info "failed to get object" {:error object})
          (if-let [body (:Body object)]
            (mem/with-buffer-out nil (fn [out] (io/copy body out)))
            (mem/as-buffer (byte-array 0))))))))

;; snapshots

(defn s3kv-snapshot-new-iterator
  [{:keys [client bucket timestamp] :as this}]
  (log/debug :task ::s3kv-snapshot-new-iterator :this this :phase :begin)
  (let [state (atom {})
        ctor (resolve 'crux.kv.s3/->S3KvIterator)]
    (ctor state this)))

(defn s3kv-snapshot-get-value
  [{:keys [client bucket timestamp] :as this} k]
  (log/debug :task ::s3kv-snapshot-get-value :this this :k k :phase :begin)
  (let [k (buffer->hex (mem/as-buffer k))
        version (->> (versions-seq client bucket {:Prefix k})
                     (filter #(= k (:Key %)))
                     (filter #(<= 0 (compare timestamp (:LastModified %))))
                     (first))]
    (when (and (some? version)
               (not (:IsDeleteMarker version)))
      (let [object (aws/invoke client {:op :GetObject
                                       :request {:Bucket bucket
                                                 :Key k
                                                 :VersionId (:VersionId version)}})]
        (if (s/valid? ::anomalies/anomaly object)
          (throw (ex-info "failed to read object" {:error object}))
          (when-let [body (:Body object)]
            (mem/with-buffer-out nil (fn [out] (io/copy body out)))))))))

;; kv store

(defn s3kv-open
  [this options]
  (log/debug :task ::s3kv-open :options options :phase :begin)
  (let [client (aws/client (assoc options :api :s3))
        result (assoc this :client client
                           :bucket (:bucket options (:db-dir options)))]
    result))

(defn s3kv-new-snapshot
  [{:keys [client bucket]}]
  (log/debug :task ::s3kv-new-snapshot :phase :begin)
  (let [ctor (resolve 'crux.kv.s3/->S3KvSnapshot)
        timestamp (Date.)]
    (ctor client bucket timestamp)))

(defn s3kv-store [{:keys [client bucket]} kvs]
  (log/debug :task ::s3kv-store :kvs kvs :phase :begin)
  (doseq [[k v] kvs]
    (let [k (buffer->hex (mem/as-buffer k))
          v (mem/as-buffer v)
          response (aws/invoke client {:op      :PutObject
                                       :request {:Bucket bucket
                                                 :Key    k
                                                 :Body   (DirectBufferInputStream. v)}})]
      (log/debug :task ::s3kv-store :k k :v v :result response)
      (when (s/valid? ::anomalies/anomaly response)
        (throw (ex-info "storing object failed" {:error response}))))))

(defn s3kv-delete [{:keys [client bucket]} ks]
  (log/debug :task ::s3kv-delete :ks ks :phase :begin)
  (doseq [k ks]
    (let [response (aws/invoke client {:op      :DeleteObject
                                       :request {:Bucket bucket
                                                 :Key (buffer->hex (mem/as-buffer k))}})]
      (when (s/valid? ::anomalies/anomaly response)
        (throw (ex-info "deleting object failed" {:error response}))))))

(defn s3kv-count-keys [{:keys [client bucket]}]
  (log/debug :task ::s3kv-count-keys :phase :begin)
  (loop [token nil
         cnt 0]
    (let [response (aws/invoke client {:op      :ListObjectsV2
                                       :request {:Bucket bucket
                                                 :ContinuationToken token}})]
      (if (s/valid? ::anomalies/anomaly response)
        (throw (ex-info "failed to list objects" {:error response}))
        (let [new-cnt (+ cnt (count (:Contents response)))]
          (if (:IsTruncated response)
            (recur (:NextContinuationToken response) new-cnt)
            new-cnt))))))

