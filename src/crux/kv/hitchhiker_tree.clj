(ns crux.kv.hitchhiker-tree
  (:require [clojure.core.async :as async]
            [crux.kv :as kv]
            [crux.lru :as lru]
            [hitchhiker.tree :as hh]
            [hitchhiker.tree.utils.async :as ha]
            [konserve.core :as k]
            [hitchhiker.tree.messaging :as hmsg]
            [hitchhiker.tree.bootstrap.konserve :as kons]
            [hitchhiker.tree.key-compare :as c]
            [hitchhiker.tree.node :as n]
            [crux.memory :as mem]
            [superv.async :as sv]
            [clojure.tools.logging :as log])
  (:import [java.io Closeable]))

(def ^:const br 300)
(def ^:const br-sqrt (long (Math/sqrt br)))

(defn load-tree
  [konserve]
  (ha/go-try
    (let [get-chan (k/get-in konserve [:kv])
          tree (ha/if-async?
                 (ha/<? get-chan)
                 (async/<!! get-chan))]
      (or tree (ha/<?? (hh/b-tree (hh/->Config br-sqrt br br)))))))

(defn left-successor
  [path]
  (ha/go-try
    (when-let [common-parent-path
               (hh/backtrack-up-path-until path
                                           (fn [_ index]
                                             (< -1 (dec index))))]
      (let [next-index (-> common-parent-path peek dec)
            parent (-> common-parent-path pop peek)
            new-sibling (-> (nth (:children parent) next-index)
                            hh/<?-resolve)
            sibling-lineage (loop [res (transient [new-sibling])
                                   s new-sibling]
                              (let [last-child (-> s :children last)]
                                (if (n/address? last-child)
                                  (let [resolved-first-child (hh/<?-resolve last-child)]
                                    (when (n/address? resolved-first-child)
                                      (recur (conj! res resolved-first-child)
                                             resolved-first-child)))
                                  (persistent! res))))
            path-suffix (-> (mapcat (fn [node]
                                      [node (dec (count (:children node)))])
                                    sibling-lineage)
                            ;; butlast ensures we end w/ node
                            (butlast))]
        (-> (pop common-parent-path)
            (conj next-index)
            (into path-suffix))))))

(deftype HitchhikerTreeKvIteratorEnd [])
(def +end+ (->HitchhikerTreeKvIteratorEnd))

(extend-protocol n/IEDNOrderable
  HitchhikerTreeKvIteratorEnd
  (-order-on-edn-types [_] -10000))

(extend-protocol c/IKeyCompare
  HitchhikerTreeKvIteratorEnd
  (-compare [this that] (if (= (type this) (type that))
                          0
                          1)))

(defrecord HitchhikerTreeKvIterator [root state]
  kv/KvIterator
  (seek [_ k]
    (let [k (mem/->on-heap k)]
      (when-let [path (hh/lookup-path root k)]
        (let [res (drop-while #(neg? (c/-compare (first %) k)) (hmsg/apply-ops-in-path path))]
          (vreset! state {:path path :key (ffirst res)})
          (mem/as-buffer (ffirst res))))))

  (next [this]
    (if (= ::nothing @state)
      (kv/seek this nil)
      (loop [{:keys [path key]} @state]
        (if-let [res (first (drop-while #(<= (c/-compare (first %) key) 0)
                                        (hmsg/apply-ops-in-path path)))]
          (do
            (vswap! state assoc :key (first res))
            (mem/as-buffer (first res)))
          (if-let [path (ha/<?? (hh/right-successor (pop path)))]
            (recur (vswap! state assoc :path path))
            (do
              (vswap! state assoc :key +end+)
              nil))))))

  (prev [_]
    (when-not (= ::nothing @state)
      (loop [{:keys [path key]} @state]
        (if-let [res (first (drop-while #(>= (c/-compare (first %) key) 0)
                                        (reverse (hmsg/apply-ops-in-path path))))]
          (do
            (vswap! state assoc :key (first res))
            (mem/as-buffer (first res)))
          (if-let [path (ha/<?? (left-successor (pop path)))]
            (recur (vswap! state assoc :path path))
            (do
              (vswap! state assoc :key nil)
              nil))))))

  (value [_]
    (let [state @state]
      (when (not= ::nothing state)
        (some-> (drop-while #(neg? (c/-compare (first %) (:key state)))
                            (hmsg/apply-ops-in-path (:path state)))
                (first)
                (second)
                (mem/as-buffer)))))

  Closeable
  (close [_]))

(defrecord HitchhikerTreeKvSnapshot [root]
  Closeable
  (close [_])

  kv/KvSnapshot
  (new-iterator ^Closeable [_]
    (->HitchhikerTreeKvIterator root (volatile! ::nothing)))

  (get-value [_ k]
    (some-> (hmsg/lookup root (mem/->on-heap k)) mem/as-buffer)))

(defrecord HitchhikerTreeKv [tree konserve]
  kv/KvStore
  (open [this options]
    (let [konserve (get options :konserve konserve)
          root (ha/<?? (load-tree konserve))]
      (assoc this :konserve konserve
                  :tree (volatile! root))))

  (new-snapshot ^Closeable
    [_]
    (->HitchhikerTreeKvSnapshot @tree))

  (store [_ kvs]
    (log/debug {:task ::kv/store :phase :begin :kvs (pr-str kvs)})
    (locking tree
      (vswap! tree #(reduce (fn [t [k v]]
                              (ha/<?? (hmsg/insert t
                                                   (mem/->on-heap k)
                                                   (mem/->on-heap v))))
                            % kvs))))

  (delete [_ ks]
    (locking tree
      (vswap! tree #(reduce (fn [t k] (ha/<?? (hmsg/delete t (mem/->on-heap k)))) % ks))))

  (fsync [_]
    (log/debug {:task ::kv/fsync :phase :begin})
    (locking tree
      (vswap! tree
              (fn [t]
                (let [backend (kons/->KonserveBackend konserve)
                      new-tree (:tree (ha/<?? (hh/flush-tree-without-root t backend)))]
                  (sv/<?? sv/S (k/assoc-in konserve [:kv] new-tree))
                  new-tree)))))

  (backup [this dir])

  (count-keys [this])

  (db-dir [this])

  (kv-name [this] "hitchhiker-tree")

  Closeable
  (close [_]))

(def kv {:start-fn (fn [{konserve :crux.hitchhiker.tree/konserve} options]
                     (lru/start-kv-store (->HitchhikerTreeKv nil konserve) options))
         :deps [:crux.hitchhiker.tree/konserve]
         :args lru/options})
