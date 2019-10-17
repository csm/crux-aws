(ns itc
  "WIP, interval tree clock impl.")

(defn ->stamp
  ([] (->stamp 1 0))
  ([id event] [id event]))

(defn split
  [id]
  (if (integer? id)
    (if (zero? id)
      [0 0]
      [[1 0] [0 1]])
    (let [[l r] id]
      (cond (zero? l)
            (let [[rs1 rs2] (split r)]
              [[0 rs1] [0 rs2]])

            (zero? r)
            (let [[ls1 ls2] (split l)]
              [[ls1 0] [ls2 0]])

            :else [[l 0] [0 r]]))))

(defn fork
  [s]
  (let [[id event] s
        [id1 id2] (split id)]
    [[id1 event] [id2 event]]))

(defn peek
  [s]
  (let [[id event] s]
    [[id event] [0 event]]))

(defn- normalize
  [id]
  (if (integer? id)
    id
    (let [[l r] id
          l (normalize l)
          r (normalize r)]
      (cond (and (integer? l) (zero? l) (integer? r) (zero? r)) 0
            (and (= 1 l) (= 1 r)) 1
            :else [l r]))))

(defn- sum
  [id1 id2]
  (if (integer? id1)
    (cond (zero? id1) id2
          (and (integer? id2) (zero? id2)) id1
          :else (throw (IllegalArgumentException. (str "can't sum " id1 " with " id2))))
    (cond (and (integer? id2) (zero? id2)) id1
          (not (integer? id2)) (let [[l1 r1] id1
                                     [l2 r2] id2
                                     ls (sum l1 l2)
                                     rs (sum r1 r2)]
                                 (normalize [ls rs])))))

(defn- event-join
  [e1 e2]
  (if (integer? e1)
    (if (integer? e2)
      (max e1 e2)
      (event-join [e1 0 0] e2))
    (if (integer? e2)
      (event-join e1 [e2 0 0])
      (if (> (first e1) (first e2))
        (event-join e2 e1)
        (normalize [(first e1)])))))

(defn join
  [s1 s2]
  (let [[id1 event1] s1
        [id2 event2] s2
        id-sum (sum id1 id2)
        event-join (event-join event1 event2)]
    [id-sum event-join]))