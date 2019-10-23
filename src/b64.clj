(ns b64
  "Modified base-64 encoding, which preserves sort order
  for input bytes."
  (:require [clojure.set :refer [map-invert]]
            [clojure.string :as string])
  (:import [java.util Base64]))

(def ^:private b64-dec
  {\- \=,
   \0 \A,
   \1 \B,
   \2 \C,
   \3 \D,
   \4 \E,
   \5 \F,
   \6 \G,
   \7 \H,
   \8 \I,
   \9 \J,
   \A \K,
   \B \L,
   \C \M,
   \D \N,
   \E \O,
   \F \P,
   \G \Q,
   \H \R,
   \I \S,
   \J \T,
   \K \U,
   \L \V,
   \M \W,
   \N \X,
   \O \Y,
   \P \Z,
   \Q \a,
   \R \b,
   \S \c,
   \T \d,
   \U \e,
   \V \f,
   \W \g,
   \X \h,
   \Y \i,
   \Z \j,
   \_ \k,
   \a \l,
   \b \m,
   \c \n,
   \d \o,
   \e \p,
   \f \q,
   \g \r,
   \h \s,
   \i \t,
   \j \u,
   \k \v,
   \l \w,
   \m \x,
   \n \y,
   \o \z,
   \p \0,
   \q \1,
   \r \2,
   \s \3,
   \t \4,
   \u \5,
   \v \6,
   \w \7,
   \x \8,
   \y \9,
   \z \+,
   \~ \/})

(def b64-enc (map-invert b64-dec))

(defn b64-encode
  [b]
  (->> (.encodeToString (Base64/getEncoder) b)
       (map b64-enc)
       (string/join)))

(defn b64-decode
  [s]
  (->> (map b64-dec s)
       (string/join)
       (.decode (Base64/getDecoder))))