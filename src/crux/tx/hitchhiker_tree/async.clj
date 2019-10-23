(ns crux.tx.hitchhiker-tree.async
  (:require hitchhiker.tree.async))

; is this nonsense actually necessary?

(alter-var-root #'hitchhiker.tree.async/*async-backend* (constantly :core.async))