(ns crux.tx.hitchhiker-tree.async
  (:require hitchhiker.tree.async))

; is this nonsense actually necessary?

(binding [hitchhiker.tree.async/*async-backend* :core.async]
  (require 'hitchhiker.tree.core))