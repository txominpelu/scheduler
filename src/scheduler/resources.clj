(ns scheduler.resources
  (:require 
            [clojure.core.typed :as t]
            [scheduler.types :as ts]
  ))


;; Resources

(t/ann getCpus [ts/Resources -> t/AnyInteger])
(defn getCpus
  [resources]
  (:cpus resources))

(t/ann getMemory [ts/Resources -> t/AnyInteger])
(defn getMemory
  [resources]
  (:memory resources))

(t/ann <= [ts/Resources ts/Resources -> Boolean])
(defn <=
  [r1 r2]
  (and
    (clojure.core/<= (getMemory r1) (getMemory r2))
    (clojure.core/<= (getCpus r1) (getCpus r2))))

(t/ann plusResources [ts/Resources ts/Resources -> ts/Resources])
(defn plusResources
      ([]
       {:cpus 0 :memory 0})
      ([{cpus :cpus memory :memory} {cpus2 :cpus memory2 :memory}]
               {:cpus (+ cpus cpus2) :memory (+ memory memory2)} ))

(t/ann minusResources [ts/Resources ts/Resources -> ts/Resources])
(defn minusResources
  [res1 res2]
  {:cpus (- (getCpus res1) (getCpus res2))
   :memory (- (getMemory res1) (getMemory res2))})

(def emptyResources {:cpus 0 :memory 0}) 
