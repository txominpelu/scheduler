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


(t/ann plusResources [ts/Resources ts/Resources -> ts/Resources])
(defn plusResources
  [res1 res2]
  {:cpus (+ (getCpus res1) (getCpus res2))})

(t/ann minusResources [ts/Resources ts/Resources -> ts/Resources])
(defn minusResources
  [res1 res2]
  {:cpus (- (getCpus res1) (getCpus res2))})

