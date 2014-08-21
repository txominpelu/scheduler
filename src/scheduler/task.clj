(ns scheduler.task
  (:require 
            [clojure.core.typed :as t]
            [scheduler.types :as ts]
            [scheduler.resources :as resources]
  ))


(t/ann getResources [ts/Demand -> ts/Resources])
(defn getResources
  [demand]
  (:resources demand))

(t/ann getCpus [ts/Demand -> t/AnyInteger])
(defn getCpus
  [demand]
  (:cpus (:resources demand)))


(t/ann getTask [ts/Demand -> ts/Task])
(defn getTask
  [demand]
  (:task demand))



(t/ann plusResources [ts/Resources ts/Resources -> ts/Resources])
(defn plusResources
  [res1 res2]
  {:cpus (+ (resources/getCpus res1) (resources/getCpus res2))})

(t/ann minusResources [ts/Resources ts/Resources -> ts/Resources])
(defn minusResources
  [res1 res2]
  {:cpus (- (resources/getCpus res1) (resources/getCpus res2))})

(t/ann resourcesUsedBy [(t/Seqable ts/Demand) -> ts/Resources])
(defn resourcesUsedBy
  [demands]
  (reduce plusResources {:cpus 0} (map getResources demands)))
