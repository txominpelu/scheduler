(ns scheduler.omega
  (:require [clojure.core.async :as async]
            [clojure.core.typed :as t]
            [clojure.core.typed.async :as ta]
            [scheduler.types :as ts]
            [scheduler.channel :as channel]
            [scheduler.cluster :as cluster]
            [scheduler.task :as task]
            [scheduler.resources :as resources]
            [scheduler.framework :as framework]))

(t/ann omegaIter [ts/Cluster -> ts/Cluster])
(defn omegaIter 
  [cluster]
  (let [finishedCh (cluster/getFinishedCh cluster) 
        demandsCh  (cluster/getDemandsCh cluster)
        events  (channel/readAll [finishedCh demandsCh])
        demands (channel/belongingTo events demandsCh)
        finishedTasks (channel/belongingTo events finishedCh)
        finishedRes (map task/getResources finishedTasks)
        resourcesFreed (reduce resources/plusResources finishedRes) 
        cluster (cluster/addResources cluster resourcesFreed) ]
        (reduce cluster/tryCommitDemand {:cluster cluster :logs []} demands)))


(def totalResources {:cpus 9 :memory 18}) 
(def consumedResources {:cpus 0 :memory 0}) 
(def dominantShares {:fr1 0 :fr2 0}) 
(def resourcesGiven {:fr1 {:cpus 0 :memory 0} :fr2 {:cpus 0 :memory 0}}) 
(def demands {:fr1 {:cpus 1 :memory 4} :fr2 {:cpus 3 :memory 1}})

(defn shares
  [resGiven totRes]
  (let [shs (map (fn [[fr ui]] [fr (apply max (map (fn [[j uij]] (/ uij (j totRes))) ui))]) resGiven)]
    (into {} shs)))

(defn minShares
  [[minFr minSh] [fr sh]]
  (if (> minSh sh)
    [fr sh]
    [minFr minSh]))

(defn withResources
  [resGiven fr res]
  (assoc resGiven fr res))

(defn drf
  [totRes consRes domShares resGiven]
  (let [i (first (reduce minShares domShares)) ;;
        di (i demands)
        newConsRes (resources/plusResources consRes di)
        ui (i resGiven)
        newResGiven (withResources resGiven i (resources/plusResources ui di))
        newDomShares (shares newResGiven totRes)]
    (if (resources/<= newConsRes totRes)
      (do 
        (println (str "Given to:" i))
        (drf totRes newConsRes newDomShares newResGiven)))))


;; Test1: 

;; Test that without priorities there can be undesired situations
;; Test that priorities allow both batch jobs and services to work properly
;; Show that sometimes priorities are not enough (alternative?)


