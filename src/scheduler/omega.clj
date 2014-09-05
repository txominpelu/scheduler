(ns scheduler.omega
  (:require [clojure.core.async :as async]
            [clojure.core.typed :as t]
            [clojure.core.typed.async :as ta]
            [scheduler.types :as ts]
            [scheduler.channel :as channel]
            [scheduler.cluster :as cluster]
            [scheduler.task :as task]
            [scheduler.utils :as utils]
            [scheduler.resources :as resources]
            [scheduler.framework :as framework]))

(t/ann omegaIter [ts/Cluster -> ts/Cluster])
(defn omegaIter 
  [sorting]
  (fn [cluster]
    (let [finishedCh (cluster/getFinishedCh cluster) 
        demandsCh  (cluster/getDemandsCh cluster)
        events  (channel/readAll [finishedCh demandsCh])
        demands (channel/belongingTo events demandsCh)
        finishedTasks (channel/belongingTo events finishedCh)
        finishedRes (map task/getResources finishedTasks)
        resourcesFreed (reduce resources/plusResources finishedRes) 
        cluster (cluster/addResources cluster resourcesFreed) ]
        (reduce cluster/tryCommitDemand {:cluster cluster :logs []} (sorting cluster demands)))))


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

(defn withDemands
  [demandsMap fr demands]
  (assoc demandsMap fr demands))

(defn internalDrf
  [totRes consRes domShares resGiven demandsMap sortedDemands]
  (let [i (first (reduce minShares domShares)) ;;
        di (first (i demandsMap))
        resDi (task/getResources di)
        newDemandsMap (withDemands demandsMap i (rest (i demandsMap)))
        newConsRes (resources/plusResources consRes resDi)
        ui (i resGiven)
        newResGiven (withResources resGiven i (resources/plusResources ui resDi))
        newDomShares (shares newResGiven totRes)
        newSortedDemands (conj sortedDemands di) 
        demandsLeft (apply concat (map (fn [[k v]] v) newDemandsMap))
        ]
    (if (resources/<= newConsRes totRes)
        (internalDrf totRes newConsRes newDomShares newResGiven newDemandsMap newSortedDemands)
        (concat newSortedDemands demandsLeft)
      )))


(defn drf
  [cluster demands] 
  (let [totalResources (cluster/getResources cluster)
        frameworks (map (fn [d] (keyword (task/getFramework d))) demands)
        dominantShares (utils/tuplesToMap (map vector frameworks (repeat 0)))
        resourcesGiven (utils/tuplesToMap (map vector frameworks (repeat resources/emptyResources)))
        demandsMap (utils/tuplesToMap (map (fn [[key val]] [(keyword key) val]) (group-by task/getFramework demands)))
        ]
    (internalDrf totalResources 
         resources/emptyResources 
         dominantShares 
         resourcesGiven
         demandsMap
         [])))

;; Test1: 

;; Test that without priorities there can be undesired situations
;; Test that priorities allow both batch jobs and services to work properly
;; Show that sometimes priorities are not enough (alternative?)


