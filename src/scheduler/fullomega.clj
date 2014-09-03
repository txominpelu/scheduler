(ns scheduler.fullomega
  (:require [clojure.core.async :as async]
            [clojure.core.typed :as t]
            [clojure.core.typed.async :as ta]
            [scheduler.types :as ts]
            [scheduler.channel :as channel]
            [scheduler.cluster :as cluster]
            [scheduler.task :as task]
            [scheduler.utils :as utils]
            [scheduler.resources :as resources]
            [scheduler.fullresources :as fullresources]
            [scheduler.omega :as omega]
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
        resourcesFreed (reduce fullresources/plusResources finishedRes) 
        cluster (cluster/addFullResources cluster resourcesFreed) ]
        (reduce cluster/tryFullCommitDemand {:cluster cluster :logs []} (sorting cluster demands)))))

(defn drf
  [cluster demands] 
  (let [totalResources (fullresources/aggregatedResources (cluster/getResources cluster))
        frameworks (map (fn [d] (keyword (task/getFramework d))) demands)
        dominantShares (utils/tuplesToMap (map vector frameworks (repeat 0)))
        resourcesGiven (utils/tuplesToMap (map vector frameworks (repeat resources/emptyResources)))
        demandsMap (utils/tuplesToMap (map (fn [[key val]] [(keyword key) val]) (group-by task/getFramework demands)))
        ]
    (println "totalResources")
    (println totalResources)
    (omega/internalDrf totalResources 
         resources/emptyResources 
         dominantShares 
         resourcesGiven
         demandsMap
         [])))


;; Test1: 

;; Test that without priorities there can be undesired situations
;; Test that priorities allow both batch jobs and services to work properly
;; Show that sometimes priorities are not enough (alternative?)


