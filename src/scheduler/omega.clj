(ns scheduler.omega
  (:require [clojure.core.async :as async]
            [clojure.core.typed :as t]
            [clojure.core.typed.async :as ta]
            [scheduler.types :as ts]
            [scheduler.channel :as channel]
            [scheduler.cluster :as cluster]
            [scheduler.framework :as framework]))

;; Omega 

(t/ann resourcesAvailable? [ts/Cluster ts/Resources -> Boolean])
(defn resourcesAvailable?
  [cluster resources]
  "returns if the resources are available in the cluster"
  (>= (cluster/getClusterCpus cluster) (cluster/getCpus resources)))

(t/ann commitResources [ts/Cluster ts/Resources -> ts/Cluster])
(defn commitResources
  [cluster resources]
  "returns the new state of the cluster after the resources are commited"
  (cluster/substractResources cluster resources))

;; Demands
 
;; Demand = {:resources {:cpus 1}}
(t/ann getResourcesNeeded [ts/OmegaDemand -> ts/Resources])
(defn getResourcesNeeded
  [demand]
  (:resources demand))

;; Cluster

(t/ann getDemandsCh [ts/Cluster -> (ta/Chan t/Any)])
(defn getDemandsCh
  [cluster]
  (:demandsCh cluster))

;; TODO: Add incremental
(t/ann tryCommitDemand [ts/Cluster ts/OmegaDemand -> ts/Cluster])
(defn tryCommitDemand 
  [cluster demand]
  " reads one demand and alters the state of the cluster if needed "
  (let [ neededRes  (getResourcesNeeded demand) ]
    (if (resourcesAvailable? neededRes cluster)
      (commitResources cluster neededRes)
      cluster)))

(t/ann omegaIter [ts/Cluster -> ts/Cluster])
(defn omegaIter 
  [cluster]
  (let [finishedCh (cluster/getFinishedCh cluster) 
        demandsCh  (getDemandsCh cluster)
        events  (channel/readAll [finishedCh demandsCh])
        demands (filter (channel/belongsTo? demandsCh) events)
        finished (filter (channel/belongsTo? finishedCh) events)
        resourcesFreed (map getResourcesNeeded finished)  
        cluster (reduce cluster/addResources cluster resourcesFreed)]
        (reduce tryCommitDemand cluster demands)))


;; Frameworks

;; Test1: 

;; Test that without priorities there can be undesired situations
;; Test that priorities allow both batch jobs and services to work properly
;; Show that sometimes priorities are not enough (alternative?)


