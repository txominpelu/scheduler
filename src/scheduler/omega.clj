(ns scheduler.omega
  (:require [clojure.core.async :as async]
            [scheduler.channel :as channel]
            [scheduler.cluster :as cluster]
            [scheduler.framework :as framework]))

;; Omega 

(defn resourcesAvailable?
  [cluster resources]
  "returns if the resources are available in the cluster"
  (>= (cluster/getClusterCpus cluster) (cluster/getCpus resources)))

(defn commitResources
  [cluster resources]
  "returns the new state of the cluster after the resources are commited"
  (cluster/substractResources cluster resources))

;; Demands
 
;; Demand = {:resources {:cpus 1}}
(defn getResourcesNeeded
  [demand]
  (:resources demand))

;; Cluster

(defn getDemandsCh
  [cluster]
  (:demandsCh cluster))

(def omegaIter (fn [cluster] 
           (omegaIter cluster)))

;; TODO: Add incremental
(defn tryCommitDemand 
  [cluster demand]
  " reads one demand and alters the state of the cluster if needed "
  (let [ neededRes  (getResourcesNeeded demand) ]
    (if (resourcesAvailable? neededRes cluster)
      (commitResources neededRes cluster)
      cluster)))

(defn omegaIter 
  [cluster]
  (let [finishedCh (cluster/getFinishedCh cluster) 
        demandsCh  (getDemandsCh cluster)
        events (channel/readAll [finishedCh demandsCh])
        demands (filter (channel/belongsTo? demandsCh) events)
        finished (filter (channel/belongsTo? finishedCh) events)
        resourcesFreed (map getResourcesNeeded finished)  
        cluster (cluster/addResources cluster (reduce cluster/addResources cluster resourcesFreed))]
        (reduce tryCommitDemand cluster demands)))


;; Frameworks

;; Test1: 

;; Test that without priorities there can be undesired situations
;; Test that priorities allow both batch jobs and services to work properly
;; Show that sometimes priorities are not enough (alternative?)


