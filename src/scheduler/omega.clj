(ns scheduler.omega
  (:require [clojure.core.async :as async]
            [clojure.core.typed :as t]
            [clojure.core.typed.async :as ta]
            [scheduler.types :as ts]
            [scheduler.channel :as channel]
            [scheduler.cluster :as cluster]
            [scheduler.resources :as resources]
            [scheduler.framework :as framework]))

;; Omega 

(t/ann resourcesAvailable? [ts/Cluster ts/Resources -> Boolean])
(defn resourcesAvailable?
  [cluster resources]
  "returns if the resources are available in the cluster"
  (>= (cluster/getClusterCpus cluster) (resources/getCpus resources)))

(t/ann commitResources [ts/Cluster ts/Resources -> ts/Cluster])
(defn commitResources
  [cluster resources]
  "returns the new state of the cluster after the resources are commited"
  (cluster/substractResources cluster resources))

;; Demands
 
;; Demand = {:resources {:cpus 1} :task (fn [] )}
(t/ann getResourcesNeeded [ts/Demand -> ts/Resources])
(defn getResourcesNeeded
  [demand]
  (:resources demand))

(t/ann getTask [ts/Demand -> [ -> t/Any]])
(defn getTask
  [demand]
  (:task demand))

;; Cluster
;; TODO: Add incremental
(t/ann tryCommitDemand [ts/Cluster ts/Demand -> ts/Cluster])
(defn tryCommitDemand 
  [cluster demand]
  " reads one demand and alters the state of the cluster if needed "
  (let [ neededRes  (getResourcesNeeded demand) 
         task (getTask demand)]
    (if (resourcesAvailable? cluster neededRes)
      (do
        (task)
        (commitResources cluster neededRes))
      cluster)))

(t/ann omegaIter [ts/Cluster -> ts/Cluster])
(defn omegaIter 
  [cluster]
  (let [finishedCh (cluster/getFinishedCh cluster) 
        demandsCh  (cluster/getDemandsCh cluster)
        events  (channel/readAll [finishedCh demandsCh])
        demands (channel/belongingTo events demandsCh)
        finished (channel/belongingTo events finishedCh)]
    (let
        [resourcesFreed (map getResourcesNeeded finished)  
        cluster (reduce cluster/addResources cluster resourcesFreed)]
        (reduce tryCommitDemand cluster demands))))


;; Frameworks

;; Test1: 

;; Test that without priorities there can be undesired situations
;; Test that priorities allow both batch jobs and services to work properly
;; Show that sometimes priorities are not enough (alternative?)


