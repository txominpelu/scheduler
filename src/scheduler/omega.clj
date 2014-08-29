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

;; Demands
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


;; Frameworks

;; Test1: 

;; Test that without priorities there can be undesired situations
;; Test that priorities allow both batch jobs and services to work properly
;; Show that sometimes priorities are not enough (alternative?)


