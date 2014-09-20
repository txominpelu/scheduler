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

;; Algo:
;; find fr with lowest share
;; try to find an offer that suits him (still not considering flexibility)
;; if I can't make an an offer (no resources) 
;;   retry drf without that framework  
;; else
;;   update values and retry drf

;; Fullshares just aggregates resources and then calls shares

(defn fullshares
  [totRes resGiven]
  (let [aggResGiven (for [[fr res] resGiven] [fr (fullresources/aggregatedResources res)])]
    (omega/shares (utils/tuplesToMap aggResGiven) (fullresources/aggregatedResources totRes) )))


(defn internalDrf
  [totRes consRes domShares resGiven demandsMap sortedDemands]
  (let [i (first (reduce omega/minShares domShares)) ;;
        di (first (i demandsMap))
        diRes (task/getResources di)
        ui (i resGiven)
        newDemandsMap (omega/withDemands demandsMap i (rest (i demandsMap)))
        demandsLeft (apply concat (map (fn [[k v]] v) newDemandsMap))
        newSortedDemands (conj sortedDemands di) 
        ]
    (if (fullresources/canMakeOfferFor (fullresources/minusResources totRes consRes) diRes)
      (let [offer (fullresources/makeOfferFor (fullresources/minusResources totRes consRes) diRes)
            newResGiven (omega/withResources resGiven i (fullresources/plusResources ui offer))
            newDomShares (fullshares totRes newResGiven)
            newConsRes (fullresources/plusResources consRes offer)
            ]
        (do 
        (internalDrf totRes newConsRes newDomShares newResGiven newDemandsMap newSortedDemands)))
        (concat newSortedDemands demandsLeft))))


;; Test1: 

;; Test that without priorities there can be undesired situations
;; Test that priorities allow both batch jobs and services to work properly
;; Show that sometimes priorities are not enough (alternative?)


