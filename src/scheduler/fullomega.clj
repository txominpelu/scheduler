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

;;(defn drf
;;  [cluster demands] 
;;  (let [totalResources (fullresources/aggregatedResources (cluster/getResources cluster))
;;        frameworks (map (fn [d] (keyword (task/getFramework d))) demands)
;;        dominantShares (utils/tuplesToMap (map vector frameworks (repeat 0)))
;;        resourcesGiven (utils/tuplesToMap (map vector frameworks (repeat resources/emptyResources)))
;;        aggDemands (map (fn [d] (task/withResources d (fullresources/aggregatedResources (task/getResources d)))) demands)
;;        demandsMap (utils/tuplesToMap (map (fn [[key val]] [(keyword key) val]) (group-by task/getFramework aggDemands)))
;;        ]
;;    (omega/internalDrf 
;;         totalResources 
;;         resources/emptyResources 
;;         dominantShares 
;;         resourcesGiven
;;         demandsMap
;;         [])))


;;(def totRes {:slave1 {:cpus 10 :memory 8}}) 
;;(def consRes {:slave1 {:cpus 8 :memory 0}}) 
;;(def dominantShares {:fr1 0 :fr2 0}) 
;;(def resGiven {:fr1 {:slave1 {:cpus 0 :memory 0}} :fr2 {:slave1 {:cpus 0 :memory 0}}}) 
;;(def demandsMap {:fr1 [d1 d2 d3] :fr2 [d5 d6 d7]})

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


