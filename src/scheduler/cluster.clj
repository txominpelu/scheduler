(ns scheduler.cluster
  (:require [clojure.core.async :as async]
            [clojure.core.typed.async :as ta]
            [clojure.core.typed :as t]
            [scheduler.types :as ts]
     ))

;; Resources

(t/ann getCpus [ts/Resources -> t/AnyInteger])
(defn getCpus
  [resources]
  (:cpus resources))

;; Cluster




(t/ann getResources [ts/Cluster -> ts/Resources])
(defn getResources
  [cluster]
  (:resources cluster))

(t/ann getClusterCpus [ts/Cluster -> t/AnyInteger])
(defn getClusterCpus
  [cluster]
  (getCpus (getResources cluster)))

(t/ann getFrameworks [ts/Cluster -> (t/Seqable ts/Framework)])
(defn getFrameworks
  [cluster]
  (:frameworks cluster))

(t/ann getRegisterCh [ts/Cluster -> (ta/Chan t/Any)])
(defn getRegisterCh
  [cluster]
  (:registerCh cluster))


(t/ann getDemandsCh [ts/Cluster -> (ta/Chan t/Any)])
(defn getDemandsCh
  [cluster]
  (:demandsCh cluster))

(t/ann getFinishedCh [ts/Cluster -> (ta/Chan t/Any)])
(defn getFinishedCh
  [cluster]
  (:finishedCh cluster))

(t/ann getIter [ts/Cluster -> [ts/Cluster -> t/Any]])
(defn getIter
  [cluster]
  (:iter cluster))

(t/ann registerFramework [ts/Cluster ts/Framework -> t/Any])
(defn registerFramework
  [cluster framework] 
  (async/thread (async/>!! (getRegisterCh cluster) framework)))

(t/ann finishFramework [ts/Cluster ts/Framework -> t/Any])
(defn finishFramework
  [cluster framework] 
  (async/thread (async/>!! (getFinishedCh cluster) framework)))

(t/ann withResources [ts/Cluster ts/Resources -> ts/Cluster])
(defn withResources
  [cluster resources]
  (assoc cluster :resources resources))

(t/ann withFrameworks [ts/Cluster (t/Seqable ts/Framework) -> ts/Cluster])
(defn withFrameworks
  [cluster frameworks]
  (assoc cluster :frameworks frameworks))

(t/ann runIter [ts/Cluster -> t/Any])
(defn runIter
  [cluster]
  ((getIter cluster) cluster))

;; Tasks

(t/ann runTask [ts/Task -> t/Any])
(defn runTask
  [task]
 (task))


(t/ann plusResources [ts/Resources ts/Resources -> ts/Resources])
(defn plusResources
  [res1 res2]
  {:cpus (+ (getCpus res1) (getCpus res2))})

(t/ann minusResources [ts/Resources ts/Resources -> ts/Resources])
(defn minusResources
  [res1 res2]
  {:cpus (- (getCpus res1) (getCpus res2))})

(t/ann addResources [ts/Cluster ts/Resources -> ts/Cluster])
(defn addResources
  [cluster resources]
  (withResources cluster (plusResources (getResources cluster) resources)))

(t/ann substractResources [ts/Cluster ts/Resources -> ts/Cluster])
(defn substractResources
  [cluster resources]
  (withResources cluster (minusResources (getResources cluster) resources)))

;; running
(t/ann initOmegaCluster [[t/Any -> t/Any] -> ts/Cluster])
(defn initOmegaCluster
  [iter]
  {:iter iter
   :resources {:cpus 10} 
   :frameworks [] 
   :registerCh (async/chan)
   :demandsCh (async/chan)
   :finishedCh (async/chan)
   })

(t/ann initMesosCluster [[t/Any -> t/Any] -> ts/Cluster])
(defn initMesosCluster
  [iter]
  {:iter iter
   :resources {:cpus 10} 
   :frameworks [] 
   :registerCh (async/chan)
   :demandsCh (async/chan)
   :finishedCh (async/chan)
   })

(t/ann wrapWithNotifyOnFinished [ts/Task ts/Framework ts/Cluster -> t/Any])
(defn wrapWithNotifyOnFinished
  [task fr cluster]
  (fn []
    (async/thread 
      (do
        (task)
        (println "notifying")
        (finishFramework cluster fr))))) 

