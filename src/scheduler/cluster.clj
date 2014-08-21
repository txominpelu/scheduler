(ns scheduler.cluster
  (:require [clojure.core.async :as async]
            [clojure.core.typed.async :as ta]
            [clojure.core.typed :as t]
            [scheduler.resources :as resources]
            [scheduler.types :as ts]
     ))
;; Cluster


(t/ann getResources [ts/Cluster -> ts/Resources])
(defn getResources
  [cluster]
  (:resources cluster))

(t/ann getClusterCpus [ts/Cluster -> t/AnyInteger])
(defn getClusterCpus
  [cluster]
  (resources/getCpus (getResources cluster)))

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

(t/ann notifyFinishedTask [ts/Cluster ts/Demand -> t/Any])
(defn notifyFinishedTask
  [cluster demand] 
  (async/thread (async/>!! (getFinishedCh cluster) demand)))

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


(t/ann addResources [ts/Cluster ts/Resources -> ts/Cluster])
(defn addResources
  [cluster resources]
  (withResources cluster (resources/plusResources (getResources cluster) resources)))

(t/ann substractResources [ts/Cluster ts/Resources -> ts/Cluster])
(defn substractResources
  [cluster resources]
  (withResources cluster (resources/minusResources (getResources cluster) resources)))

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

(defn wrap
  [task name cluster cpus]
  (let [demand {:id name :resources {:cpus cpus}}
        t (fn [] (async/thread 
                  (do
                    (task)
                    (println (str "runned task: " name))
                    (notifyFinishedTask cluster demand))))]
     (assoc demand :task t)))

;; FIXME: Circular reference
(t/ann wrapWithNotifyOnFinished [ts/Task ts/Cluster -> t/Any])
(defn wrapWithNotifyOnFinished
  ([task name cluster] (wrap task name cluster 1))
  ([task name cluster cpus] (wrap task name cluster cpus)))
  

