(ns scheduler.cluster
  (:require [clojure.core.async :as async]
            [clojure.core.typed.async :as ta]
            [clojure.core.typed :as t]
            [scheduler.resources :as resources]
            [scheduler.fullresources :as fullresources]
            [scheduler.task :as task]
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

(t/ann getClusterMemory [ts/Cluster -> t/AnyInteger])
(defn getClusterMemory
  [cluster]
  (resources/getMemory (getResources cluster)))

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

(t/ann addFullResources [ts/Cluster ts/Resources -> ts/Cluster])
(defn addFullResources
  [cluster resources]
  (withResources cluster (fullresources/plusResources (getResources cluster) resources)))

(t/ann substractResources [ts/Cluster ts/Resources -> ts/Cluster])
(defn substractResources
  [cluster resources]
  "returns the new state of the cluster after the resources are commited"
  (withResources cluster (resources/minusResources (getResources cluster) resources)))

(t/ann substractFullResources [ts/Cluster ts/Resources -> ts/Cluster])
(defn substractFullResources
  [cluster resources]
  "returns the new state of the cluster after the resources are commited"
  (withResources cluster (fullresources/minusResources (getResources cluster) resources)))

;; Omega 

;; Cluster
;; TODO: Add incremental
(t/ann tryCommitDemand [ts/Cluster ts/Demand -> ts/Cluster])
(defn tryCommitDemand 
  [{cluster :cluster logs :logs} demand]
  " reads one demand and alters the state of the cluster if needed "
  (let [ neededRes  (task/getResources demand) 
         task (task/getTask demand)
         available (resources/<= neededRes (getResources cluster))
         log {:demand demand  :success available}
         newCluster (if available
                     (do 
                       (task)
                       (substractResources cluster neededRes))
                     cluster) ]
      {:cluster newCluster :logs (conj logs log)}))

(t/ann tryFullCommitDemand [ts/Cluster ts/Demand -> ts/Cluster])
(defn tryFullCommitDemand 
  [{cluster :cluster logs :logs} demand]
  " reads one demand and alters the state of the cluster if needed "
  (let [ neededRes  (task/getResources demand) 
         task (task/getTask demand)
         available (fullresources/<= neededRes (getResources cluster))
         log {:demand demand  :success available}
         newCluster (if available
                     (do 
                       (task)
                       (substractFullResources cluster neededRes))
                     cluster) ]
      
      {:cluster newCluster :logs (conj logs log)}))

;; running
(t/ann initOmegaCluster [[t/Any -> t/Any] -> ts/Cluster])
(defn initOmegaCluster
  [iter]
  {:iter iter
   :resources {:cpus 10 :memory 8} 
   :frameworks [] 
   :registerCh (async/chan)
   :demandsCh (async/chan)
   :finishedCh (async/chan)
   })

(defn initFullOmegaCluster
  [iter]
  {:iter iter
   :resources {:slave1 {:cpus 10 :memory 8} 
               :slave2 {:cpus 0 :memory 0} 
               :slave3 {:cpus 0 :memory 0} 
               :slave4 {:cpus 0 :memory 0}}
   :frameworks [] 
   :registerCh (async/chan)
   :demandsCh (async/chan)
   :finishedCh (async/chan)
   })

(t/ann initMesosCluster [[t/Any -> t/Any] -> ts/Cluster])
(defn initMesosCluster
  [iter]
  {:iter iter
   :resources {:cpus 10 :memory 8} 
   :frameworks [] 
   :registerCh (async/chan)
   :demandsCh (async/chan)
   :finishedCh (async/chan)
   })

(defn wrap
  [task name cluster res framework]
  (let [demand {:id name 
                :resources res
                :framework framework}
        t (fn [] (async/thread 
                  (do
                    (task)
                    (println (str "runned task: " name))
                    (notifyFinishedTask cluster demand))))]
     (assoc demand :task t)))

(defn fullwrap
  [task name cluster res framework]
  (let [demand {:id name 
                :resources res 
                :framework framework}
        t (fn [] (async/thread 
                  (do
                    (task)
                    (println (str "runned task: " name))
                    (notifyFinishedTask cluster demand))))]
     (assoc demand :task t)))

;; FIXME: Circular reference
(t/ann wrapWithNotifyOnFinished [ts/Task ts/Cluster -> t/Any])
(defn wrapWithNotifyOnFinished
  ([task name cluster] (wrap task name cluster {:cpus 1 :memory 1} "fr1"))
  ([task name cluster res framework] (wrap task name cluster res framework)))
  

;; FIXME: Circular reference
(t/ann fullwrapWithNotifyOnFinished [ts/Task ts/Cluster -> t/Any])
(defn fullwrapWithNotifyOnFinished
  ([task name cluster resources framework] (fullwrap task name cluster resources framework)))
  

