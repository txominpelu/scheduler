(ns scheduler.cluster
  (:require [clojure.core.async :as async]
            [clojure.core.typed.async :as ta]
            [clojure.core.typed :as t]
     ))

(t/defalias Resources (t/HMap :mandatory {:cpus t/AnyInteger}))
(t/defalias Task [ -> t/Any])
(t/defalias Framework (t/HMap :mandatory {:tasks (t/Seqable Task) :name String}))
(t/defalias Cluster (t/HMap :mandatory {
                                      :resources Resources
                                      :frameworks (t/Seqable Framework)
                                      :registerCh (ta/Chan t/Any)
                                      :finishedCh (ta/Chan t/Any)
                                      :iter [ Cluster -> t/Any]
                                      }))
;; Resources

(t/ann getCpus [Resources -> t/AnyInteger])
(defn getCpus
  [resources]
  (:cpus resources))

;; Cluster




(t/ann getResources [Cluster -> Resources])
(defn getResources
  [cluster]
  (:resources cluster))

(t/ann getClusterCpus [Cluster -> t/AnyInteger])
(defn getClusterCpus
  [cluster]
  (getCpus (getResources cluster)))

(t/ann getFrameworks [Cluster -> (t/Seqable Framework)])
(defn getFrameworks
  [cluster]
  (:frameworks cluster))

(t/ann getRegisterCh [Cluster -> (ta/Chan t/Any)])
(defn getRegisterCh
  [cluster]
  (:registerCh cluster))

(t/ann getFinishedCh [Cluster -> (ta/Chan t/Any)])
(defn getFinishedCh
  [cluster]
  (:finishedCh cluster))

(t/ann getIter [Cluster -> [Cluster -> t/Any]])
(defn getIter
  [cluster]
  (:iter cluster))

(t/ann registerFramework [Cluster Framework -> t/Any])
(defn registerFramework
  [cluster framework] 
  (async/thread (async/>!! (getRegisterCh cluster) framework)))

(t/ann finishFramework [Cluster Framework -> t/Any])
(defn finishFramework
  [cluster framework] 
  (async/thread (async/>!! (getFinishedCh cluster) framework)))

(t/ann withResources [Cluster Resources -> Cluster])
(defn withResources
  [cluster resources]
  (assoc cluster :resources resources))

(t/ann withFrameworks [Cluster (t/Seqable Framework) -> Cluster])
(defn withFrameworks
  [cluster frameworks]
  (assoc cluster :frameworks frameworks))

(t/ann runIter [Cluster -> t/Any])
(defn runIter
  [cluster]
  ((getIter cluster) cluster))

;; Tasks

(t/ann runTask [[ -> t/Any] -> t/Any])
(defn runTask
  [task]
 (task))

(t/ann plusResources [Resources Resources -> Resources])
(defn plusResources
  [res1 res2]
  {:cpus (+ (getCpus res1) (getCpus res2))})

(t/ann minusResources [Resources Resources -> Resources])
(defn minusResources
  [res1 res2]
  {:cpus (- (getCpus res1) (getCpus res2))})
