(ns scheduler.cluster
  (:require [clojure.core.async :as async]
     ))

;; Resources

(defn getCpus
  [resources]
  (:cpus resources))

;; Cluster




(defn getResources
  [cluster]
  (:resources cluster))

(defn getClusterCpus
  [cluster]
  (getCpus (getResources cluster)))

(defn getFrameworks
  [cluster]
  (:frameworks cluster))

(defn getRegisterCh
  [cluster]
  (:registerCh cluster))

(defn getFinishedCh
  [cluster]
  (:finishedCh cluster))

(defn getIter
  [cluster]
  (:iter cluster))

(defn registerFramework
  [cluster framework] 
  (async/thread (async/>!! (getRegisterCh cluster) framework)))

(defn finishFramework
  [cluster framework] 
  (async/thread (async/>!! (getFinishedCh cluster) framework)))

(defn withResources
  [cluster resources]
  (assoc cluster :resources resources))

(defn withFrameworks
  [cluster frameworks]
  (assoc cluster :frameworks frameworks))

(defn runIter
  [cluster]
  ((getIter cluster) cluster))

;; Tasks

(defn runTask
  [task]
 (task))

(defn plusResources
  [res1 res2]
  {:cpus (+ (getCpus res1) (getCpus res2))})

(defn minusResources
  [res1 res2]
  {:cpus (- (getCpus res1) (getCpus res2))})
