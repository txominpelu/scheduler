(ns scheduler.cluster
  (:require [clojure.core.async :as async]))

;; Resources

(defn getCpus
  [resources]
  (:cpus resources))

;; Cluster

(defn getResources
  [cluster]
  (:resources cluster))

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

(defn runIter
  [cluster]
  ((getIter cluster) cluster))

