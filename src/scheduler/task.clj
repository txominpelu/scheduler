(ns scheduler.task
  (:require 
            [clojure.core.typed :as t]
            [scheduler.types :as ts]
            [scheduler.resources :as resources]
  ))


(t/ann getResources [ts/Demand -> ts/Resources])
(defn getResources
  [demand]
  (:resources demand))

(t/ann getCpus [ts/Demand -> t/AnyInteger])
(defn getCpus
  [demand]
  (:cpus (:resources demand)))


(t/ann getTask [ts/Demand -> ts/Task])
(defn getTask
  [demand]
  (:task demand))

(t/ann getFramework [ts/Demand -> String])
(defn getFramework
  [demand]
  (:framework demand))
