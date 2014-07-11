(ns scheduler.framework
  (:require [clojure.core.async :as async]
            [scheduler.channel :as channel]
            [scheduler.cluster :as cluster]
     ))

(defn minus
  [all toDelete]
  (clojure.set/difference (set all) (set toDelete)))

(defn add
  [all toAdd]
  (clojure.set/union (set all) (set toAdd)))

(defn updateFrameworks
  [frameworks registerCh deRegisterCh]
  (let [register (channel/readAll registerCh)
        deRegister (channel/readAll deRegisterCh)]
    (minus (add frameworks register) deRegister)))


(defn runTask
  [task]
 (task))

(defn getTasks
  [framework]
 (:tasks framework))

(defn createFramework
  [name tasks]
  {:name name :tasks tasks})

(defn offeredResources
  [resources framework]
  (let [cpus (cluster/getCpus resources)
        tasksToRun (min (count (getTasks framework)) cpus)
       ]
    (doall (for [i (range 0 tasksToRun)]
      (runTask (nth (getTasks framework) i))))))

