(ns scheduler.framework
  (:require [clojure.core.async :as async]
            [scheduler.channel :as channel]
            [scheduler.cluster :as cluster]
            [clojure.core.typed :as t]
     ))

(t/ann minus (t/All [x] [(t/Seqable x) (t/Seqable x) -> (t/Set x)]))
(defn minus
  [all toDelete]
  (clojure.set/difference (set all) (set toDelete)))

(t/ann add (t/All [x] [(t/Seqable x) (t/Seqable x) -> (t/Set x)]))
(defn add
  [all toAdd]
  (clojure.set/union (set all) (set toAdd)))

(defn updateFrameworks
  [frameworks registerCh deRegisterCh finishedCh]
  (let [register (channel/readAll registerCh)
        finished (channel/readAll finishedCh)
        deRegister (channel/readAll deRegisterCh)]
    (minus (minus (add frameworks register) deRegister) finished)))


(defn withTasks
  [framework tasks]
 (assoc framework :tasks tasks))

(defn getTasks
  [framework]
 (:tasks framework))

(t/ann createFramework [String (t/Seqable Any) -> (t/HMap :mandatory {:name String, :tasks (t/Seqable Any)})])
(defn createFramework
  [name tasks]
  {:name name :tasks tasks})

(defn offeredResources
  "offers resources to a framework and returns the resources that are left"
  [resources framework]
  (let [cpus (cluster/getCpus resources)
        tasks (getTasks framework)
        ;; FIXME: One task is one CPU
        tasksToRun (min (count tasks) cpus)
       ]
    {:tasks (take tasksToRun tasks) :framework (withTasks framework (subvec tasks tasksToRun)) }))

