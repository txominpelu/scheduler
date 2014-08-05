(ns scheduler.framework
  (:require [clojure.core.async :as async]
            [scheduler.channel :as channel]
            [scheduler.cluster :as cluster]
            [clojure.core.typed :as t]
            [clojure.core.typed.async :as ta]
     ))

(t/ann minus (t/All [x] [(t/Seqable x) (t/Seqable x) -> (t/Set x)]))
(defn minus
  [all toDelete]
  (clojure.set/difference (set all) (set toDelete)))

(t/ann add (t/All [x] [(t/Seqable x) (t/Seqable x) -> (t/Set x)]))
(defn add
  [all toAdd]
  (clojure.set/union (set all) (set toAdd)))

(t/ann updateFrameworks (t/All [x] [(t/Seqable x) (ta/Chan x) (ta/Chan x) -> (t/Seqable x)]))
(defn updateFrameworks
  [frameworks registerCh deRegisterCh]
  (let [register (channel/readAll registerCh)
        deRegister (channel/readAll deRegisterCh)]
    (minus (add frameworks register) deRegister)))


(t/ann withTasks [cluster/Framework (t/Seqable cluster/Task) -> cluster/Framework])
(defn withTasks
  [framework tasks]
 (assoc framework :tasks tasks))

(t/ann getTasks [cluster/Framework -> (t/Seqable cluster/Task)])
(defn getTasks
  [framework]
 (:tasks framework))

(t/ann createFramework [String (t/Seqable Any) -> (t/HMap :mandatory {:name String, :tasks (t/Seqable Any)})])
(defn createFramework
  [name tasks]
  {:name name :tasks tasks})

(t/ann offeredResources [cluster/Resources cluster/Framework -> (t/HMap :mandatory {:tasks (t/ASeq cluster/Task), :framework cluster/Framework} )])
(defn offeredResources
  "offers resources to a framework and returns the resources that are left"
  [resources framework]
  (let [cpus (cluster/getCpus resources)
        tasks (getTasks framework)
        ;; FIXME: One task is one CPU
        tasksToRun (int (min (count tasks) cpus))
        tk (t/inst take cluster/Task) 
        sv (t/inst subvec cluster/Task)
       ]
    {:tasks (tk tasksToRun tasks) :framework (withTasks framework (sv (vec tasks) tasksToRun)) }))

