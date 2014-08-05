(ns scheduler.framework
  (:require [clojure.core.async :as async]
            [scheduler.channel :as channel]
            [scheduler.cluster :as cluster]
            [clojure.core.typed :as t]
            [clojure.core.typed.async :as ta]
            [scheduler.types :as ts]
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


(t/ann withTasks [ts/Framework (t/Seqable ts/Task) -> ts/Framework])
(defn withTasks
  [framework tasks]
 (assoc framework :tasks tasks))

(t/ann getTasks [ts/Framework -> (t/Seqable ts/Task)])
(defn getTasks
  [framework]
 (:tasks framework))

(t/ann createFramework [String (t/Seqable Any) -> (t/HMap :mandatory {:name String, :tasks (t/Seqable Any)})])
(defn createFramework
  [name tasks]
  {:name name :tasks tasks})

(t/ann offeredResources [ts/Resources ts/Framework -> (t/HMap :mandatory {:tasks (t/ASeq ts/Task), :framework ts/Framework} )])
(defn offeredResources
  "offers resources to a framework and returns the resources that are left"
  [resources framework]
  (let [cpus (cluster/getCpus resources)
        tasks (getTasks framework)
        ;; FIXME: One task is one CPU
        tasksToRun (int (min (count tasks) cpus))
        tk (t/inst take ts/Task) 
        sv (t/inst subvec ts/Task)
       ]
    {:tasks (tk tasksToRun tasks) :framework (withTasks framework (sv (vec tasks) tasksToRun)) }))

