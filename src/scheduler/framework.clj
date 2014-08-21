(ns scheduler.framework
  (:require [clojure.core.async :as async]
            [scheduler.channel :as channel]
            [scheduler.task :as task]
            [scheduler.resources :as resources]
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

(t/ann updateFrameworks (t/All [x] [(t/Set x) (t/Seqable x) (t/Seqable x) -> (t/Seqable x)]))
(defn updateFrameworks
  [frameworks register deRegister]
    (minus (add frameworks register) deRegister))


(t/ann withTasks [ts/Framework (t/Seqable ts/Task) -> ts/Framework])
(defn withTasks
  [framework demands]
 (assoc framework :demands demands))

(t/ann getDemands [ts/Framework -> (t/Seqable ts/Task)])
(defn getDemands
  [framework]
 (:demands framework))

(t/ann getClusterDemands [ts/Cluster -> (t/Seqable (t/Seqable ts/Task))])
(defn getClusterDemands
  [cluster]
  (map getDemands (cluster/getFrameworks cluster)))

(t/ann createFramework [String (t/Seqable ts/Demand) -> ts/Framework])
(defn createFramework
  [name demands]
  {:name name :demands demands})

;; This scheme doesn't consider task dependencies
;; needed recursive function 
(t/ann addDemandIfResources [ts/Demand -> (t/Seqable ts/Demand)])
(defn addDemandIfResources 
  [cpus]
  (fn [acc t]
    (if (<= (resources/getCpus (task/resourcesUsedBy (conj acc t))) cpus)
      (conj acc t)
      acc )))


(t/ann offeredResources [ts/Resources ts/Framework -> (t/HMap :mandatory {:tasks (t/ASeq ts/Demand), :framework ts/Framework} )])
(defn offeredResources
  "offers resources to a framework and returns the resources that are left"
  [resources framework]
  (let [cpus (resources/getCpus resources)
        tasks (getDemands framework)
        ;; FIXME: One task is one CPU
        tasksToRun (reverse (reduce (addDemandIfResources cpus) [] tasks))
        tasksLeft (remove (set tasksToRun) tasks)
       ]
    {:tasks tasksToRun :framework (withTasks framework tasksLeft) }))

