(ns scheduler.core
  (:require [clojure.core.async :as async]
            [scheduler.channel :as channel]
            [scheduler.cluster :as cluster]
            [scheduler.framework :as framework])
  (:gen-class))


(defn add
  [resources all]
  (clojure.set/union (set all) resources))

(defn minus
  [all toDelete]
  (clojure.set/difference (set all) (set toDelete)))

(defn updateResources
  [resources finished]
    (cluster/plusResources resources {:cpus (count finished)}))

;; resp => {:accepted true :task {:cmd ""}}

(defn resourcesUsedBy
  [tasks]
  {:cpus (count tasks)})


(defn offerToAll
  [frameworks resources cluster]
  (let [frameworks (seq frameworks)
        step (fn step [res frameworks newFrameworks]
     (if (not (empty? frameworks))
            (let [fr (first frameworks)
                  {tasks :tasks newFr :framework} (framework/offeredResources res fr)
                  ;; FIXME: Treat resources as monoids
                  newResources (cluster/minusResources res (resourcesUsedBy tasks))]
                  (doall (for [t tasks] (t)))
                  (step newResources (rest frameworks) (conj newFrameworks newFr )))
            (cluster/withResources (cluster/withFrameworks cluster (set newFrameworks)) res)))]
    (step resources frameworks [])))

(defn offerResources
  [cluster]
  (let [registerCh (cluster/getRegisterCh cluster) 
        finishedCh (cluster/getFinishedCh cluster) 
        frameworks (cluster/getFrameworks cluster)
        resources  (cluster/getResources cluster)
        events     (channel/readAll [finishedCh registerCh])
        finished   (channel/belongingTo events finishedCh)
        registered (channel/belongingTo events registerCh)
        resources (updateResources resources finished)
        frameworks (framework/updateFrameworks frameworks registered [])
        ]
    (offerToAll frameworks resources cluster)))

;; Mesos Master
  ;; init():
  ;;   available = starts with cluster resources
  ;;   frameworks = []
  ;;
  ;; loop():
  ;;   frameworks += updateFrameworks()
  ;;   resources += finishedTasksOrErrors() //wait for first
  ;;   for fr in frameworks: // ensure fairness when traversing the frameworks even at the end
  ;;                         // next time I'll traverse frameworks I will start by the second
  ;;     resp = offer (resources, fr) : // do I offer all resources or on a per/machine basis
  ;;     if resp = accepted:
  ;;       launchJob(resp.resourcesNeeded, resp.task)
  ;;       resources -= resp.resourcesNeeded
  ;;     
  
;; Scheduler
  ;;
  ;; init():
  ;;   tasks = list of tasks to run with the resources they need
  ;;   register to mesos master
  ;;
  ;;

(defn mesosIter
  [cluster] 
  (offerResources cluster))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (println "Hello, World!"))
