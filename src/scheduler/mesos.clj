(ns scheduler.mesos
  (:require [clojure.core.async :as async]
            [clojure.core.typed :as t]
            [scheduler.types :as ts]
            [scheduler.task :as task]
            [scheduler.resources :as resources]
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

(t/ann updateResources [ts/Resources (t/Seqable ts/Demand) -> ts/Resources])
(defn updateResources
  [resources finished]
    (reduce resources/plusResources resources finished))

(defn offerToAll
  [cluster fr]
  (let [{demands :tasks newFr :framework} (framework/offeredResources (cluster/getResources cluster) fr)
        ;; FIXME: Treat resources as monoids
        resources (map task/getResources demands)
        resourcesTaken (reduce resources/plusResources resources)
        newCluster (cluster/substractResources cluster resourcesTaken)
        newFrameworks (conj (cluster/getFrameworks newCluster) newFr)]
        (doall (for [d demands] ((task/getTask d))))
        (cluster/withFrameworks newCluster (set newFrameworks))))

;; resp => {:accepted true :task {:cmd ""}}

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
    (reduce offerToAll (cluster/withResources cluster resources) frameworks)))

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
