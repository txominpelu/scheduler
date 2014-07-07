(ns scheduler.core
  (:require [clojure.core.async :as async])
  (:gen-class))

(defn readAll
  [ch]
  (let [step (fn step [acc] 
               (let [ l (async/<!! ch)] 
                (if (= nil l)
                  acc
                  (step (conj acc l)))))]
    (step [nil])))

(defn minus
  [all toDelete]
  (println toDelete)
  (clojure.set/difference (set all) (set toDelete)))

(defn add
  [all toAdd]
  (println toAdd)
  (clojure.set/union (set all) (set toAdd)))

(defn updateFrameworks
  [frameworks registerCh deRegisterCh]
  (let [register (readAll registerCh)
        deRegister (readAll deRegisterCh)]
    (minus (add frameworks register) deRegister)))

(defn offerResources
  [registerCh finishedCh frameworks resources]
  (updateFrameworks frameworks registerCh))

;; Mesos Master
  ;; init():
  ;;   available = starts with cluster resources
  ;;   frameworks = []
  ;;
  ;; loop():
  ;;   frameworks += registeredFrameworks()
  ;;   frameworks -= deregisteredFrameworks()
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
(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (println "Hello, World!"))
