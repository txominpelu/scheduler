(ns scheduler.core
  (:gen-class))

(defn
  (

(defn

;; Mesos Master
  ;; init():
  ;;   available = starts with cluster resources
  ;;   frameworks = []
  ;;
  ;; loop():
  ;;   frameworks += registeredFrameworks()
  ;;   frameworks -= deregisteredFrameworks()
  ;;   resources += finishedTasks()
  ;;   for fr in frameworks: // ensure fairness when traversing the frameworks 
  ;;     resp = offer (resources, fr) : // do I offer all resources or on a per/machine basis
  ;;     if resp = accepted:
  ;;       launchJob(
  ;;
  ;;   
  ;; 
  ;;   
  ;; onFrameworkRegister():
  ;;   
  ;;
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
