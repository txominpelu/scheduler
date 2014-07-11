(ns scheduler.core
  (:require [clojure.core.async :as async]
            [scheduler.channel :as channel]
            [scheduler.framework :as framework])
  (:gen-class))


(defn add
  [resources all]
  (clojure.set/union (set all) resources))

(defn minus
  [all toDelete]
  (clojure.set/difference (set all) (set toDelete)))

(defn updateResources
  [resources finishedTasksCh]
  (let [finished (channel/readAll finishedTasksCh)]
    (add resources finished)))

;; resp => {:accepted true :task {:cmd ""}}

(defn offer
  []
  (println "offer"))
;;  [frameworks resources]
;;  (let [step 
;;        (fn step [frameworks]
;;          (let [resp (makeOffer (first frameworks))
;;                left (rest frameworks)]
;;            (if (isAccepted? resp)
;;              (launchTask resp)
;;              (if (nil? left)
;;                nil
;;                (step left)))))]
;;    (step frameworks)))
              
(defn offerResources
  [registerCh finishedCh frameworks resources]
  (let [frameworks (framework/updateFrameworks frameworks registerCh (async/chan))
        resources (updateResources resources finishedCh)]
    (doall (for [f frameworks] 
      (framework/offeredResources resources f))
      )
    {:resources resources :frameworks frameworks}))

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
(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (println "Hello, World!"))
