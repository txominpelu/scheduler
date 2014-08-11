(ns scheduler.core-test
  (:require [clojure.test :refer :all]
            [clojure.core.async :as async]
            [scheduler.core :refer :all]
            [scheduler.framework :as framework]
            [scheduler.cluster :as cluster]
     )
   )
;; Cluster - Test
(defn initCluster
  []
  {:iter (fn [cluster] 
           (offerResources 
             cluster))
   :resources {:cpus 10} 
   :frameworks [] 
   :finishedCh (async/chan)
   :registerCh (async/chan)})

;; FIXME: For the moment frameworks are only considered to have one task when the task finishes the framework is unreg.
(defn wrapWithNotifyOnFinished
  [task fr cluster]
  (fn []
    (async/thread 
      (do
        (task)
        (println "notifying")
        (cluster/finishFramework cluster fr))))) 

;; Framework
(deftest registerFramework-test
  (testing "registration of a framework is taken into account "
    (let [cluster (initCluster)
          task (wrapWithNotifyOnFinished (fn [] (println "Run task!")) "fr1" cluster)
          framework (framework/createFramework "fr1" [task])]
      (cluster/registerFramework cluster framework)
      (Thread/sleep 1000)
    (let [frameworks (framework/updateFrameworks (cluster/getFrameworks cluster) (cluster/getRegisterCh cluster) (async/chan))]
      (is (= [[task]] (map framework/getTasks frameworks)))
      (is (= #{framework} frameworks ))))))




;; create a scheduler with one job with one task
;; connect the scheduler to the cluster
;; see the result of the execution of the task
(deftest runOneTask-test
  (testing "run one task"
    (let [cluster (initCluster)
          task (wrapWithNotifyOnFinished (fn [] (println "Run task!")) "fr1" cluster)
          framework (framework/createFramework "fr1" [task])]
      (cluster/registerFramework cluster framework)
      (let [newCluster (cluster/runIter cluster)]
       (is (= (cluster/getFrameworks newCluster) #{(framework/withTasks framework [])} ))
       (is (= (cluster/getClusterCpus newCluster) (- (cluster/getClusterCpus cluster) 1)))
           (let [newCluster (cluster/runIter newCluster)]
             (is (= (cluster/getFrameworks newCluster) #{(framework/withTasks framework [])} ))
             (is (= (cluster/getClusterCpus newCluster) (cluster/getClusterCpus cluster))))
           ))))

;; create a scheduler with one job with one task
;; connect the scheduler to the cluster
;; see the result of the execution of the task

(defn runClusterTillNoTask
  [cluster]
  (let [step (fn step [cluster]
        (if (empty? (flatten (framework/getClusterTasks cluster)))
          cluster
          (step (cluster/runIter cluster))))]
    (step cluster)))
  
(deftest runClusterTillNoTask-test
  (testing "run till there is no more task"
    (let [cluster (initCluster)
          task (wrapWithNotifyOnFinished (fn [] (println "Run task!")) "fr1" cluster)
          framework (framework/createFramework "fr1" [task])]
      (cluster/registerFramework cluster framework)
      (is (= [] (flatten (framework/getClusterTasks (runClusterTillNoTask cluster))))))))
