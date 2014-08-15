(ns scheduler.core-test
  (:require [clojure.test :refer :all]
            [clojure.core.async :as async]
            [scheduler.core :refer :all]
            [scheduler.framework :as framework]
            [scheduler.cluster :as cluster]
     )
   )

;; FIXME: For the moment frameworks are only considered to have one task when the task finishes the framework is unreg.

;; Framework
(deftest registerFramework-test
  (testing "registration of a framework is taken into account "
    (let [cluster (cluster/initMesosCluster mesosIter)
          task (cluster/wrapWithNotifyOnFinished (fn [] (println "Run task!")) "fr1" cluster)
          framework (framework/createFramework "fr1" [task])]
      (cluster/registerFramework cluster framework)
      (Thread/sleep 1000)
    (let [frameworks (cluster/getFrameworks (cluster/runIter cluster))]
      (is (= [[]] (map framework/getTasks frameworks)))
      (is (= #{(framework/withTasks framework [])} frameworks ))))))




;; create a scheduler with one job with one task
;; connect the scheduler to the cluster
;; see the result of the execution of the task
(deftest runOneTask-test
  (testing "run one task"
    (let [cluster (cluster/initMesosCluster  mesosIter)
          task (cluster/wrapWithNotifyOnFinished (fn [] (println "Run task!")) "fr1" cluster)
          framework (framework/createFramework "fr1" [task])]
      (cluster/registerFramework cluster framework)
      (let [newCluster (cluster/runIter cluster)]
       (is (= (cluster/getFrameworks newCluster) #{(framework/withTasks framework [])} ))
       (is (= (cluster/getClusterCpus newCluster) (- (cluster/getClusterCpus cluster) 1)))
           ;;(let [newCluster (cluster/runIter newCluster)]
            ;; (is (= (cluster/getFrameworks newCluster) #{(framework/withTasks framework [])} ))
            ;; (is (= (cluster/getClusterCpus newCluster) (cluster/getClusterCpus cluster))))
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
    (let [cluster (cluster/initMesosCluster mesosIter)
          task (cluster/wrapWithNotifyOnFinished (fn [] (println "Run task!")) "fr1" cluster)
          framework (framework/createFramework "fr1" [task])]
      (cluster/registerFramework cluster framework)
      (is (= [] (flatten (framework/getClusterTasks (runClusterTillNoTask cluster))))))))



;; TODO: Consider type of job
;; TODO: Consider schDecisionTime, [taskDependencies?]
;; fixture
;; framework1,arrivalTime //framework name
;; task1,duration
;; task2,duration
;;
;; e.g
;; fr1, 0
;; task1_1,1
;; task1_2,1
;; task1_3,1
;;
;; fr2, 3
;; task2_1,1
;; task2_2,1
;; task2_3,1
