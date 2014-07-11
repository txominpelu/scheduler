(ns scheduler.core-test
  (:require [clojure.test :refer :all]
            [clojure.core.async :as async]
            [scheduler.core :refer :all]
            [scheduler.framework :as framework]
            [scheduler.cluster :as cluster]
     ))
;; Cluster - Test

(defn initCluster
  []
  {:iter (fn [cluster] 
           (offerResources 
             (cluster/getRegisterCh cluster) 
             (cluster/getFinishedCh cluster) 
             (cluster/getFrameworks cluster)
             (cluster/getResources cluster)))
   :resources {:cpus 10} 
   :frameworks [] 
   :finishedCh (async/chan)
   :registerCh (async/chan)})


;; Framework
(deftest registerFramework-test
  (testing "registration of a framework is taken into account "
    (let [cluster (initCluster)
          framework (framework/createFramework "fr1" [])]
      (cluster/registerFramework cluster framework)
      (is (= (cluster/getFrameworks (cluster/runIter cluster)) #{framework} )))))


;; create a scheduler with one job with one task
;; connect the scheduler to the cluster
;; see the result of the execution of the task
(deftest runOneTask-test
  (testing "run one task"
    (let [cluster (initCluster)
          framework (framework/createFramework "fr1" [(fn [] (println "Run task!"))])]
      (cluster/registerFramework cluster framework)
      (is (= (cluster/getFrameworks (cluster/runIter cluster)) #{framework} )))))

