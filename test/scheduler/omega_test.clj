(ns scheduler.omega-test
  (:require [clojure.test :refer :all]
            [clojure.core.async :as async]
            [scheduler.omega :refer :all]
            [scheduler.framework :as framework]
            [scheduler.cluster :as cluster]
     )
   )

 
(defn demandResources 
  [cluster demand] 
  (async/thread (async/>!! (cluster/getDemandsCh cluster) demand)))

;; Test that one framework asks for all cpus and then another framework and see that is the first
(deftest competeAllCluster-test
  (testing "when two frameworks compete for the whole cluster the first gets it"
    (let [cluster (cluster/initOmegaCluster omegaIter)
          demand1 (cluster/wrapWithNotifyOnFinished (fn [] (println "Run task!")) "t1" cluster 10)
          demand2 (cluster/wrapWithNotifyOnFinished (fn [] (println "Run task!")) "t2" cluster 1)
         ]
         (demandResources cluster demand1)
         (demandResources cluster demand2)
         (let [newCluster (cluster/runIter cluster)]
           (is (= 0 (cluster/getClusterCpus newCluster)))))))


;; Test that both ask for half of the resources and they both get them

(deftest halfEach-test
  (testing "when two frameworks take half of the cluster"
    (let [cluster (cluster/initOmegaCluster omegaIter)
          demand1 (cluster/wrapWithNotifyOnFinished (fn [] (println "Run task!")) "t1" cluster 5)
          demand2 (cluster/wrapWithNotifyOnFinished (fn [] (println "Run task!")) "t2" cluster 5)
         ]
         (demandResources cluster demand1)
         (demandResources cluster demand2)
         (let [newCluster (cluster/runIter cluster)]
           (is (= 0 (cluster/getClusterCpus newCluster)))))))

;; Test that finished tasks are taken into account
(deftest resourcesRestored-test
  (testing "resources are restored when a tasks finished"
    (let [cluster (cluster/initOmegaCluster omegaIter)
          demand1 (cluster/wrapWithNotifyOnFinished (fn [] (println "Run task!")) "t1" cluster 10) ;; 10 cpus
         ]
         (demandResources cluster demand1)
         (let [newCluster (cluster/runIter cluster)]
           (is (= 0 (cluster/getClusterCpus newCluster)))
           (let [newCluster (cluster/runIter newCluster)]
            (is (= 10 (cluster/getClusterCpus newCluster))))))))

;; Test that one framework asks always before the other and always gets the resources (1/2/1/2) -
;;   2 always gets refused


