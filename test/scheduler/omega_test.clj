(ns scheduler.omega-test
  (:require [clojure.test :refer :all]
            [clojure.core.async :as async]
            [scheduler.omega :refer :all]
            [scheduler.framework :as framework]
            [scheduler.cluster :as cluster]
     )
   )

 
(defn demandResources 
  [cluster demands] 
  (doall (for [d demands] 
           (do
             (async/thread (async/>!! (cluster/getDemandsCh cluster) d))
             (Thread/sleep 100))))) ;; FIXME: Ugly!!!

(defn createDemand
  [cluster] 
  (fn [{cpus :cpus id :id}]
    (cluster/wrapWithNotifyOnFinished (fn [] (println "Run task!")) id cluster cpus)))

(defn test-demands
  [demands expSuccess expFailed expCpus]
  (let [cluster (cluster/initOmegaCluster omegaIter) 
        cDemand (createDemand cluster)
        demands (map cDemand demands)]
        (demandResources cluster demands)
        (let [{newCluster :cluster logs :logs} (cluster/runIter cluster)
              [success failed] (partition-by :success logs)]
           (is (= (map :id (map :demand success)) expSuccess))
           (is (= (map :id (map :demand failed))  expFailed))
           (is (= expCpus (cluster/getClusterCpus newCluster))))))



;; Test that one framework asks for all cpus and then another framework and see that is the first
(deftest competeAllCluster-test
  (testing "when two frameworks compete for the whole cluster the first gets it"
    (let [demand1 {:id "t1" :cpus 10}
          demand2 {:id "t2" :cpus 1}]
      (test-demands [demand1 demand2] ["t1"] ["t2"] 0))))

(deftest halfEach-test
  (testing "when two frameworks take half of the cluster"
    (let [demand1 {:id "t1" :cpus 5}
          demand2 {:id "t2" :cpus 5}]
      (test-demands [demand1 demand2] ["t1" "t2"] [] 0))))

;; Test that both ask for half of the resources and they both get them

;; Test that finished tasks are taken into account
(deftest resourcesRestored-test
  (testing "resources are restored when a tasks finished"
    (let [cluster (cluster/initOmegaCluster omegaIter)
          demand1 (cluster/wrapWithNotifyOnFinished (fn [] (println "Run task!")) "t1" cluster 10) ;; 10 cpus
         ]
         (demandResources cluster [demand1])
         (let [{newCluster :cluster} (cluster/runIter cluster)]
           (is (= 0 (cluster/getClusterCpus newCluster)))
           (let [{newCluster :cluster} (cluster/runIter newCluster)]
            (is (= 10 (cluster/getClusterCpus newCluster))))))))

;; Test that one framework asks always before the other and always gets the resources (1/2/1/2) -
;;   2 always gets refused


