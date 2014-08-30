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
  (fn [{cpus :cpus memory :memory id :id}]
    (cluster/wrapWithNotifyOnFinished (fn [] (println "Run task!")) id cluster cpus memory)))

(defn test-iter
  [cluster [demands expSuccess expFailed expCpus]]
    (let [ cDemand (createDemand cluster)
           demands (map cDemand demands)]
          (demandResources cluster demands)
          (let [{newCluster :cluster logs :logs} (cluster/runIter cluster)
                [success failed] (partition-by :success logs)]
             (is (= (map :id (map :demand success)) expSuccess))
             (is (= (map :id (map :demand failed))  expFailed))
             (is (= expCpus (cluster/getClusterCpus newCluster)))
            newCluster)))

(defn test-n-iters
  [fixtures]
  (let [cluster (cluster/initOmegaCluster omegaIter)]
    (reduce (fn [acc f] (test-iter acc f)) cluster fixtures)))


;; Test that one framework asks for all cpus and then another framework and see that is the first
(deftest competeAllCluster-test
  (testing "when two frameworks compete for the whole cluster the first gets it"
    (let [demand1 {:id "t1" :cpus 10 :memory 8}
          demand2 {:id "t2" :cpus 1 :memory 8}]
      (test-n-iters [[[demand1 demand2] ["t1"] ["t2"] 0]]))))

(deftest halfEach-test
  (testing "when two frameworks take half of the cluster"
    (let [demand1 {:id "t1" :cpus 5 :memory 4}
          demand2 {:id "t2" :cpus 5 :memory 4}]
      (test-n-iters [[[demand1 demand2] ["t1" "t2"] [] 0]]))))

(deftest competeMemory-test
  (testing "when two frameworks compete for memory"
    (let [demand1 {:id "t1" :cpus 1 :memory 5}
          demand2 {:id "t2" :cpus 1 :memory 4}]
      (test-n-iters [[[demand1 demand2] ["t1"] ["t2"] 9]]))))

;; Test that both ask for half of the resources and they both get them

;; Test that finished tasks are taken into account
(deftest resourcesRestored-test
  (testing "resources are restored when a tasks finished"
    (let [cluster (cluster/initOmegaCluster omegaIter)
          demand1 (cluster/wrapWithNotifyOnFinished (fn [] (println "Run task!")) "t1" cluster 10 8) ;; 10 cpus
         ]
         (demandResources cluster [demand1])
         (let [{newCluster :cluster} (cluster/runIter cluster)]
           (is (= 0 (cluster/getClusterCpus newCluster)))
           (let [{newCluster :cluster} (cluster/runIter newCluster)]
            (is (= 10 (cluster/getClusterCpus newCluster))))))))

;; Test that one framework asks always before the other and always gets the resources (1/2/1/2) -
;;   2 always gets refused


;; Test that drf works
(deftest drf-test
  (testing "that drf works"
    (drf totalResources consumedResources dominantShares resourcesGiven)))

