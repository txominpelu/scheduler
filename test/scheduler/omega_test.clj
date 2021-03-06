(ns scheduler.omega-test
  (:require [clojure.test :refer :all]
            [clojure.core.async :as async]
            [scheduler.omega :refer :all]
            [scheduler.resources :as resources]
            [scheduler.framework :as framework]
            [scheduler.cluster :as cluster]
     )
   )

 
(defn fifo
  [cluster demands]
  demands)

(defn demandResources 
  [cluster demands] 
  (doall (for [d demands] 
           (do
             (async/thread (async/>!! (cluster/getDemandsCh cluster) d))
             (Thread/sleep 100))))) ;; FIXME: Ugly!!!

(defn createDemand
  [cluster] 
  (fn [{cpus :cpus memory :memory id :id framework :framework}]
    (cluster/wrapWithNotifyOnFinished (fn [] (println "Run task!")) id cluster cpus memory framework)))

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
  ([fixtures]
    (let [cluster (cluster/initOmegaCluster (omegaIter fifo))]
      (test-n-iters fixtures cluster)))
  ([fixtures cluster]
      (reduce (fn [acc f] (test-iter acc f)) cluster fixtures)))


;; Test that one framework asks for all cpus and then another framework and see that is the first
(deftest competeAllCluster-test
  (testing "when two frameworks compete for the whole cluster the first gets it"
    (let [demand1 {:id "t1" :cpus 10 :memory 8 :framework "fr1"}
          demand2 {:id "t2" :cpus 1 :memory 8 :framework "fr2"}]
      (test-n-iters [[[demand1 demand2] ["t1"] ["t2"] 0]]))))

(deftest halfEach-test
  (testing "when two frameworks take half of the cluster"
    (let [demand1 {:id "t1" :cpus 5 :memory 4 :framework "fr1"}
          demand2 {:id "t2" :cpus 5 :memory 4 :framework "fr2"}]
      (test-n-iters [[[demand1 demand2] ["t1" "t2"] [] 0]]))))

(deftest competeMemory-test
  (testing "when two frameworks compete for memory"
    (let [demand1 {:id "t1" :cpus 1 :memory 5 :framework "fr1"}
          demand2 {:id "t2" :cpus 1 :memory 4 :framework "fr2"}]
      (test-n-iters [[[demand1 demand2] ["t1"] ["t2"] 9]]))))

;; Fairness testso
(def totalResources {:cpus 9 :memory 18}) 
(def d1Data {:id "t1" :cpus 1 :memory 4 :framework "fr1"})
(def d2Data {:id "t2" :cpus 3 :memory 1 :framework "fr2"})
(defn dem1 [cluster] ((createDemand cluster) d1Data))
(defn dem2 [cluster] ((createDemand cluster) d2Data))

(deftest dominantFairness-test
  (testing "when the cluster assign resources with fairness"
    (let [cluster (cluster/withResources (cluster/initOmegaCluster (omegaIter drf)) totalResources)
          demandsFr1 (repeat 5 d1Data)
          demandsFr2 (repeat 5 d2Data)]
      (test-n-iters [[(concat demandsFr1 demandsFr2) ["t1" "t2" "t1" "t2" "t1"] ["t1" "t1" "t2" "t2" "t2"] 0]] cluster))))


;; Test that both ask for half of the resources and they both get them

;; Test that finished tasks are taken into account
(deftest resourcesRestored-test
  (testing "resources are restored when a tasks finished"
    (let [cluster (cluster/initOmegaCluster (omegaIter fifo))
          demand1 (cluster/wrapWithNotifyOnFinished (fn [] (println "Run task!")) "t1" cluster 10 8 "fr1") ;; 10 cpus
         ]
         (demandResources cluster [demand1])
         (let [{newCluster :cluster} (cluster/runIter cluster)]
           (is (= 0 (cluster/getClusterCpus newCluster)))
           (let [{newCluster :cluster} (cluster/runIter newCluster)]
            (is (= 10 (cluster/getClusterCpus newCluster))))))))

;; Test that one framework asks always before the other and always gets the resources (1/2/1/2) -
;;   2 always gets refused


(def dominantShares {:fr1 0 :fr2 0}) 
(def resourcesGiven {:fr1 {:cpus 0 :memory 0} :fr2 {:cpus 0 :memory 0}}) 

(deftest internalDrf-test
  (testing "that internally drf works"
    (let [cluster (cluster/withResources (cluster/initOmegaCluster (omegaIter fifo)) totalResources)
          d1 (dem1 cluster)
          d2 (dem2 cluster)
          demands {:fr1 (repeat 5 d1) :fr2 (repeat 5 d2)}]
    (internalDrf totalResources resources/emptyResources dominantShares resourcesGiven demands []))))

(deftest drf-test
  (testing "that drf works"
    (let [cluster (cluster/withResources (cluster/initOmegaCluster (omegaIter fifo)) totalResources)
          d1 (dem1 cluster)
          d2 (dem2 cluster)
          demandsFr1 (repeat 5 d1)
          demandsFr2 (repeat 5 d2)
          result (drf cluster (concat demandsFr1 demandsFr2))
         ]
    (is (= (map :id (take 5 result))
           (map :id [d1 d2 d1 d2 d1]))))))

