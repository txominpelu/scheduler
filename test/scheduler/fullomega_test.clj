(ns scheduler.fullomega-test
  (:require [clojure.test :refer :all]
            [clojure.core.async :as async]
            [scheduler.fullomega :refer :all]
            [scheduler.resources :as resources]
            [scheduler.framework :as framework]
            [scheduler.cluster :as cluster]
            [scheduler.omega-test :as omega-test]
     )
   )

 
;; Test that one framework asks for all cpus and then another framework and see that is the first
(deftest competeAllCluster-test
  (testing "when two frameworks compete for the whole cluster the first gets it"
    (let [demand1 {:id "t1" :cpus 10 :memory 8 :framework "fr1"}
          demand2 {:id "t2" :cpus 1 :memory 8 :framework "fr2"}]
      (omega-test/test-n-iters [[[demand1 demand2] ["t1"] ["t2"] 0]] true))))

(deftest halfEach-test
  (testing "when two frameworks take half of the cluster"
    (let [demand1 {:id "t1" :cpus 5 :memory 4 :framework "fr1"}
          demand2 {:id "t2" :cpus 5 :memory 4 :framework "fr2"}]
      (omega-test/test-n-iters [[[demand1 demand2] ["t1" "t2"] [] 0]] true))))

(deftest competeMemory-test
  (testing "when two frameworks compete for memory"
    (let [demand1 {:id "t1" :cpus 1 :memory 5 :framework "fr1"}
          demand2 {:id "t2" :cpus 1 :memory 4 :framework "fr2"}]
      (omega-test/test-n-iters [[[demand1 demand2] ["t1"] ["t2"] 9]] true))))

;; Fairness testso
(def totalResources {:slave1 {:cpus 9 :memory 18}}) 
(def d1Data {:id "t1" :cpus 1 :memory 4 :framework "fr1"})
(def d2Data {:id "t2" :cpus 3 :memory 1 :framework "fr2"})
(defn dem1 [cluster] ((omega-test/createDemand cluster true) d1Data))
(defn dem2 [cluster] ((omega-test/createDemand cluster true) d2Data))

(deftest dominantFairness-test
  (testing "when the cluster assign resources with fairness"
    (let [cluster (cluster/withResources (cluster/initOmegaCluster (omegaIter drf)) totalResources)
          demandsFr1 (repeat 5 d1Data)
          demandsFr2 (repeat 5 d2Data)]
      (omega-test/test-n-iters [[(concat demandsFr1 demandsFr2) ["t1" "t2" "t1" "t2" "t1"] ["t1" "t1" "t2" "t2" "t2"] 0]] cluster true))))


;; Test that both ask for half of the resources and they both get them

;; Test that finished tasks are taken into account
(deftest resourcesRestored-test
  (testing "resources are restored when a tasks finished"
    (let [cluster (cluster/initFullOmegaCluster (omegaIter omega-test/fifo))
          demand1 (cluster/fullwrapWithNotifyOnFinished (fn [] (println "Run task!")) "t1" cluster {:slave1 {:cpus 10 :memory 8}} "fr1") ;; 10 cpus
         ]
         (omega-test/demandResources cluster [demand1])
         (let [{newCluster :cluster} (cluster/runIter cluster)]
           (is (= {:cpus 0 :memory 0} (:slave1 (cluster/getResources newCluster))))
           (let [{newCluster :cluster} (cluster/runIter newCluster)]
            (is (= {:cpus 10 :memory 8} (:slave1 (cluster/getResources newCluster)))))))))

;; Test that one framework asks always before the other and always gets the resources (1/2/1/2) -
;;   2 always gets refused

