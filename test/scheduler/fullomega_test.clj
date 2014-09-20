(ns scheduler.fullomega-test
  (:require [clojure.test :refer :all]
            [clojure.core.async :as async]
            [scheduler.fullomega :refer :all]
            [scheduler.omega :as omega]
            [scheduler.resources :as resources]
            [scheduler.fullresources :as fullresources]
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
  (fn [{res :resources id :id framework :framework}]
     (cluster/fullwrapWithNotifyOnFinished (fn [] (println "Run task!")) id cluster res framework)))

(defn test-iter
  ([cluster [demands expSuccess expFailed expRes]]
    (let [ cDemand (createDemand cluster)
           demands (map cDemand demands)]
          (demandResources cluster demands)
          (let [{newCluster :cluster logs :logs} (cluster/runIter cluster)
                [success failed] (partition-by :success logs)]
             (is (= (map :id (map :demand success)) expSuccess))
             (is (= (map :id (map :demand failed))  expFailed))
             (is (= (fullresources/normalize expRes) (fullresources/normalize (cluster/getResources newCluster))))
            newCluster))))

(defn test-n-iters
  ([fixtures]
    (let [iter (omegaIter fifo)
          cluster (cluster/initFullOmegaCluster iter)]
      (test-n-iters fixtures cluster)))
  ([fixtures cluster]
      (reduce (fn [acc f] (test-iter acc f)) cluster fixtures)))

 
;; Test that one framework asks for all cpus and then another framework and see that is the first
(deftest competeAllCluster-test
  (testing "when two frameworks compete for the whole cluster the first gets it"
    (let [demand1 {:id "t1" :resources {:slave1 {:cpus 10 :memory 8}} :framework "fr1"}
          demand2 {:id "t2" :resources {:slave1 {:cpus 1 :memory 8}} :framework "fr2"}]
      (test-n-iters [[[demand1 demand2] ["t1"] ["t2"] {:slave1 {:cpus 0 :memory 0}}]]))))

(deftest halfEach-test
  (testing "when two frameworks take half of the cluster"
    (let [demand1 {:id "t1" :resources {:slave1 {:cpus 5 :memory 4}} :framework "fr1"}
          demand2 {:id "t2" :resources {:slave1 {:cpus 5 :memory 4}} :framework "fr2"}]
      (test-n-iters [[[demand1 demand2] ["t1" "t2"] [] {:slave1 {:cpus 0 :memory 0}}]]))))

(deftest competeMemory-test
  (testing "when two frameworks compete for memory"
    (let [demand1 {:id "t1" :resources {:slave1 {:cpus 1 :memory 5}} :framework "fr1"}
          demand2 {:id "t2" :resources {:slave1 {:cpus 1 :memory 4}} :framework "fr2"}]
      (test-n-iters [[[demand1 demand2] ["t1"] ["t2"] {:slave1 {:cpus 9 :memory 3}}]]))))

;; Fairness tests
(def totalResources {:slave1 {:cpus 9 :memory 18}}) 
(def d1Data {:id "t1" :resources {:slave1 {:cpus 1 :memory 4}} :framework "fr1"})
(def d2Data {:id "t2" :resources {:slave1 {:cpus 3 :memory 1}} :framework "fr2"})
(defn dem1 [cluster] ((createDemand cluster true) d1Data))
(defn dem2 [cluster] ((createDemand cluster true) d2Data))

(deftest dominantFairness-test
  (testing "when the cluster assign resources with fairness"
    (let [cluster (cluster/withResources (cluster/initFullOmegaCluster (omegaIter (omega/drf internalDrf fullresources/emptyResources))) totalResources)
          demandsFr1 (repeat 5 d1Data)
          demandsFr2 (repeat 5 d2Data)]
      (test-n-iters [[(concat demandsFr1 demandsFr2) ["t1" "t2" "t1" "t2" "t1"] ["t1" "t1" "t2" "t2" "t2"] {:slave1 {:cpus 0 :memory 4}}]] cluster))))


;; Test that both ask for half of the resources and they both get them

;; Test that finished tasks are taken into account
(deftest resourcesRestored-test
 (testing "resources are restored when a tasks finished"
    (let [cluster (cluster/initFullOmegaCluster (omegaIter fifo))
          demand1 (cluster/fullwrapWithNotifyOnFinished (fn [] (println "Run task!")) "t1" cluster {:slave1 {:cpus 10 :memory 8}} "fr1") ;; 10 cpus
         ]
         (demandResources cluster [demand1])
         (let [{newCluster :cluster} (cluster/runIter cluster)]
           (is (= {:cpus 0 :memory 0} (:slave1 (cluster/getResources newCluster))))
           (let [{newCluster :cluster} (cluster/runIter newCluster)]
            (is (= {:cpus 10 :memory 8} (:slave1 (cluster/getResources newCluster)))))))))
;;
;; Test that one framework asks always before the other and always gets the resources (1/2/1/2) -
;;   2 always gets refused

