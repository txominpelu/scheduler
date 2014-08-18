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
  (testing "when two clusters compete for the whole cluster the first gets it"
    (let [cluster (cluster/initOmegaCluster omegaIter)
          task (cluster/wrapWithNotifyOnFinished (fn [] (println "Run task!")) "fr1" cluster)
          ;;fr1 (framework/createFramework "fr1" )
          ;;fr2 (framework/createFramework "fr2" [task])
          demand1 {:resources {:cpus 10} :task task :id 1}
          demand2 {:resources {:cpus 10} :task task :id 2}
         ]
         (demandResources cluster demand1)
         (demandResources cluster demand2)
         (let [newCluster (cluster/runIter cluster)]
           (is (= 0 (cluster/getClusterCpus newCluster)))))))
         ;;(is (= [] (flatten (framework/getClusterTasks (runClusterTillNoTask cluster))))))))


;; Test that both ask for half of the resources and they both get them
;; Test that one framework asks always before the other and always gets the resources (1/2/1/2) -
;;   2 always gets refused


