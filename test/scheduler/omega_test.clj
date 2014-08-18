(ns scheduler.omega-test
  (:require [clojure.test :refer :all]
            [clojure.core.async :as async]
            [scheduler.omega :refer :all]
            [scheduler.framework :as framework]
            [scheduler.cluster :as cluster]
     )
   )

 
(deftest competeAllCluster-test
  (testing "when two clusters compete for the whole cluster the first gets it"
    (let [cluster (cluster/initOmegaCluster omegaIter)
          task (wrapWithNotifyOnFinished (fn [] (println "Run task!")) "fr1" cluster)
          fr1 (framework/createFramework "fr1" [task])
          fr2 (framework/createFramework "fr2" [task])
         ]
         (is (= [] (flatten (framework/getClusterTasks (runClusterTillNoTask cluster))))))))


;; Test that one framework asks for all cpus and then another framework and see that is the first
;; Test that both ask for half of the resources and they both get them
;; Test that one framework asks always before the other and always gets the resources (1/2/1/2) -
;;   2 always gets refused


