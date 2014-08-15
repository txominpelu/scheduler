(ns scheduler.omega-test
  (:require [clojure.test :refer :all]
            [clojure.core.async :as async]
            [scheduler.omega :refer :all]
            [scheduler.framework :as framework]
            [scheduler.cluster :as cluster]
     )
   )

 
;;(deftest runClusterTillNoTask-test
;;  (testing "run till there is no more task"
;;    (let [cluster (initCluster)
;;          task (wrapWithNotifyOnFinished (fn [] (println "Run task!")) "fr1" cluster)
;;          framework (framework/createFramework "fr1" [task])]
;;      (cluster/registerFramework cluster framework)
;;      (is (= [] (flatten (framework/getClusterTasks (runClusterTillNoTask cluster))))))))


