(ns scheduler.fullresources-test
  (:require [clojure.test :refer :all]
            [scheduler.fullresources :refer :all]
  ))

(def clusterResources
  {:slave1 {:cpus 4 :memory 8} 
   :slave2 {:cpus 4 :memory 8} 
   :slave3 {:cpus 4 :memory 8} 
   :slave4 {:cpus 4 :memory 8} })

(def nodeDemand1
  {:slave1 {:cpus 4 :memory 8}})

(deftest minusResources-test
  (testing "cannot ask twice for the same resource"
    (let [newRes (minusResources clusterResources nodeDemand1)]
      (is (= newRes {:slave1 {:cpus 0 :memory 0} 
                     :slave2 {:cpus 4 :memory 8} 
                     :slave3 {:cpus 4 :memory 8} 
                     :slave4 {:cpus 4 :memory 8}})))))

(deftest plusResources-test
  (testing "cannot ask twice for the same resource"
    (let [newRes (plusResources clusterResources nodeDemand1)]
      (is (= newRes {:slave1 {:cpus 8 :memory 16} 
                     :slave2 {:cpus 4 :memory 8} 
                     :slave3 {:cpus 4 :memory 8} 
                     :slave4 {:cpus 4 :memory 8}})))))
