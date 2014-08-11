(ns scheduler.framework-test
  (:require [clojure.test :refer :all]
            [clojure.core.async :as async]
            [scheduler.framework :refer :all]
     ))

(deftest updateFramework-test
  (testing "Test that the list of frameworks is updated"
    (let [registerCh (async/chan)
          deRegisterCh (async/chan)
          initialFrameworks (list "framework1" "framework2" "framework3")]
      (is (= (updateFrameworks initialFrameworks ["framework4"] ["framework1"])  #{"framework2" "framework3" "framework4"})))))
