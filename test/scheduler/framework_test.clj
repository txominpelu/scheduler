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
      (async/thread (async/>!! registerCh "framework4"))
      (async/thread (async/>!! deRegisterCh "framework1"))
      (is (= (updateFrameworks initialFrameworks registerCh deRegisterCh)  #{"framework2" "framework3" "framework4"})))))
