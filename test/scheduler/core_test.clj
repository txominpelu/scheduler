(ns scheduler.core-test
  (:require [clojure.test :refer :all]
            [clojure.core.async :as async :refer :all]
            [scheduler.core :refer :all]))

(deftest updateFramework-test
  (testing "Test that the list of frameworks is updated"
    (let [registerCh (chan)
          deRegisterCh (chan)
          initialFrameworks (list "framework1" "framework2" "framework3")]
      (thread (>!! registerCh "framework4"))
      (thread (>!! deRegisterCh "framework1"))
      (close! registerCh)
      (close! deRegisterCh)
      (is (= (updateFrameworks initialFrameworks registerCh deRegisterCh)  1)))))
