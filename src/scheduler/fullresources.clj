(ns scheduler.fullresources
  (:require 
            [clojure.core.typed :as t]
            [scheduler.types :as ts]
            [scheduler.utils :as utils]
            [scheduler.resources :as resources]
  ))



;;(t/ann getNode [ts/FullResources t/AnyInteger -> ts/FullResources])
;;(defn getNode
;;  [resources n]
;;  (assert (contains? [1 2 3 4] n))
;;  (let [key (keyword (str "slave" n))]
;;    (key resources)))


;; Resources
(t/ann slaveOrEmpty [ts/FullResources t/Keyword -> ts/Resources])
(defn slaveOrEmpty
  [res key]
  (if (nil? (key res))
    resources/emptyResources
    (key res)))

(def emptyResources
  {:slave1 resources/emptyResources
   :slave2 resources/emptyResources
   :slave3 resources/emptyResources
   :slave4 resources/emptyResources })

(t/ann normalize [ts/FullResources -> ts/FullResources])
(defn normalize
  [res]
  (utils/tuplesToMap (for [k [:slave1 :slave2 :slave3 :slave4]] [k (slaveOrEmpty res k)])))

(t/ann <= [ts/FullResources ts/FullResources -> Boolean])
(defn <=
  [res1 res2]
  (let [res1 (normalize res1)
        res2 (normalize res2)]
    ;;(println (str res1 " <= " res2 " == " (every? true? (for [[k v] res1] (resources/<= (k res1) (k res2))))))
    (every? true? (for [[k v] res1] (resources/<= (k res1) (k res2))))))


(t/ann minusResources [ts/FullResources ts/FullResources -> ts/FullResources])
(defn minusResources
  [res1 res2]
  (let [res1 (normalize res1)
        res2 (normalize res2)]
    (utils/tuplesToMap (for [[k v] res1] [k (resources/minusResources (k res1) (k res2))]))))

(t/ann plusResources [ts/FullResources ts/FullResources -> ts/FullResources])
(defn plusResources
  ([]
   {:slave1 resources/emptyResources})
  ([res1 res2]
    (let [res1 (normalize res1)
        res2 (normalize res2)]
      (utils/tuplesToMap (for [[k v] res1] [k (resources/plusResources (k res1) (k res2))])))))

(t/ann aggregatedResources [ts/FullResources -> ts/Resources])
(defn aggregatedResources
  [res]
  (reduce resources/plusResources (map second res)))


(defn canMakeOfferFor
  [res1 res2]
  (assert (= (count (keys res2)) 1)) 
  (let [res1 (normalize res1)]
    (>  (count (for [[k v] res1 :when (resources/<= (first (vals res2)) v)] k)) 0)))

(defn makeOfferFor
  [res1 res2]
  (assert (= (count (keys res2)) 1)) 
  (let [res1 (normalize res1) ]
    (utils/tuplesToMap [(first (for [[k v] res1 :when (resources/<= (first (vals res2)) v)] [k (first (vals res2))]))])))

