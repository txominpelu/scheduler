(ns scheduler.channel
  (:require [clojure.core.async :as async]
            [clojure.core.typed :as t]
            [clojure.core.typed.async :as ta]
     )
  )

(t/ann ^:no-check readAll 
       (t/All [x]  [(t/Seqable (ta/Chan x)) -> (t/Seqable x)])   )
(defn readAll
  " blocks to get one and then takes till there's no more element in the channels"
  [chs]
  (let [step (fn step [acc] 
                 (let [ [l ch]  (async/alts!! (conj chs (async/timeout 100)) :default nil)] 
                  (if (= nil l)
                    acc
                    (step (conj acc l)))))]
      (step [(first (async/alts!! chs))])))

;;(defn readAll
  ;;([chs timeout] (readRecursive (fn []  (async/alts!! [(conj chs (async/timeout timeout))] :default nil))))
 ;; ([chs] (readRecursive (fn [] ))))
