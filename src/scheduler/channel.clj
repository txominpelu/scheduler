(ns scheduler.channel
  (:require [clojure.core.async :as async]
            [clojure.core.typed :as t]
            [clojure.core.typed.async :as ta]
     )
  )

(t/ann ^:no-check readAll (t/All [x]  [(ta/Chan x) -> (t/Seqable x)])   )
(defn readAll
  [ch]
  (let [step (fn step [acc] 
               (let [ [l ch] (async/alts!! [ch (async/timeout 100)] :default nil) ] 
                (if (= nil l)
                  acc
                  (step (conj acc l)))))]
    (step [])))
