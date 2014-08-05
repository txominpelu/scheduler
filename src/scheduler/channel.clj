(ns scheduler.channel
  (:require [clojure.core.async :as async]
            [clojure.core.typed :as t]
            [clojure.core.typed.async :as ta]
     )
  )

(t/ann ^:no-check readAll (t/All [x]  [(ta/Chan x) -> (t/Seqable x)])   )
(defn readAll
  [ch]
  (let [step (t/cf (fn step [acc] 
               (let [ [l ch] (async/alts!! [ch (t/cf (async/timeout 100) (t/Seqable x))] :default nil) ] 
                (if (= nil l)
                  acc
                  (step (conj acc l))))) [(t/Seqable x) -> (t/Seqable x)])]
    (step [])))
