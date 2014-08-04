(ns scheduler.channel
  (:require [clojure.core.async :as async]
            [clojure.core.typed :as t]
            [clojure.core.typed.async :refer [Chan]]
     )
  )

(t/ann readAll (t/All [x]  [(Chan x) -> (t/Seqable x)]))
(defn readAll
  [ch]
  (let [step (fn step [acc] 
               (let [ [l ch] (async/alts!! [ch (async/timeout 100)]) ] 
                (if (= nil l)
                  acc
                  (step (conj acc l)))))]
    (step [])))
