(ns scheduler.channel
  (:require [clojure.core.async :as async])
  )

(defn readAll
  [ch]
  (let [step (fn step [acc] 
               (let [ [l ch] (async/alts!! [ch (async/timeout 100)]) ] 
                (if (= nil l)
                  acc
                  (step (conj acc l)))))]
    (step [])))
