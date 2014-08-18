(ns scheduler.channel
  (:require [clojure.core.async :as async]
            [clojure.core.typed :as t]
            [clojure.core.typed.async :as ta]
            [scheduler.types :as ts]
     )
  )

(t/ann ^:no-check readAll 
       (t/All [x]  [(t/Seqable (ta/Chan x)) -> (t/Seqable (ts/Message))])   )
(defn readAll
  " blocks to get one and then takes till there's no more element in the channels"
  [chs]
  (let [[fl fch] (async/alts!! chs)
        step (fn step [acc] 
                 (let [ [l ch]  (async/alts!! (conj chs (async/timeout 100)) :default nil)] 
                  (if (= nil l)
                    acc
                    (step (conj acc {:content l :channel ch})))))]
      (step [{:content fl :channel fch}])))

(t/ann belongsTo? [(ta/Chan t/Any) -> [ts/Message -> Boolean]])
(defn belongsTo?
  [channel] 
  (fn [{ch :channel}] (= channel ch)))

