(ns scheduler.channel
  (:require [clojure.core.async :as async]
            [clojure.core.typed :as t]
            [clojure.core.typed.async :as ta]
            [scheduler.types :as ts]
     )
  )

(t/ann ^:no-check readAll [(t/Seqable (ta/Port t/Any)) -> (t/Seqable (ts/Message))])
(defn readAll
  " blocks to get one and then takes till there's no more element in the channels"
  [chs]
  (let [[fl fch] (async/alts!! chs)
        step (t/ann-form (fn step [acc] 
                 (let [ [l ch]  (async/alts!! (conj chs (async/timeout 100)) :default nil)] 
                  (if (= nil l)
                    acc
                    (step (conj acc {:content l :channel ch}))))) [(t/Seqable ts/Message) -> (t/Seqable ts/Message)])]
      (step [{:content fl :channel fch}])))

(t/ann ^:no-check belongsTo? [(ta/Port t/Any) -> [ts/Message -> Boolean]])
(defn belongsTo?
  [channel] 
  (t/ann-form (fn [msg] (= channel (:channel msg))) [ts/Message -> Boolean]))

(defn belongingTo
  [events channel]
  (map :content (filter (belongsTo? channel) events)))
