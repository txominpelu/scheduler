(ns scheduler.framework
  (:require [clojure.core.async :as async]
            [scheduler.channel :as channel]
     ))

(defn minus
  [all toDelete]
  (clojure.set/difference (set all) (set toDelete)))

(defn add
  [all toAdd]
  (clojure.set/union (set all) (set toAdd)))

(defn updateFrameworks
  [frameworks registerCh deRegisterCh]
  (let [register (channel/readAll registerCh)
        deRegister (channel/readAll deRegisterCh)]
    (minus (add frameworks register) deRegister)))

