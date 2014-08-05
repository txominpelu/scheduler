(ns scheduler.types
  (:require 
            [clojure.core.typed.async :as ta]
            [clojure.core.typed :as t]
     ))

(t/defalias Resources (t/HMap :mandatory {:cpus t/AnyInteger}))
(t/defalias Task [ -> t/Any])
(t/defalias Framework (t/HMap :mandatory {:tasks (t/Seqable Task) :name String}))
(t/defalias Cluster (t/HMap :mandatory {
                                      :resources Resources
                                      :frameworks (t/Seqable Framework)
                                      :registerCh (ta/Chan t/Any)
                                      :finishedCh (ta/Chan t/Any)
                                      :iter [ Cluster -> t/Any]
                                      }))
