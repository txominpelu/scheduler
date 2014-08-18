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
                                      :finishedCh (ta/Chan t/Any)
                                      :iter [ t/Any -> t/Any] ;; In reality is [t/Cluster -> t/Any] but
                                                              ;; it doesn't work when running the tests
                                      :demandsCh (ta/Chan t/Any)
                                      :registerCh (ta/Chan t/Any)
                                      }
                             ))
;; Channel
(t/defalias Message (t/HMap :mandatory {:content t/Any :channel (ta/Chan t/Any)}))

;; Omega demands
(t/defalias OmegaDemand (t/HMap :mandatory {:resources Resources}))
