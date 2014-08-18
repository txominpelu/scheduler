(defproject scheduler "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [ 
                  [org.clojure/clojure "1.6.0"]
                  [org.clojure/core.async "0.1.303.0-886421-alpha"]
                  [org.clojure/core.typed "0.2.66"]
                  [org.clojure/core.match "0.2.1"]
                 ]
  :main ^:skip-aot scheduler.core
  :target-path "target/%s"
  :plugins [[lein-typed "0.3.5"]]
  :core.typed {:check [scheduler.channel 
                       scheduler.framework 
                       scheduler.core 
                       scheduler.omega
                       scheduler.cluster 
                       scheduler.types]}
  :profiles {:uberjar {:aot :all}})
