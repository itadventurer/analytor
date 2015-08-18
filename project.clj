(defproject analytor "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [org.clojure/java.jdbc "0.4.1"]
                 [org.postgresql/postgresql "9.4-1201-jdbc41"]
                 ]
  :repositories [["snapshots" {:url "https://maven.azapps.de/artifactory/mirakeldb-snapshot"}]
                 ["releases" {:url "https://maven.azapps.de/artifactory/mirakeldb-releases"}]])
