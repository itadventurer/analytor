(defproject analytor "0.2.0"
  :description "A library to analyze database schemas"
  :url "https://github.com/azapps/analytor"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [org.clojure/java.jdbc "0.4.1"]
                 [org.postgresql/postgresql "9.4-1201-jdbc41"]
                 [prismatic/schema "1.0.4"]
                 ])
