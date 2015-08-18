(ns analytor.analyzer-test
  (:require [analytor.analyzer :refer :all]
            [clojure.test :refer :all]
            [clojure.java.jdbc :as jdbc]))

(def test-db-connection
  {:subprotocol "postgresql"
   :subname "//localhost:5432/analytor_test"})

(defn create-db
  "Create a test database"
  []
  (jdbc/db-do-commands test-db-connection
                       (jdbc/create-table-ddl :fruit
                                              [:id :int]
                                              [:name "varchar(32)"]
                                              [:appearance "varchar(32)"]
                                              [:cost :int]
                                              [:grade :real])))

(defn destroy-db
  "Destroy the test database"
  []
  (jdbc/db-do-commands test-db-connection
                       (jdbc/drop-table-ddl :fruit)))

(defn analytor-test-fixture [f]
  (create-db)
  (f)
  (destroy-db))

(use-fixtures :once analytor-test-fixture)

(deftest simple-test
  (is (= [:fruit {:id [:integer]
                  :name [:varchar {:size 32}]
                  :appearance [:varchar {:size 32}]
                  :cost [:integer]
                  :grade [:real]}]
         (analyze test-db-connection))))
