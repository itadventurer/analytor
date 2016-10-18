(ns analytor.core-test
  (:require [analytor.core :refer :all]
            [clojure.test :refer :all]
            [clojure.java.jdbc :as jdbc]
            [schema.core :as s]))

(s/set-fn-validation! true)

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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Tests

(deftest test-match-db-type
  (are [uri db-type] (= db-type (match-db-type uri))
    "jdbc:postgresql://localhost:5432/analytor_test" :postgresql
    "jdbc:mysql://HOST/DATABASE" :mysql
    "jdbc:microsoft:sqlserver://HOST:1433;DatabaseName=DATABASE" :microsoft-sql-server
    "jdbc:sqlite:///COMPUTERNAME/shareA/dirB/dbfile" :sqlite
    "jdbc:h2:tcp://localhost//data/test" :h2))

(deftest simple-test
  (is (= [{:table-name :fruit,
           :columns
           [{:column-name :id,
             :data-type {:type :integer},
             :nullable? true,
             :autoincrement? false}
            {:column-name :name,
             :data-type {:size 32, :type :varchar},
             :nullable? true,
             :autoincrement? false}
            {:column-name :appearance,
             :data-type {:size 32, :type :varchar},
             :nullable? true,
             :autoincrement? false}
            {:column-name :cost,
             :data-type {:type :integer},
             :nullable? true,
             :autoincrement? false}
            {:column-name :grade,
             :data-type {:type :real},
             :nullable? true,
             :autoincrement? false}],
           :primary-key nil,
           :foreign-keys nil}]
         (analyze test-db-connection))))

(deftest simple-transaction-test
  (is (= [{:table-name :fruit,
           :columns
           [{:column-name :id,
             :data-type {:type :integer},
             :nullable? true,
             :autoincrement? false}
            {:column-name :name,
             :data-type {:size 32, :type :varchar},
             :nullable? true,
             :autoincrement? false}
            {:column-name :appearance,
             :data-type {:size 32, :type :varchar},
             :nullable? true,
             :autoincrement? false}
            {:column-name :cost,
             :data-type {:type :integer},
             :nullable? true,
             :autoincrement? false}
            {:column-name :grade,
             :data-type {:type :real},
             :nullable? true,
             :autoincrement? false}],
           :primary-key nil,
           :foreign-keys nil}]
         (jdbc/with-db-connection [test-db-connection test-db-connection]
           (analyze test-db-connection)))))
