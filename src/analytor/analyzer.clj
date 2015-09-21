(ns analytor.analyzer
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.test :refer [with-test is are]]))
;; using apply here is not an option because apply does not like Java functions

(def db-hierarchy
  (-> (make-hierarchy)
      (derive :h2 ::standard)
      (derive :mysql ::standard)
      (derive :postgresql ::standard)
      (derive :sqlite ::standard)
      (derive :microsoft-sql-server ::standard)))

(defn isPostgres? [url]
  (string? (re-find #"postgresql" url)))

(defn isMySql? [url]
  (string? (re-find #"mysql" url)))

(defn isSqlite? [url]
  (string? (re-find #"sqlite" url)))

(defn isH2? [url]
  (string? (re-find #"h2" url)))

(defn isMicrosoft? [url]
  (string? (re-find #"microsoft:sqlserver" url)))

(with-test
  (defn match-db-type
    "Matches the jdbc-url to a database type"
    [url]
    (cond
      (isPostgres? url) :postgresql
      (isMySql? url) :mysql
      (isMicrosoft? url) :microsoft-sql-server
      (isSqlite? url)  :sqlite
      (isH2? url)  :h2
      :else ::standard))
  (are [uri db-type] (= db-type (match-db-type uri))
    "jdbc:postgresql://localhost:5432/analytor_test" :postgresql
    "jdbc:mysql://HOST/DATABASE" :mysql
    "jdbc:microsoft:sqlserver://HOST:1433;DatabaseName=DATABASE" :microsoft-sql-server
    "jdbc:sqlite:///COMPUTERNAME/shareA/dirB/dbfile" :sqlite
    "jdbc:h2:tcp://localhost//data/test" :h2))


(defn analyze-data-type-args
  "Returns a vector containing the data type arguments for the given
  column meta data."
  [dtype column-meta]
  (condp contains? dtype
    #{:nvarchar :varbinary :varchar}
    {:size (:column_size column-meta)}

    #{:binary :blob :char :clob :nchar :nclob :float :time :timestamp}
    {:size (:column_size column-meta)}

    #{:decimal :numeric}
    (let [scale (:decimal_digits column-meta)]
      (if (= scale 0)
        {:size (:column_size column-meta)}
        {:size (:column_size column-meta) :digits scale}))

    {}))


(defmulti match-datatype (fn [db-type data-type] db-type))

(defmethod match-datatype ::standard
  [_ data-type]
  (keyword data-type))

(defmacro match-datatype-imp [db-type]
  (defmethod match-datatype db-type
    [_ data-type]
    (let [data-type (match-datatype ::standard data-type)
          mappings (eval (symbol (str "analytor.analyzer/analyzer-data-type-aliases-" (name db-type))))]
      (if (contains? mappings data-type)
        (data-type mappings)
        data-type))))

(def analyzer-data-type-aliases-postgresql
  {:bool :boolean
   :bpchar :char
   :bytea :blob
   :float4 :real
   :float8 :double
   :int2 :smallint
   :int4 :integer
   :int8 :bigint
   :text :nclob
   :timestamptz :timestamp
   :timetz :time})

(match-datatype-imp :postgresql)

(def analyzer-data-type-aliases-microsoft-sql-server
  {:bit :boolean
   :datetime2 :timestamp
   :image :blob
   :int :integer
   :ntext :nclob
   :text :clob})

(match-datatype-imp :microsoft-sql-server)

(def analyzer-data-type-aliases-sqlite
  {:time-with-time-zone :time
   :timestamp-with-time-zone :timestamp})

(match-datatype-imp :sqlite)

(def analyzer-data-type-aliases-mysql
  {:bit :boolean
   :int :integer
   :text :clob
   :tinyblob :blob
   :tinytext :clob})

(match-datatype-imp :mysql)

(defmacro with-db-metadata
  "A wrapper that extracts the metadata from the current
  connection. The difference to the JDBC function is that it does not
  close the connection afterwards because it is handled by the
  with-db-transaction"
  [binding & body]
  `(let [^java.sql.Connection con# (:connection ~(second binding))
         ~(first binding) (.getMetaData con#)]
     ~@body))

(defmacro with-db-metadata-seq
  [binding & body]
  `(resultset-seq
    (with-db-metadata ~binding
      ~@body)))

(defn transform-column
  "Transform the column metadata"
  [column conn]
  (let [name (:column_name column)
        datatype (match-datatype (match-db-type
                                  (with-db-metadata [md conn]
                                    (.getURL md)))
                                 (:type_name column))
        additional (analyze-data-type-args datatype column)]
    (if (empty? additional)
      [(keyword name) [datatype]]
      [(keyword name) [datatype additional]])))

(defn transform-table
  "Transform the table metadata"
  [table conn]
  (let [table-name (:table_name table)
        columns (with-db-metadata-seq [md conn]
                  (.getColumns md  nil nil table-name nil))
        column-spec (map #(transform-column % conn) columns)]
    [(keyword table-name) (into {} column-spec)]))
(defn analyze
  "Analyze the all tables in a given database schema"
  [conn]
  ;; We put it in a transaction. That guarantees us that we have a
  ;; `:connection` key in the `conn` map. We also never call a
  ;; function from jdbc that would close the connection, because that
  ;; would break a lot of things
  (jdbc/with-db-transaction [conn conn]
    (let [tables (with-db-metadata-seq [md conn]
                   (.getTables md nil nil nil (into-array ["TABLE" "VIEW"])))]
      (into {} (map #(transform-table % conn) tables)))))
