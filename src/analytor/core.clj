(ns analytor.core
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.test :refer [with-test is are]]
            [schema.core :as s]))

(s/defschema Type
  [(s/one s/Keyword "cname") (s/optional {s/Keyword s/Any} "params")])

(s/defschema Columns
  [[(s/one s/Keyword "column-name") (s/one Type "column-type")]])

(s/defschema ForeignKey
  {:column s/Keyword
   :target-table s/Keyword
   :target-column s/Keyword})

(s/defschema Table
  {:columns Columns
   :primary-key (s/maybe [s/Keyword])
   :foreign-keys (s/maybe [ForeignKey])})

(s/defschema Analysis
  [[(s/one s/Keyword "table-name") (s/one Table "table")]])

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
          mappings (eval (symbol (str "analytor.core/analyzer-data-type-aliases-" (name db-type))))]
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
  `(doall (resultset-seq
           (with-db-metadata ~binding
             ~@body))))


(s/defn transform-column :- [(s/one s/Keyword "name") (s/one Type "type")]
  "Transform the column metadata"
  [column conn]
  (let [column-name (:column_name column)
        column-type (clojure.string/lower-case (:type_name column))
        datatype (match-datatype (match-db-type
                                  (with-db-metadata [md conn]
                                    (.getURL md)))
                                 column-type)
        additional (analyze-data-type-args datatype column)]
    (if (empty? additional)
      [(keyword column-name) [datatype]]
      [(keyword column-name) [datatype additional]])))

(defn vec-or-nil
  [v]
  (when (seq v)
    (vec v)))

(s/defn transform-table :- [(s/one s/Keyword "table-name") (s/one Table "table-data")]
  "Transform the table metadata"
  [table conn]
  (let [table-name (:table_name table)
        columns (with-db-metadata-seq [md conn]
                  (.getColumns md  nil nil table-name nil))
        column-spec (map #(transform-column % conn) columns)
        primary-key (vec-or-nil
                     (map (comp keyword :column_name)
                          (with-db-metadata-seq [md conn]
                            (.getPrimaryKeys md nil nil table-name))))
        foreign-keys (vec-or-nil
                      (map
                       (fn [imported-key]
                         {:column (keyword (:fkcolumn_name imported-key))
                          :target-table (keyword (:pktable_name imported-key))
                          :target-column (keyword (:pkcolumn_name imported-key))})
                       (with-db-metadata-seq [md conn]
                         (.getImportedKeys md nil nil table-name))))]
    [(keyword table-name) {:columns column-spec
                           :primary-key primary-key
                           :foreign-keys foreign-keys}]))

(s/defn analyze :- Analysis
  "Analyze the all tables in a given database schema"
  [conn]
  ;; We put it in a transaction. That guarantees us that we have a
  ;; `:connection` key in the `conn` map. We also never call a
  ;; function from jdbc that would close the connection, because that
  ;; would break a lot of things
  (jdbc/with-db-transaction [conn conn]
    (let [tables (with-db-metadata-seq [md conn]
                   (.getTables md nil nil nil (into-array ["TABLE" "VIEW"])))]
      (doall (map #(transform-table % conn) tables)))))
