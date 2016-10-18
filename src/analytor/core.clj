(ns analytor.core
  (:require
   [clojure.java.jdbc :as jdbc]
   [schema.core :as s]))

(defn strict-map
  [& args]
  (doall (apply map args)))

(s/defschema Type
  [(s/one s/Keyword "type") (s/optional {s/Keyword s/Any} "params")])

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
      (if (zero? scale)
        {:size (:column_size column-meta)}
        {:size (:column_size column-meta) :digits scale}))

    {}))


(def data-type-aliases
  {:postgresql
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
    :timetz :time}
   :microsoft-sql-server
   {:bit :boolean
    :datetime2 :timestamp
    :image :blob
    :int :integer
    :ntext :nclob
    :text :clob}
   :sqlite
   {:time-with-time-zone :time
    :timestamp-with-time-zone :timestamp}
   :mysql
   {:bit :boolean
    :int :integer
    :text :clob
    :tinyblob :blob
    :tinytext :clob}})

(defn match-datatype
  [db-type data-type]
  (let [data-type (keyword data-type)]
    (or (get-in data-type-aliases [db-type data-type])
        data-type)))

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
        column-spec (strict-map #(transform-column % conn) columns)
        primary-key (vec-or-nil
                     (strict-map (comp keyword :column_name)
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
      (strict-map #(transform-table % conn) tables))))
