# Analytor

[![Clojars Project](https://img.shields.io/clojars/v/analytor.svg)](https://clojars.org/analytor)

Analytor is a library to analyze the database schema and structure. It
is inspired (and reuses some code) by the analyzer
from [lobos](https://github.com/budu/lobos/), but does not need any
global state.

## Usage

Analytor exposes one function, `analyze` which takes a connection and
returns a Analysis data-structure:

```clj
user> (require '[analytor.core :refer :all])
user> (analyze test-db-connection)
[{:table-name :fruit,
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
```

The tables and columns appear in the same order as present in the
database.

## Supported Databases

Analytor should support the same databases as lobos:

* H2
* MySQL
* PostgreSQL
* SQLite
* MS SQL Server

But is currently only tested with PostgreSQL. If you find any issues
with other DBMS please open an issue.

## License

Copyright © 2016 Anatoly Zelenin, Georg Semmler

Distributed under the [Eclipse Public License](https://opensource.org/licenses/eclipse-1.0.php), the same as Clojure.
