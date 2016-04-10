# Analytor

[![Clojars Project](https://img.shields.io/clojars/v/analytor.svg)](https://clojars.org/analytor)

Analytor is a library to analyze the database schema and structure. It
is inspired (and reuses some code) on the analyzer from
[lobos](https://github.com/budu/lobos/), but does not need any global
state.


## Usage

Analytor exposes one function, `analyze` which takes a connection and
returns a Analysis data-structure:

```clj
user> (require '[analytor.core :refer :all])
user> (analyze test-db-connection)
([:accounts
  {:columns
   ([:id [:serial]]
    [:name [:varchar {:size 255}]]
    [:description [:nclob {:size 2147483647}]]),
   :primary-key [:id],
   :foreign-keys nil}]
 [:accounts_users
  {:columns
   ([:account_id [:integer]]
    [:user_id [:integer]]
    [:is_admin [:boolean]]),
   :primary-key [:account_id :user_id],
   :foreign-keys
   [{:column :account_id, :target-table :accounts, :target-column :id}
    {:column :user_id, :target-table :users, :target-column :id}]}]
 [:users
  {:columns
   ([:id [:serial]]
    [:email [:varchar {:size 255}]]
    [:encrypted_password [:varchar {:size 255}]]),
   :primary-key [:id],
   :foreign-keys nil}])
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
