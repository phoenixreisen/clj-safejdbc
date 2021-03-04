(ns clj-safejdbc.core-test
  (:require [clojure.test :refer :all]
            [clj-safejdbc.core :refer :all])
  (:import (java.sql DriverManager Timestamp)
           (java.text SimpleDateFormat)))
;-------------------------------------------------------------------------------
(Class/forName "org.h2.Driver")
;-------------------------------------------------------------------------------
(defn get-connection 
  "Liefert eine Connection zur In-Memory-DB mit dem Namen `db-name`.
   Die DB wird mit der ersten Connection erzeugt und existiert bis die JVM
   beendet wird."
  [db-name]
  (DriverManager/getConnection 
    (str "jdbc:h2:mem:" db-name ";DB_CLOSE_DELAY=-1")))
;-------------------------------------------------------------------------------
(defn ^:private ts [s]
  (-> (SimpleDateFormat. "dd.MM.yyyy")
      (.parse s)
      (.getTime)
      (Timestamp.)))
;-------------------------------------------------------------------------------
(def create-table-person-sql 
  "create table Person(id int, firstname varchar(255), lastname varchar(255),
   birthdate timestamp)")
;-------------------------------------------------------------------------------
(deftest execute-test 
  (let [con (get-connection "execute-test")
        _ (execute con update! create-table-person-sql)
        _ (execute (get-connection "execute-test") 
            update! "insert into Person (id, firstname, lastname, birthdate) 
                     values (1, 'Jan', 'Hermanns', '1975-04-22 10:30:00')")
        _ (execute (get-connection "execute-test") 
            update! "insert into Person (id, firstname, lastname, birthdate) 
                     values (2, 'Johanna', 'Hermanns', '2012-07-08 22:00:00')")
        n (execute (get-connection "execute-test") 
            query-single-value "select count(*) from Person")]
    (is (.isClosed con))
    (is (= 2 n))))
;-------------------------------------------------------------------------------
(deftest sql-executer-test 
  (let [exec (sql-executer #(get-connection "sql-executer-test"))
        _ (exec update! create-table-person-sql)
        _ (exec update! "insert into Person (id, firstname, lastname, birthdate) 
                         values (1, 'Jan', 'Hermanns', '1975-04-22 10:30:00')")
        _ (exec update! "insert into Person (id, firstname, lastname, birthdate) 
                         values (2, 'Johanna', 'Hermanns', '2012-07-08 22:00:00')")
        n (exec query-single-value "select count(*) from Person")]
    (is (= 2 n))))
;-------------------------------------------------------------------------------
(deftest test1
  (binding [clj-safejdbc.core/*log* (fn [x] (println x))]

    ;; Wir erzeugen eine anonyme in-memory DB, die nur solange existiert, bis
    ;; die Connection geschlossen wird.
    (dotx (DriverManager/getConnection "jdbc:h2:mem:") 
      [_ (update! create-table-person-sql)
       _ (update! "insert into Person (id, firstname, lastname, birthdate) 
                   values (1, 'Jan', 'Hermanns', '1975-04-22 10:30:00')")
       _ (update! "insert into Person (id, firstname, lastname, birthdate) 
                   values (2, 'Kathrin', 'Hermanns', '1979-02-12 16:10:00')")
       n (query-single-value "select count(*) from Person")]
      (is (= n 2)))))
;-------------------------------------------------------------------------------
(deftest test2
  (let [insert-person-sql "insert into Person (id, firstname, lastname, birthdate) values (?, ?, ?, ?)"]

    ;; Wir erzeugen eine anonyme in-memory DB, die nur solange existiert, bis
    ;; die Connection geschlossen wird.
    (dotx (DriverManager/getConnection "jdbc:h2:mem:") 
          [_ (update! create-table-person-sql)
           _ (update! [insert-person-sql 1, "Jan", "Hermanns", (ts "22.04.1975")])
           _ (update! [insert-person-sql 2, "Kathrin", "Dumrath", (ts "12.02.1979")])
           _ (query-single-value "select count(*) from Person")
           _ (update! ["update Person set lastname=? where id=2" "Hermanns"])
           k (query-first "select * from Person where id=2")
           ]
          (is (and (= (:firstname k) "Kathrin")
                   (= (:lastname k) "Hermanns"))))))
;-------------------------------------------------------------------------------
