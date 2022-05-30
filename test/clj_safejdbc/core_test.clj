(ns clj-safejdbc.core-test
  (:require [clojure.test :refer :all]
            [clj-safejdbc.core :refer :all])
  (:import (java.sql DriverManager Timestamp SQLException)
           (java.text SimpleDateFormat)
           (java.util UUID)))
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
(defn ^:private uuid []
  (str (UUID/randomUUID)))
;-------------------------------------------------------------------------------
(defn ^:private connection-provider 
  "Liefert eine FN zurueck, die bei Aufruf eine Connection liefert."
  []
  (let [uuid (str (UUID/randomUUID))]
    (fn [] (get-connection uuid))))
;-------------------------------------------------------------------------------
(defn ^:private get-sql-executer []
  (sql-executer (connection-provider)))
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
  (let [uuid (uuid)
        con (get-connection uuid)
        _ (execute con update! create-table-person-sql)
        _ (execute (get-connection uuid) 
            update! "insert into Person (id, firstname, lastname, birthdate) 
                     values (1, 'Jan', 'Hermanns', '1975-04-22 10:30:00')")
        _ (execute (get-connection uuid) 
            update! "insert into Person (id, firstname, lastname, birthdate) 
                     values (2, 'Johanna', 'Hermanns', '2012-07-08 22:00:00')")
        n (execute (get-connection uuid) 
            query-single-value "select count(*) from Person")]
    (is (.isClosed con))
    (is (= 2 n))))
;-------------------------------------------------------------------------------
(deftest sql-executer-test 
  (let [exec (get-sql-executer)
        _ (exec update! create-table-person-sql)
        _ (exec update! "insert into Person (id, firstname, lastname, birthdate) 
                         values (1, 'Jan', 'Hermanns', '1975-04-22 10:30:00')")
        _ (exec update! "insert into Person (id, firstname, lastname, birthdate) 
                         values (2, 'Johanna', 'Hermanns', '2012-07-08 22:00:00')")
        n (exec query-single-value "select count(*) from Person")]
    (is (= 2 n))))
;-------------------------------------------------------------------------------
(deftest apply-to-tx-test 
  (let [insert-person-sql "insert into Person (id, firstname, lastname, birthdate) values (?, ?, ?, ?)"
        con-provider (connection-provider)
        select-person (fn [id] 
                        (exec (con-provider) 
                          (query-first ["select * from Person where id=?" id])))]
    (apply-to-tx (con-provider) 
      (update! create-table-person-sql)
      (update! [insert-person-sql 1, "Jan", "Hermanns", (ts "22.04.1975")])
      (update! [insert-person-sql 2, "Kathrin", "Dumrath", (ts "12.02.1979")]))
    (let [{:keys [firstname lastname]} (select-person 2)]
      (is (and (= firstname "Kathrin") (= lastname  "Dumrath"))))
    (try 
      (apply-to-tx (con-provider) 
        (update! ["update Person set firstname=?" "Jan Philipp"])
        (update! ["updat Person set lastname=? where id=2" "Hermanns"]))
      (catch SQLException _))
    (let [jan (select-person 1)
          kathrin (select-person 2)]
      (is (and (= (:firstname jan) "Jan") (= (:lastname jan) "Hermanns")))
      (is (and (= (:firstname kathrin) "Kathrin") (= (:lastname kathrin) "Dumrath"))))))
;-------------------------------------------------------------------------------
(deftest dotx-test
  (let [insert-person-sql "insert into Person (id, firstname, lastname, birthdate) values (?, ?, ?, ?)"]
    (dotx (get-connection (uuid)) 
      [_ (update! create-table-person-sql)
       _ (update! [insert-person-sql 1, "Jan", "Hermanns", (ts "22.04.1975")])
       _ (update! [insert-person-sql 2, "Kathrin", "Dumrath", (ts "12.02.1979")])
       _ (query-single-value "select count(*) from Person")
       _ (update! ["update Person set lastname=? where id=2" "Hermanns"])
       k (query-first "select * from Person where id=2")]
      (is (and (= (:firstname k) "Kathrin")
            (= (:lastname k) "Hermanns"))))))
;-------------------------------------------------------------------------------
(deftest log-test 
  (testing "Mit diesem Test stellen wir sicher, dass ein ggf. vorhandener
            Logger (zum Loggen der SQL-Statements) korrekt aufgerufen wird."
    (let [logs (atom [])
          logger (fn [x] (swap! logs conj x))
          exec (get-sql-executer)
          sql-insert1 "insert into Person (id, firstname) values (1, 'Jan')"
          sql-insert2 ["insert into Person (id, firstname) values (?, ?)" 
                       2 "Johanna"]]
      (with-bindings {#'*log* logger}
        (exec update! create-table-person-sql)
        (exec update! sql-insert1)
        (exec update! sql-insert2))
      (is (= @logs [create-table-person-sql sql-insert1 sql-insert2])))))
;-------------------------------------------------------------------------------
