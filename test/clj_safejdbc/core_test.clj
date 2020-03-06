(ns clj-safejdbc.core-test
  (:require [clojure.test :refer :all]
            [clj-safejdbc.core :refer :all])
  (:import (java.sql DriverManager Timestamp)
           (java.text SimpleDateFormat)))

(Class/forName "org.h2.Driver")

(defn get-connnection []
  (DriverManager/getConnection "jdbc:h2:mem:test"))

(defn ts [s]
  (-> (SimpleDateFormat. "dd.MM.yyyy")
      (.parse s)
      (.getTime)
      (Timestamp.)))

(def create-table-person-sql 
  "create table Person(id int, firstname varchar(255), lastname varchar(255),
   birthdate timestamp)")

(def insert-person-sql 
  "insert into Person (id, firstname, lastname, birthdate) values (?, ?, ?, ?)")

(deftest test1
  (binding [clj-safejdbc.core/*log* (fn [x] (println x))]
    (dotx (get-connnection) 
      [_ (update! create-table-person-sql)
       _ (update! "insert into Person (id, firstname, lastname, birthdate) 
                   values (1, 'Jan', 'Hermanns', '1975-04-22 10:30:00')")
       _ (update! "insert into Person (id, firstname, lastname, birthdate) 
                   values (2, 'Kathrin', 'Hermanns', '1979-02-12 16:10:00')")
       n (query-single-value "select count(*) from Person")]
      (is (= n 2)))))

(deftest test2
  (dotx (get-connnection) 
    [_ (update! create-table-person-sql)
     _ (update! [insert-person-sql 1, "Jan", "Hermanns", (ts "22.04.1975")])
     _ (update! [insert-person-sql 2, "Kathrin", "Dumrath", (ts "12.02.1979")])
     _ (query-single-value "select count(*) from Person")
     _ (update! ["update Person set lastname=? where id=2" "Hermanns"])
     k (query-first "select * from Person where id=2")
     ]
    (is (and (= (:firstname k) "Kathrin")
             (= (:lastname k) "Hermanns")))))

