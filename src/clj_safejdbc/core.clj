;;; Jan Hermanns, 2014
(ns clj-safejdbc.core
  (:import [java.sql  SQLException Connection ResultSet PreparedStatement
            Statement ResultSetMetaData]))

(def ^:dynamic *log* 
  "If you want to log your SQL, then just bind `*log*` to a appropriate
   function of one argument."
  false)

;; This is mostly a bugfix for Microsofts sqljdbc4.jar JDBC driver (which
;; sometimes creates new Boolean Objects) and Clojure has this inconsistence
;; that "(if (new Boolean false) true)" returns true (see CLJ-1718).
(defn- normalize [x]
  (if (= (type x) Boolean) (.booleanValue x) x))

(defn generic-fill-prepared-statement
  "Fills the PreparedStatement with the values from the sequence
   data."
  [^PreparedStatement ps data]
  (loop [s data, i 1]
    (when (seq s)
      (.setObject ps i (first s))
      (recur (rest s) (inc i)))))

(defn map-rs
  "Returns a vector consisting of the result of applying f to
   each row of the ResultSet rs. Therefore f should accept a
   ResultSet as it's only argument."
  [f ^ResultSet rs]
  (letfn [(map-rs* [f ^ResultSet rs accu] 
            (if (.next rs)
              (recur f rs (conj accu (f rs)))
              accu))]
         (map-rs* f rs [])))

(defn get-column-indexes 
  "Returns a seq containing all column indexes. The seq always
   starts at 1." 
  [^ResultSetMetaData rs-meta]
   (let [column-count (.getColumnCount rs-meta)]
     (range 1 (inc column-count))))

(defn get-column-labels
  "Returns a seq containing all column labels as strings."
  [^ResultSet rs]
  (let [^ResultSetMetaData rs-meta (.getMetaData rs)]
    (reduce #(conj %1 (.getColumnLabel rs-meta ^int %2))
            [] 
            (get-column-indexes rs-meta))))

(defn rs->maps
  "Iterates over the ResultSet rs and transforms each row into a
   map, where the keys are the lowercase column labels as
   keywords and the values being the values. The maps are
   returned in order in a vector."
  [^ResultSet rs]
  (let [column-labels (get-column-labels rs)]
    (letfn [(row-fn [^ResultSet rs] 
              (reduce #(let[^String label %2]
                         (assoc %1 
; TODO I am very unhappy with the toLowerCase() call here. Maybe
; we should add another keyword arg (e.g. `column-label-fn')
; instead.
                                (keyword (.toLowerCase label))
                                (normalize (.getObject rs label))))
                      {} 
                      column-labels))]
           (map-rs row-fn rs))))

(defn- close-quietly [^Connection con]
  (try (.close con)
    (catch SQLException exc)))

(defn- rollback-quietly [^Connection con]
  (try (.rollback con)
    (catch SQLException exc)))

(defn rs->vecs
  "Iterates over the ResultSet rs and returns a vector of vectors
   (each vector containing the values of a row of the
   ResultSet)."
  [^ResultSet rs]
  (letfn [(row-fn [^ResultSet rs]
            (reduce #(conj %1 (normalize (.getObject rs ^int %2))) 
                    [] 
                    (get-column-indexes (.getMetaData rs))))]
    (map-rs row-fn rs)))

(def get-pk
  "This rs-fn is especially useful in conjunction with the
   insert! fn, to retrieve the values of automatically generated
   primary keys."
  (comp ffirst rs->vecs))

(defn apply-to-tx
  "Establishes a transactional context upon the Connection con
   and evaluates the functions in order.  The transactional
   connection is passed to each of the functions - therefore each
   fn must accept exactly one argument (the connection).  The
   result of the last fn gets returned."
  [^Connection con f & fs]
  (letfn [(apply-to-tx* [tx g & gs]
                 (let [result (g tx)]
                   (if (nil? gs)
                     result
                     (recur tx (first gs) (next gs)))))]
         (let [exc (atom nil)]
           (try
             (.setAutoCommit con false)
             (let [result (apply apply-to-tx* con f fs)]
               (.commit con)
               result)
             (catch Throwable t
               (reset! exc t)
               (rollback-quietly con))
             (finally
               (close-quietly con)
               (when @exc (throw @exc)))))))

(defn update!
  "Returns a fn that (when called with a Connection) executes the
   sql insert/update statement and returns the number of altered
   rows.
   Examples:
   (update! \"INSERT INTO Customers (Name, Age) VALUES ('Jan', 37)\")"
  [sql & {:keys [fill-fn] :or {fill-fn generic-fill-prepared-statement}}]
  (if *log* (*log* sql))
  (if (string? sql)
    (fn [^Connection con]
      (with-open [^Statement stmt (.createStatement con)]
        (.executeUpdate stmt sql)))
    (let [sql-str (first sql)
          data (rest sql)]
      (fn [^Connection con]
        (with-open [^PreparedStatement pstmt (.prepareStatement con sql-str)]
          (fill-fn pstmt data)
          (.executeUpdate pstmt))))))

(defn insert!
  "This function is useful, if you want to insert records which
   have an identity column.  It returns a fn that (when called
   with a Connection) executes the sql insert statement and
   returns the db-generated primary key as the result of the
   rs-fn.
   Examples:
   (insert \"INSERT INTO Customers (Name, Age) VALUES ('Jan', 37)\")"
  [sql & {:keys [rs-fn fill-fn] :or {rs-fn   get-pk 
                                     fill-fn generic-fill-prepared-statement}}]
  (if *log* (*log* sql))
  (if (string? sql)
    (fn [^Connection con]
      (with-open [^PreparedStatement pstmt 
                  (.prepareStatement con 
                                     ^String sql 
                                     Statement/RETURN_GENERATED_KEYS)]
        (.executeUpdate pstmt)
        (with-open [^ResultSet rs (.getGeneratedKeys pstmt)]
          (rs-fn rs))))
    (let [sql-str (first sql)
          data (rest sql)]
      (fn [^Connection con]
        (with-open [^PreparedStatement pstmt 
                    (.prepareStatement con 
                                       ^String sql-str 
                                       Statement/RETURN_GENERATED_KEYS)]
          (fill-fn pstmt data)
          (.executeUpdate pstmt)
          (with-open [^ResultSet rs (.getGeneratedKeys pstmt)]
            (rs-fn rs)))))))

(defn call!
  "Returns a fn that (when called with a Connection) executes the
   sql stored procedure and returns the number of altered rows.
   Examples:
   (call! [\"{call mystoredproc(?, ?)}\" 123 456])"
  [sql & {:keys [fill-fn] :or {fill-fn generic-fill-prepared-statement}}]
  (if *log* (*log* sql))
  (let [sql-str (first sql)
        data (rest sql)]
    (fn [^Connection con]
      (with-open [^PreparedStatement pstmt (.prepareCall con sql-str)]
        (fill-fn pstmt data)
        (.executeUpdate pstmt)))))

(defn call
  "Returns a fn that (when called with a Connection) executes the
   sql stored procedure and passes the ResultSet over to the
   rs-fn.  The result of the rs-fn is then returned.
   Examples:
   (call [\"{call mystoredproc(?, ?)}\" 123 456])"
  [sql & {:keys [rs-fn fill-fn] 
          :or {rs-fn   rs->maps
               fill-fn generic-fill-prepared-statement}}]
  (if *log* (*log* sql))
  (let [sql-str (first sql)
        data (rest sql)]
    (fn [^Connection con]
      (with-open [^PreparedStatement pstmt (.prepareCall con sql-str)]
        (fill-fn pstmt data)
        (with-open [^ResultSet rs (.executeQuery pstmt)]
          (rs-fn rs))))))

(defn query
  "Returns a fn that (when called with a Connection) executes the
   sql query and passes the ResultSet over to the rs-fn.  The
   result of the rs-fn is then returned.
   Examples:
   (query \"SELECT * FROM Customers\")
   (query [\"SELECT * FROM Customers WHERE Age=?\" 21])"
  [sql & {:keys [rs-fn fill-fn] 
          :or {rs-fn   rs->maps
               fill-fn generic-fill-prepared-statement}}]
  (if *log* (*log* sql))
  (if (string? sql)
    (fn [^Connection con]
      (with-open [^Statement stmt (.createStatement con)
                  ^ResultSet rs   (.executeQuery stmt sql)]
        (rs-fn rs)))
    (let [sql-str (first sql)
          data (rest sql)]
      (fn [^Connection con]
        (with-open [^PreparedStatement pstmt (.prepareStatement con sql-str)]
          (fill-fn pstmt data)
          (with-open [^ResultSet rs (.executeQuery pstmt)]
            (rs-fn rs)))))))
  
(defn query-first
  "Returns a fn that (when called with a Connection) executes the
   sql query and returns just the first row as a result.
   Examples:
   (query-first \"SELECT * FROM Customers WHERE Id=42\")
   (query-first [\"SELECT * FROM Customers WHERE Name=? AND Age=?\" \"Jan\" 37])"
  [sql & {:keys [rs-fn fill-fn] :or {rs-fn   rs->maps
                                     fill-fn generic-fill-prepared-statement}}]
  (comp first (query sql :rs-fn rs-fn :fill-fn fill-fn)))

(defn query-single-value
  "Returns a fn that (when called with a Connection) executes the
   sql query and returns a single value as a result.
   Examples:
   (query-single-value \"SELECT count(*) FROM Customers\")
   (query-single-value [\"SELECT count(*) FROM Customers WHERE Name=?\" 
                       \"Jan\"])"
  [sql & {:keys [fill-fn] :or {fill-fn generic-fill-prepared-statement}}]
  (comp ffirst #(let [f (query sql :rs-fn rs->vecs :fill-fn fill-fn)]
                  (f %))))

(defmacro dotx [con bindings & body]
  (let [tx (gensym "tx")]
    `(apply-to-tx ~con (fn [~tx]
                  (let [~@(map-indexed #(if (even? %1) %2 (cons %2 (list `~tx)))
                                       bindings)]
                    ~@body)))))

(defn execute [^Connection con f & args] 
  (try 
    ((apply f args) con)
    (finally (.close con))))

(defn sql-executer [con-fn]
  (fn [f & args]
    (let [con (con-fn)]
      (try 
        ((apply f args) con)
        (finally (.close con))))))
