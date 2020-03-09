(defproject clj-safejdbc "0.2.10"
  :description "A fast library for accessing JDBC in Clojure"
  :url "https://github.com/phoenixreisen/clj-safejdbc"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.7.0"]]
  :plugins [[s3-wagon-private "1.2.0"]]
  :profiles 
  {:dev   
   {:dependencies [[com.h2database/h2 "1.4.197"]]}}

  :repositories [["private" {:url "s3p://phoenixmvnrepo/release/" 
                             :sign-releases false
                             :passphrase :env 
                             :username :env}]])
