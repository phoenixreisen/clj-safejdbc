# Change Log
All notable changes to this project will be documented in this file. This change log follows the conventions of [keepachangelog.com](http://keepachangelog.com/).

## 0.2.12  - 2022-05-30
Query Connection Timeout
Die folgenden FNs sollen um den optionalen Parameter `query-timeout`
erweitert werden (dieser Wert wird dann im weiteren Verlauf auf dem
zugrunde liegenden Statement gesetzt):

    - update!
    - insert!
    - call!
    - call
    - query
    - query-first
    - query-single-value
