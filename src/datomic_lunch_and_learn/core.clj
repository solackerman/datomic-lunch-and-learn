(ns datomic-lunch-and-learn.core
  (:require [datomic.api :as d]
            ;; [datomic-q-explain.core :as dq]
            [clojure.pprint :refer :all]))


(d/create-database "datomic:free://localhost:4334/test")

(def db-conn (d/connect "datomic:free://localhost:4334/test"))

;; add a schema
(def schema  [{:db/id #db/id[:db.part/db]
               :db/ident :person/name
               :db/index true
               :db/valueType :db.type/string
               :db/cardinality :db.cardinality/one
               :db/doc "A person's name"
               :db.install/_attribute :db.part/db}

              {:db/id #db/id[:db.part/db]
               :db/ident :person/email
               :db/unique :db.unique/identity
               :db/valueType :db.type/string
               :db/cardinality :db.cardinality/one
               :db/doc "A person's email"
               :db.install/_attribute :db.part/db}

              {:db/id #db/id[:db.part/db]
               :db/ident :person/manages
               :db/valueType :db.type/ref
               :db/cardinality :db.cardinality/many
               :db/doc "Who a person manages"
               :db.install/_attribute :db.part/db}

              {:db/id #db/id[:db.part/db]
               :db/ident :person/title
               :db/index true
               :db/valueType :db.type/keyword
               :db/cardinality :db.cardinality/one
               :db/doc "A person's title"
               :db.install/_attribute :db.part/db}])

(d/transact db-conn schema)

(def sol (d/tempid :db.part/user))
(def andy (d/tempid :db.part/user))
(def paul (d/tempid :db.part/user))
(def bruce (d/tempid :db.part/user))
(def eamonn (d/tempid :db.part/user))

;; add sol, bruce, paul, and andy
(pprint @(d/transact-async db-conn
                           [[:db/add sol :person/name "sol"]
                            [:db/add sol :person/email "sol.ackerman@cenx.com"]
                            [:db/add sol :person/title :developer]

                            [:db/add bruce :person/name "bruce"]
                            [:db/add bruce :person/email "bruce.wessels@cenx.com"]
                            [:db/add bruce :person/title :team-lead]
                            [:db/add bruce :person/manages sol]

                            {:db/id paul
                             :person/name "paul"
                             :person/email "paul.mcrae@cenx.com"
                             :person/title :manager
                             :person/manages #{bruce sol andy}}

                            {:db/id andy
                             :person/name "andy"
                             :person/email "andy.brown@cenx.com"
                             :person/title :developer}]))

;; EAVT
;; AEVT
;; VAET
;; AVET

(def t1 (java.util.Date.))

(def db-t1 (d/db db-conn))

;; get :db/id for paul and andy
(def paul-id (d/entid db-t1 [:person/email "paul.mcrae@cenx.com"]))
(def andy-id (d/entid db-t1 [:person/email "andy.brown@cenx.com"]))

;; add eamonn, make andy a team lead
(d/transact db-conn [{:db/id eamonn
                      :person/name "eamonn"
                      :person/email "eamonn.garry@cenx.com"
                      :person/title :director
                      :person/manages paul-id}

                     [:db/retract andy-id :person/title :developer]
                     [:db/add andy-id :person/title :team-lead]])

;;; Datalog Queries

;; tell me everything about andy, and when we knew it
(d/q '{:find [?ai ?v ?tx ?d]
       :in [$ ?e]
       :where [[$ ?e ?a ?v ?t ?d]
               [$ ?a :db/ident ?ai]
               [$ ?t :db/txInstant ?tx]]}
     (d/db db-conn)
     andy-id)

;; who manages who?

(d/q '[:find ?m1 ?p1
       :in $
       :where
       [?m :person/manages ?p]
       [?m :person/name ?m1]
       [?p :person/name ?p1]]
     (d/db db-conn))

;; datomic rules

(def rules '[[[name-or-email ?e ?v]
              [?e :person/name ?v]]
             [[name-or-email ?e ?v]
              [?e :person/email ?v]]

             [[managers ?m ?p]
              [?m :person/manages ?p]]
             [[managers ?m ?p]
              [?m :person/manages ?x]
              [managers ?x ?p]]])

(d/q '[:find ?v
       :in $ % ?e
       :where
       [name-or-email ?e ?v]]
     (d/db db-conn) rules paul-id)

(d/q '[:find ?m1 ?p1
       :in $ %
       :where
       [managers ?m ?p]
       [?m :person/name ?m1]
       [?p :person/name ?p1]]
     (d/db db-conn) rules)

;; not, missing? (Newer than our version of datomic)

(d/q '[:find ?n
       :in $
       :where
       [?m :person/name ?n]
       [(missing? $ ?m :person/manages)]
       (not [?m :person/name "andy"])]
     (d/db db-conn))

;; Query join 2 'databases'

(d/q '{:find [?v ?ta ?tb]
       :in [$a $b]
       :where [[$a ?ea :person/name ?v]
               [$b ?eb :person/name ?v]
               [$a ?ea :person/title ?ta]
               [$b ?eb :person/title ?tb]]}
     (d/db db-conn)
     [[1 :person/name "sol"]
      [1 :person/title :the-duke-of-awesome]])

;; Query explain
(pprint (dq/q-explain
         '[:find ?m1 ?p1
           :in $ ?p1
           :where
           [?m :person/manages ?p]
           [?m :person/name ?m1]
           [?p :person/name ?p1]]
         (d/db db-conn) "sol"))

;; Entities

(->> [:person/email "sol.ackerman@cenx.com"]
     (d/entity (d/db db-conn))
     :person/_manages
     (map d/touch)
     pprint)

;; Pull

(d/pull (d/db db-conn)
        '[*]
        [:person/email "eamonn.garry@cenx.com"])

(d/pull (d/db db-conn)
        '[:person/email :person/name :person/title {:person/_manages ...}]
        [:person/email "sol.ackerman@cenx.com"])

(d/pull-many (d/db db-conn)
             '[:person/email :person/name :person/title]
             [[:person/email "sol.ackerman@cenx.com"]
              [:person/email "bruce.wessels@cenx.com"]])
