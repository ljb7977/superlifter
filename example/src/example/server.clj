(ns example.server
  (:require [io.pedestal.http :as server]
            [com.walmartlabs.lacinia.resolve :as resolve]
            [com.walmartlabs.lacinia.pedestal :as lacinia]
            [com.walmartlabs.lacinia.schema :as schema]
            [superlifter.lacinia :refer [inject-superlifter with-superlifter]]
            [superlifter.api :as s]
            [promesa.core :as prom]
            [urania.core :as u]
            [clojure.tools.logging :as log]))

(def pet-db (atom {"abc-123" {:name "Lyra"
                              :age 11}
                   "def-234" {:name "Pantalaimon"
                              :age 11}
                   "ghi-345" {:name "Iorek"
                              :age 41}}))

;; def-fetcher - a convenience macro like defrecord for things which cannot be combined
(s/def-fetcher FetchPets []
  (fn [_this env]
    (map (fn [id] {:id id}) (keys (:db env)))))

;; def-superfetcher - a convenience macro like defrecord for combinable things
(s/def-superfetcher FetchPet [id]
  (fn [many env]
    (log/info "Combining request for" (count many) "pets" (map :id many))
    (map (:db env) (map :id many))))

(defn- resolve-pets [context _args _parent]
  (with-superlifter context
    (-> (s/enqueue! (->FetchPets))
        (s/update-trigger! :pet-details :elastic
                           (fn [trigger-opts pet-ids]
                             (update trigger-opts :threshold + (count pet-ids)))))))

(defn- resolve-pet [context args _parent]
  (with-superlifter context
    (-> (prom/promise {:id (:id args)})
        (s/update-trigger! :pet-details :elastic
                           (fn [trigger-opts _pet-ids]
                             (update trigger-opts :threshold inc)))
        (prom/then (fn [result]
                     (resolve/with-context result {::pet-id (:id args)}))))))

(defn- resolve-pet-details [context _args {:keys [id]}]
  (with-superlifter context
    (s/enqueue! :pet-details (->FetchPet id))))


(defn fetch-pet-details [db ids]
  [])


(defprotocol DataLoad
  (load [this context]))
(defmacro def-superfetcher-v2 [sym bucket-id bulk-fetcher]
  (let [do-fetch-fn (fn [many {:keys [db]}]
                      (let [ids (map :id many)
                            data (->> (bulk-fetcher db ids)
                                      (group-by :id))]
                        (map data ids)))]
    `(defrecord ~sym [id]  ;; 항상 argument는 id여야 함.
       u/DataSource
       (-identity [this#] (:id this#))
       (-fetch [this# env#]
         (unwrap first (~do-fetch-fn [this#] env#)))

       u/BatchedSource
       (-fetch-multi [muse# muses# env#]
         (let [muses# (cons muse# muses#)]
           (unwrap (fn [responses#]
                     (zipmap (map u/-identity muses#)
                             responses#))
                   (~do-fetch-fn muses# env#))))

       DataLoad
       (load [this# context#]
         (with-superlifter context#
           (s/enqueue! ~bucket-id this#))))))


;; Dataloader 선언과 비슷하게! bulk로 가져와서 id별로 다시 분배하는 로직은 최대한 밖에 안 보이게 하자
(def-superfetcher-v2 SomeSuperFetcher :pet-details fetch-pet-details)
;; 만약 아래와 같은 형태로 쓸 수 있다면 어떨까?
(defn resolve-pet-details-2
  [{:keys [db] :as ctx} arg {:keys [id]}]
  (-> (->SomeSuperFetcher id)
      (load ctx);; 이러면 해당 petDetail 하나만 딱 잘 나와야 한다
      (p/then ...)))

:=>
(defn resolve-pet-details-expanded
  [context args {:keys [id]}]
  (with-superlifter context
    (s/enqueue! :pet-details (->FetchPet id))))

(def schema
  {:objects {:PetDetails {:fields {:name {:type 'String}
                                   :age {:type 'Int}}}
             :Pet {:fields {:id {:type 'String}
                            :details {:type :PetDetails
                                      :resolve resolve-pet-details}}}}
   :queries {:pets
             {:type '(list :Pet)
              :resolve resolve-pets}
             :pet
             {:type :Pet
              :resolve resolve-pet
              :args {:id {:type 'String}}}}})

(def lacinia-opts {:graphiql true})

(def superlifter-args
  {:buckets {:default {:triggers {:queue-size {:threshold 1}}}
             :pet-details {:triggers {:elastic {:threshold 0}}}}
   :urania-opts {:env {:db @pet-db}}})

(def service
  (lacinia/service-map
   (fn [] (schema/compile schema))
   (assoc lacinia-opts
          :interceptors (into [(inject-superlifter superlifter-args)]
                              (lacinia/default-interceptors (fn [] (schema/compile schema)) lacinia-opts)))))

;; This is an adapted service map, that can be started and stopped
;; From the REPL you can call server/start and server/stop on this service
(defonce runnable-service (server/create-server service))

(defn -main
  "The entry-point for 'lein run'"
  [& _args]
  (log/info "\nCreating your server...")
  (server/start runnable-service))

(comment
  (do (server/stop s)
      (def runnable-service (server/create-server service))
      (def s (server/start runnable-service))))
