(ns superlifter.core
  (:require [urania.core :as u]
            [promesa.core :as prom]
            [medley.core :refer [map-kv-vals]]
            #?(:clj [superlifter.logging :refer [log]]
               :cljs [superlifter.logging :refer-macros [log]]))
  (:refer-clojure :exclude [resolve]))

#?(:cljs (def Throwable js/Error))

(defprotocol Cache
  (->urania [this])
  (urania-> [this new-value]))

(extend-protocol Cache
  #?(:clj clojure.lang.Atom
     :cljs cljs.core/Atom)
  (->urania [this]
    (deref this))
  (urania-> [this new-value]
    (reset! this new-value)))

(def default-bucket-id :default)

(defn- clear-ready [bucket]
  (update bucket :queue assoc :ready []))

(defn- ready-all [bucket]
  (update bucket :queue (fn [queue]
                          (-> (assoc queue :waiting [])
                              (assoc :ready (:waiting queue))))))

(defn- update-bucket!
  "context: {:buckets (atom {:a {:trigger {:elastic ...
                                     :interval ...}}}}
  update bucket with bucket-id, applying f
  after updating bucket, if there are any ready muses, fetch them from data source"
  [context bucket-id f]
  (let [bucket-id  (if (contains? @(:buckets context) bucket-id)
                     bucket-id
                     (do (log :warn "Bucket" bucket-id "does not exist, using default bucket")
                         default-bucket-id))
        new-bucket (-> (:buckets context)
                       (swap! #(update % bucket-id (comp f clear-ready)))
                       (get bucket-id))]
    (if-let [muses (not-empty (get-in new-bucket [:queue :ready]))]  ;; if there are any ready muses, get that
      (let [cache (get-in new-bucket [:urania-opts :cache])]
        (log :info "Fetching" (count muses) "muses from bucket" bucket-id)
        (-> (u/execute! (u/collect muses)  ;; 여러 데이터 소스에서 가져올 데이터를 하나로 뭉침
                        (merge (:urania-opts new-bucket)
                               (when cache
                                 {:cache (->urania cache)})))
            (prom/then (fn [[result new-cache-value]]
                         (when cache
                           (urania-> cache new-cache-value))  ;; data caching
                         result)))) ;; urania가 fetch해온 데이터 리턴
      (do (log :debug "Nothing ready to fetch for" bucket-id)
          (prom/resolved nil)))))

(defn- fetch-bucket! [context bucket-id]
  (update-bucket! context bucket-id ready-all))

(defn fetch!
  "Performs a fetch of all muses in the queue"
  ([context] (fetch! context default-bucket-id))
  ([context bucket-id]
   (fetch-bucket! context bucket-id)))

(defn fetch-all! [context]
  (prom/then (prom/all (map (partial fetch! context) (keys @(:buckets context))))
             (fn [results]
               (reduce into [] results))))

(defn enqueue!
  "Enqueues a muse describing work to be done and returns a promise which will be delivered with the result of the work.
   The muses in the queue will all be fetched together when a trigger condition is met."
  ([context muse] (enqueue! context default-bucket-id muse))
  ([context bucket-id muse]
   (let [p (prom/deferred)
         delivering-muse (u/map (fn [result]
                                  (prom/resolve! p result)
                                  result)
                                muse)] ;; muse의 data fetching이 완료된 다음에 프로미스가 resolve되게 됨
     (log :debug "Enqueuing muse into" bucket-id (:id muse))
     (update-bucket! context
                     bucket-id
                     (fn [bucket]
                       (let [enqueue-fns (->> bucket :triggers vals (keep :enqueue-fn))  ;; get all enqueue-fns in the bucket
                             bucket' (update-in bucket [:queue :waiting] conj delivering-muse)] ;; bucket의 waiting list에 delivering-muse 추가
                         ;; 버킷에 모든 enqueue function을 적용한다
                         (reduce (fn [b trigger-fn] (trigger-fn b)) bucket' enqueue-fns))))
     p)))

(defn- fetch-all-handling-errors! [context bucket-id]
  (try (prom/catch (fetch-bucket! context bucket-id)
           (fn [error]
             (log :warn "Fetch failed" error)))
       (catch Throwable t
         (log :warn "Fetch failed" t))))

(defmulti start-trigger! (fn [kind _context _bucket-id _opts] kind))

(defmethod start-trigger! :queue-size [_ _context _bucket-id {:keys [threshold] :as opts}]
  (assoc opts :enqueue-fn (fn [{:keys [queue] :as bucket}]
                            (if (= threshold (count (:waiting queue)))
                              (-> bucket
                                  (assoc-in [:queue :ready] (take threshold (:waiting queue)))
                                  (update-in [:queue :waiting] #(drop threshold %)))
                              bucket))))

(defmethod start-trigger! :elastic [kind  _context _bucket-id opts]
  (assoc opts
         :enqueue-fn (fn [{:keys [queue] :as bucket}]
                       (let [threshold (get-in bucket [:triggers kind :threshold] 0)]
                         (if (<= threshold (count (:waiting queue)))
                           (-> bucket
                               (assoc-in [:queue :ready] (:waiting queue))
                               (assoc-in [:queue :waiting] [])
                               (assoc-in [:triggers kind :threshold] 0))
                           bucket)))))

(defn update-trigger! [context bucket-id trigger-kind opts-fn]
  (update-bucket! context bucket-id (fn [bucket]
                                      (update-in bucket [:triggers trigger-kind] opts-fn))))

(defmethod start-trigger! :interval [_ context bucket-id opts]
  (let [watcher #?(:clj (future (loop []
                                  (Thread/sleep (:interval opts))
                                  (fetch-all-handling-errors! context bucket-id)
                                  (recur)))
                   :cljs (js/setInterval #(fetch-all-handling-errors! context bucket-id)
                                         (:interval opts)))]
    (assoc opts :stop-fn #?(:clj #(future-cancel watcher)
                            :cljs #(js/clearInterval watcher)))))

#?(:cljs
   (defn- check-debounced [context bucket-id interval last-updated]
     (let [lu @last-updated]
       (cond
         (nil? lu) (js/setTimeout check-debounced interval context bucket-id interval last-updated)

         (= :exit lu) nil

         (<= interval (- (js/Date.) lu))
         (do (fetch-all-handling-errors! context bucket-id)
             (compare-and-set! last-updated lu nil)
             (js/setTimeout check-debounced 0 context bucket-id interval last-updated))

         :else
         (js/setTimeout check-debounced (- interval (- (js/Date.) lu)) context bucket-id interval last-updated)))))

(defmethod start-trigger! :debounced [_ context bucket-id opts]
  (let [interval (:interval opts)
        last-updated (atom nil)
        watcher #?(:clj (future (loop []
                                  (let [lu @last-updated]
                                    (cond
                                      (nil? lu) (do (Thread/sleep interval)
                                                    (recur))

                                      (= :exit lu) nil

                                      (<= interval (- (System/currentTimeMillis) lu))
                                      (do (fetch-all-handling-errors! context bucket-id)
                                          (compare-and-set! last-updated lu nil)
                                          (recur))

                                      :else
                                      (do (Thread/sleep (- interval (- (System/currentTimeMillis) lu)))
                                          (recur))))))
                   :cljs (js/setTimeout check-debounced 0 context bucket-id interval last-updated))]
    (assoc opts
           :enqueue-fn (fn [bucket]
                         (reset! last-updated #?(:clj (System/currentTimeMillis)
                                                 :cljs (js/Date.)))
                         bucket)
           :stop-fn #(do #?(:clj (future-cancel watcher)
                            :cljs (js/clearInterval watcher))
                         (reset! last-updated :exit)))))

(defmethod start-trigger! :default [_context _bucket-id _trigger-kind opts]
  opts)

(defn start-triggers!
  "map of triggers -> new map of started triggers
  triggers: {:elastic {:threshold 0}
             :interval {:interval 100}}

  Output: {:elastic {:threshold 0
                     :enqueue-fn <some-function>}
           :interval {:interval 100
                      :stop-fn <some-function>}}
  "
  [triggers context bucket-id]
  (log :debug "Starting" (count triggers) "triggers for bucket" bucket-id)
  (map-kv-vals (fn [trigger-kind trigger-opts]
                 (log :debug "Starting trigger" trigger-kind "for bucket" bucket-id trigger-opts)
                 (start-trigger! trigger-kind context bucket-id trigger-opts))
               triggers))

(defn- start-bucket! [{:keys [urania-opts] :as context} bucket-id opts]
  (log :debug "Starting bucket" bucket-id)
  (-> opts
      (assoc :queue {:ready [] :waiting []} :id bucket-id)
      (update :urania-opts #(merge urania-opts %))
      (update :triggers start-triggers! context bucket-id)))

(defn- start-buckets! [{:keys [buckets] :as context}]
  (swap! buckets #(map-kv-vals (fn [bucket-id opts] (start-bucket! context bucket-id opts)) %))
  context)

(defn- stop-bucket! [context bucket-id]
  (doseq [{:keys [stop-fn]} (vals (:triggers (get @(:buckets context) bucket-id)))
          :when stop-fn]
    (stop-fn)))

(defn add-bucket! [context bucket-id opts]
  (log :debug "Adding bucket" bucket-id opts)
  (swap-vals! (:buckets context)
              (fn [buckets]
                (if (contains? buckets bucket-id)
                  (do (log :warn "Bucket" bucket-id "already exists")
                      buckets)
                  (assoc buckets bucket-id (start-bucket! context bucket-id opts)))))
  context)

(defn default-opts []
  {:urania-opts {:cache (atom {})}})

(defn start!
  "Starts a superlifter with the supplied options, which can contain:

  :buckets {:default bucket-opts
            :my-bucket bucket-opts
            ...}

  :urania-opts The options map supplied to urania for running muses.
               Contains :env, :cache and :executor
               :cache must implement the Cache protocol
               See urania documentation for details

  The `:default` bucket is used for all activity not associated with a named bucket.

  Bucket options can contain the following:

  :triggers    Conditions to perform a fetch of all muses in the queue.
               Triggers is a map of trigger-kind to trigger, looking like:

               {:queue-size {:threshold 10}
                :interval   {:interval 100}

               The fetch will be performed whenever any single trigger condition is met.

               Triggers can be of several types:

               Queue size trigger, which performs the fetch when the queue reaches n items
               {:queue-size {:threshold n}}

               Interval trigger, which performs the fetch every n milliseconds
               {:interval {:interval n}}

               If no triggers are supplied, superlifter runs in 'manual' mode and fetches will only be performed when you call `fetch!`

               You can supply your own trigger definition by participating in the `start-trigger!` multimethod.

  :urania-opts Override the top-level urania-opts at the bucket level


  Returns a context which can be used to stop superlifter, enqueue muses and trigger fetches.
  "
  [opts]
  (-> (merge (default-opts) opts)
      (update-in [:buckets default-bucket-id] #(or % {}))
      (update :buckets atom)
      (start-buckets!)))

(defn stop!
  "Stops superlifter"
  [context]
  (run! (partial stop-bucket! context) (keys @(:buckets context)))
  context)
