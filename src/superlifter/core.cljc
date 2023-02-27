(ns superlifter.core
  (:require [urania.core :as u]
            [promesa.core :as prom]
            [medley.core :as medley]
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

(defn- update-bucket! [context bucket-id f]
  (let [bucket-id (if (contains? @(:buckets context) bucket-id)
                    bucket-id
                    (do (log :warn "Bucket" bucket-id "does not exist, using default bucket")
                        default-bucket-id))
        new (-> (swap! (:buckets context)
                       (fn [buckets]
                         (update buckets bucket-id (comp f clear-ready))))
                (get bucket-id))]
    (if-let [muses-and-promises (not-empty (get-in new [:queue :ready]))]
      (let [cache (get-in new [:urania-opts :cache])

            muses    (map :muse    muses-and-promises)
            promises (map :promise muses-and-promises)]
        (log :info "Fetching" (count muses-and-promises) "muses from bucket" bucket-id)
        (-> (u/execute! (u/collect muses)
                        (merge (:urania-opts new)
                               (when cache
                                 {:cache (->urania cache)})))
            (prom/then
             (fn [[result new-cache-value]]
               (when cache
                 (urania-> cache new-cache-value))
               (doall (map prom/resolve! promises result))))
               ;(run! (fn [[p result]] (prom/resolve! p result))
               ;      (zipmap promises result))))
            (prom/catch
             (fn [ex]
               (doall (map prom/reject! promises (repeat ex)))))))
               ;(run! (fn [p] (prom/reject! p ex))
               ;      promises)))))
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
   (let [promise (prom/deferred)]
     (log :debug "Enqueuing muse into" bucket-id (:id muse))
     (update-bucket! context
                     bucket-id
                     (fn [bucket]
                       (reduce (fn [b trigger-fn]
                                 (trigger-fn b))
                               (update-in bucket [:queue :waiting] conj {:muse    muse
                                                                         :promise promise})
                               (keep :enqueue-fn (vals (:triggers bucket))))))
     promise)))

(defn- fetch-all-handling-errors! [context bucket-id]
  (try (prom/catch (fetch-bucket! context bucket-id)
                   (fn [error]
                     (log :warn "Fetch failed" error)))
       (catch Throwable t
         (log :warn "Fetch failed" t))))

(defmulti start-trigger! (fn [kind _bucket-id _opts] kind))

(defmethod start-trigger! :queue-size [_ _bucket-id {:keys [threshold] :as opts}]
           (assoc opts :enqueue-fn (fn [{:keys [queue] :as bucket}]
                                     (if (= threshold (count (:waiting queue)))
                                       (-> bucket
                                           (assoc-in [:queue :ready] (take threshold (:waiting queue)))
                                           (update-in [:queue :waiting] #(drop threshold %)))
                                       bucket))))

(defmethod start-trigger! :elastic [kind _bucket-id opts]
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

(defmethod start-trigger! :interval [_ bucket-id opts]
  (let [start-fn #?(:clj (fn [context]
                           (let [watcher (future (loop []
                                                   (Thread/sleep (:interval opts))
                                                   (fetch-all-handling-errors! context bucket-id)
                                                   (recur)))]
                             ;; return a function to stop the watcher
                             #(future-cancel watcher)))
                    :cljs (fn [context]
                            (let [watcher
                                  (js/setInterval #(fetch-all-handling-errors! context bucket-id)
                                                  (:interval opts))]
                              #(js/clearInterval watcher))))]
    (-> opts
        (assoc :start-fn start-fn))))

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

(defmethod start-trigger! :debounced [_ bucket-id opts]
  (let [interval (:interval opts)
        last-updated (atom nil)
        start-fn #?(:clj (fn [context]
                           (let [watcher (future (loop []
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
                                                           (recur))))))]

                             ;; return a function to stop the watcher
                             (fn []
                               (future-cancel watcher)
                               (reset! last-updated :exit))))
                    :cljs (fn [context]
                            (let [watcher (js/setTimeout check-debounced 0 context bucket-id interval last-updated)]
                              (fn []
                                  (js/clearInterval watcher)
                                  (reset! last-updated :exit)))))]
    (assoc opts
           :enqueue-fn (fn [bucket]
                         (reset! last-updated #?(:clj (System/currentTimeMillis)
                                                 :cljs (js/Date.)))
                         bucket)
           :start-fn start-fn)))

(defmethod start-trigger! :default [_trigger-kind _bucket-id opts]
  opts)

(defn- start-triggers! [bucket-id bucket-opts]
  (update bucket-opts :triggers
          (fn [triggers]
            (log :debug "Starting" (count triggers) "triggers for bucket" bucket-id)
            (->> triggers
                 (medley/map-kv-vals (fn [trigger-kind trigger-opts]
                                       (log :debug "Starting trigger" trigger-kind "for bucket" bucket-id trigger-opts)
                                       (start-trigger! trigger-kind bucket-id trigger-opts)))))))

(defn- start-bucket! [bucket-id bucket-opts urania-opts]
  (log :debug "Starting bucket" bucket-id)
  (let [bucket-opts (-> bucket-opts
                        (assoc :queue {:ready [] :waiting []}
                               :id bucket-id)
                        (update :urania-opts #(merge urania-opts %)))]
    (start-triggers! bucket-id bucket-opts)))

(defn- start-buckets! [{:keys [buckets urania-opts] :as context}]
  (swap! buckets
         (fn [buckets]
           (medley/map-kv-vals (fn [bucket-id bucket-opts]
                                 (start-bucket! bucket-id bucket-opts urania-opts))
                               buckets)))
  context)

(defn- stop-bucket! [context bucket-id]
  (doseq [stop-fn (vals (get-in context [:stop-fns bucket-id]))]
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

(defn start-trigger-watchers!
  "Calls start-fn for each trigger if exists, traversing all triggers in the buckets atom.
  Returns new context with :stop-fns associated, which is a map storing functions to stop watcher threads of those triggers."
  [context]
  (let [stop-fns (->> (for [[bucket-id bucket] @(:buckets context)]
                        [bucket-id (->> (for [[trigger-kind trigger] (:triggers bucket)
                                              :when (#{:interval :debounced} trigger-kind)]
                                          [trigger-kind ((:start-fn trigger) context)])  ;; [trigger-kind -> stop-fn]
                                        (into {}))]) ;; {trigger-kind -> stop-fn}
                      (into {}))]
    (assoc context :stop-fns stop-fns)))

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
      (start-buckets!)
      (start-trigger-watchers!)))

(defn stop!
  "Stops superlifter"
  [context]
  (run! (partial stop-bucket! context) (-> context :stop-fns keys))
  context)
