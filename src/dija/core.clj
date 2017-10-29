(ns dija.core
  (import (java.util.concurrent
           DelayQueue
           Delayed
           TimeUnit)))

(def repo (atom {}))
(def delay (DelayQueue.))

(defprotocol TimedEntry
  (time [this])
  (data [this]))




(defn- remaining [unit time]
  (let [remaining_millis (- time (System/currentTimeMillis))]
    (.convert unit remaining_millis (TimeUnit/MILLISECONDS))))


(deftype DelayedEntry [t d]
  TimedEntry
  (time [this] t)
  (data [this] d)
  Delayed
  (getDelay [this time_unit]
    (remaining time_unit t))
  (compareTo [this delayed]
    (- t (time delayed))))



(defn add! [k v & {:keys [ttl callback] :or {ttl 10} :as ops}]
  (let [time' (+
               (System/currentTimeMillis)
               (* ttl 1000))]
    (.offer delay (DelayedEntry. time' k))
    (swap! repo assoc k v)))


(defn remove* [k]
  (let [itr-seq (iterator-seq (.iterator delay))]
    (doseq [d itr-seq]
      (when (= (data d) k)
        (.remove delay d)))
    (swap! repo dissoc k)))   

  
(defn get* [k]
  (get @repo k))

(defn- consume []
  (loop []
    (let [d (data (.take delay))]
      (swap! repo dissoc d)
      (recur))))

(def consumer (Thread. consume))
(.start consumer)
