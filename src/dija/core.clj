(ns dija.core
  (import (java.util.concurrent
           DelayQueue
           Delayed
           TimeUnit)))




(defprotocol ITimedEntry
  (time [this])
  (data [this]))

(defprotocol IBag
  (clear [this])
  (add! [this k v ops])
  (get* [this k])
  (remove* [this k]))



(deftype DelayedEntry [t d]
  ITimedEntry
  (time [this] t)
  (data [this] d)
  Delayed
  (getDelay [this time_unit]
    (remaining time_unit t))
  (compareTo [this delayed]
    (- t (time delayed))))


(deftype BagImpl [exe delay repo]
  IBag
  (add! [this k v {:keys [ttl listener] :or {ttl 10} :as ops}]
    (let [time' (+
                 (System/currentTimeMillis)
                 (* ttl 1000))]
      (.offer delay (DelayedEntry. time' k))
      (swap! repo assoc k v)))

  
  (get* [this k]
    (get @repo k))

  
  (remove* [this k]
    (let [itr-seq (iterator-seq (.iterator delay))]
      (doseq [d itr-seq]
        (when (= (data d) k)
          (.remove delay d)))
      (swap! repo dissoc k))))


(defn- remaining [unit time]
  (let [remaining_millis (- time (System/currentTimeMillis))]
    (.convert unit remaining_millis (TimeUnit/MILLISECONDS))))

(defn- consume [q a]
  (fn []
    (loop []
      (let [d (data (.take q))]
        (swap! a dissoc d)
        (recur)))))


(defn bag []
  (let [a (atom {})
        q (DelayQueue.)
        t (Thread. (consume q a))]
    (.start t)
    (BagImpl. nil q a)))
