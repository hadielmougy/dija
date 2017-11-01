(ns dija.core
  (:import (java.util.concurrent
           DelayQueue
           Delayed
           TimeUnit)))



(defprotocol ITimedEntry
  (time* [this])
  (data* [this]))

(defn- consumer [q a]
  (fn []
    (loop []
      (let [d (data* (.take q))]
        (swap! a dissoc d)
        (recur)))))


(defprotocol IBag
  (clear [this])
  (add! [this k v ops])
  (get* [this k])
  (remove* [this k]))

(defn-  remaining [unit time]
  (let [remaining_millis (- time (System/currentTimeMillis))]
    (.convert unit remaining_millis (TimeUnit/MILLISECONDS))))



(deftype DelayedEntry [t d]
  ITimedEntry
  (time* [this] t)
  (data* [this] d)
  Delayed
  (getDelay [this time_unit]
    (remaining time_unit t))
  (compareTo [this delayed]
    (- t (time* delayed))))



(defn- start-if-needed [started? delay-queue repo]
  ; race condition could happen here. lock could be used here
  (if (not @started?)
    (let [t (Thread. (consumer delay-queue repo))]
      (.start t)
      (swap! started? not))))


(deftype BagImpl [started? delay repo]
  IBag
  (add! [this k v {:keys [ttl listener] :or {ttl 10} :as ops}]
    (start-if-needed started? delay repo)
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
        (when (= (data* d) k)
          (.remove delay d)))
      (swap! repo dissoc k)))


  (clear [this]
    (reset! repo {})))



(defn bag []
  (let [a (atom {})
        q (DelayQueue.)
        started? (atom false)]
    (BagImpl. started? q a)))
