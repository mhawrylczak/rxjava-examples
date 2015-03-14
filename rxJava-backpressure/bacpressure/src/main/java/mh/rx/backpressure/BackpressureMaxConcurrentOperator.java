package mh.rx.backpressure;

import rx.Observable;
import rx.Subscriber;
import rx.functions.Func1;

import java.util.concurrent.atomic.LongAdder;


class BackpressureMaxConcurrentOperator<T, R> implements Observable.Operator<R, T> {
    private final Func1<T, Observable<R>> transformer;
    private final long maxConcurrent;

    public static <T, R> BackpressureMaxConcurrentOperator<T, R> create(Func1<T, Observable<R>> transformer, long maxConcurrent) {
        return new BackpressureMaxConcurrentOperator<>(transformer, maxConcurrent);
    }

    private BackpressureMaxConcurrentOperator(Func1<T, Observable<R>> transformer, long maxConcurrent) {
        this.transformer = transformer;
        this.maxConcurrent = maxConcurrent;
    }

    @Override
    public Subscriber<? super T> call(Subscriber<? super R> subscriber) {
        return new BackpressureMaxConcurrentSubscriber<>(subscriber, transformer, maxConcurrent);
    }


    private static class BackpressureMaxConcurrentSubscriber<T, R> extends Subscriber<T> {
        private final Func1<T, Observable<R>> transformer;
        private final long maxConcurrent;
        private final LongAdder trackedObservablesCount = new LongAdder();
        private volatile boolean completed = false;
        private final Subscriber<? super R> subscriber;

        public BackpressureMaxConcurrentSubscriber(Subscriber<? super R> subscriber, Func1<T, Observable<R>> transformer, long maxConcurrent) {
            this.subscriber = subscriber;
            this.transformer = transformer;
            this.maxConcurrent = maxConcurrent;
        }

        @Override
        public void onStart() {
            super.onStart();
            trackedObservablesCount.increment();
            request(maxConcurrent);
        }

        @Override
        public void onCompleted() {
            doOnCompleteTrackedObservable();
        }

        @Override
        public void onError(Throwable e) {
            if (!subscriber.isUnsubscribed()) {
                synchronized (subscriber) {
                    if(!completed) {
                        completed = true;
                        subscriber.onError(e);
                    }
                }
            }
        }

        @Override
        public void onNext(T t) {
            trackedObservablesCount.increment();
            transformer.call(t)
                    .doOnNext(next -> {
                        if (!completed) {
                            synchronized (subscriber) {
                                subscriber.onNext(next);
                            }
                        }
                    })
                    .doOnError(err -> {
                        synchronized (subscriber) {
                            if (!completed) {
                                completed = true;
                                subscriber.onError(err);
                            }
                        }
                    })
                    .doOnCompleted(() -> {
                        if (!doOnCompleteTrackedObservable()) {
                            request(1);
                        }
                    }).subscribe();
        }

        private boolean doOnCompleteTrackedObservable() {
            trackedObservablesCount.decrement();
            if (!subscriber.isUnsubscribed()) {
                if (trackedObservablesCount.longValue() == 0) {
                    synchronized (subscriber) {
                        if (!completed) {
                            completed = true;
                            subscriber.onCompleted();
                        }
                    }
                    return true;
                }
            }
            return false;
        }
    }
}
