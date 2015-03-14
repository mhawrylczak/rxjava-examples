package mh.rx.backpressure;

import rx.Observable;
import rx.Subscriber;
import rx.functions.Func1;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.LongAdder;


class BackpressureMaxConcurrentTransformerSemaphore<T, R> implements Observable.Transformer<T, R> {
    private final Func1<T, Observable<R>> transformer;
    private final long maxConcurrent;

    public static <T, R> BackpressureMaxConcurrentTransformerSemaphore<T, R> create(Func1<T, Observable<R>> transformer, long maxConcurrent) {
        return new BackpressureMaxConcurrentTransformerSemaphore<>(transformer, maxConcurrent);
    }

    private BackpressureMaxConcurrentTransformerSemaphore(Func1<T, Observable<R>> transformer, long maxConcurrent) {
        this.transformer = transformer;
        this.maxConcurrent = maxConcurrent;
    }

    @Override
    public Observable<R> call(Observable<T> sourceObservable) {
        return Observable.create((Subscriber<? super R> child) -> {
            sourceObservable.subscribe(new BackpressureMaxConcurrentSubscriber(child, transformer, maxConcurrent));
        });
    }


    private static class BackpressureMaxConcurrentSubscriber<T, R> extends Subscriber<T> {
        private final Func1<T, Observable<R>> transformer;
        private final long maxConcurrent;
        private final LongAdder trackedObservablesCount = new LongAdder();
        private volatile boolean completed = false;
        private final Subscriber<? super R> subscriber;
        private final Semaphore semaphore = new Semaphore(1);

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
                try {
                    semaphore.acquireUninterruptibly();
                    if (!completed) {
                        completed = true;
                        subscriber.onError(e);
                    }
                }finally {
                    semaphore.release();
                }
            }
        }

        @Override
        public void onNext(T t) {
            trackedObservablesCount.increment();
            transformer.call(t)
                    .doOnNext(next -> {
                        if (!completed) {
                            try {
                                semaphore.acquireUninterruptibly();
                                subscriber.onNext(next);
                            }finally {
                                semaphore.release();
                            }
                        }
                    })
                    .doOnError(err -> {
                        try {
                            semaphore.acquireUninterruptibly();
                            if (!completed) {
                                completed = true;
                                subscriber.onError(err);
                            }
                        }finally {
                            semaphore.release();
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
                    try {
                        semaphore.acquireUninterruptibly();
                        if (!completed) {
                            completed = true;
                            subscriber.onCompleted();
                        }
                    }finally {
                        semaphore.release();
                    }
                    return true;
                }
            }
            return false;
        }
    }
}
