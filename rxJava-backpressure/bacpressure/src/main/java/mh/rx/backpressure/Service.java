package mh.rx.backpressure;


import rx.Observable;
import rx.Subscriber;
import rx.functions.Action0;
import rx.schedulers.Schedulers;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;


public class Service {
    private AtomicInteger concurrentInvocations = new AtomicInteger(0);
    private AtomicInteger maxInvocations = new AtomicInteger(0);

    public Observable<Integer> getIdentifiers(int count){
        return Observable.range(0, count);
    }

    public Observable<String> getTitle(final int id){
        return Observable.create(new Observable.OnSubscribe<String>() {
            @Override
            public void call(final Subscriber<? super String> subscriber) {
                subscriber.onStart();
                updateMax(concurrentInvocations.incrementAndGet());
                Schedulers.newThread().createWorker().schedule(new Action0() {
                    @Override
                    public void call() {
                        if (!subscriber.isUnsubscribed()){
                            subscriber.onNext(String.valueOf(id));
                            concurrentInvocations.decrementAndGet();
                            subscriber.onCompleted();
                        }else{
                            concurrentInvocations.decrementAndGet();
                        }

                    }
                }, 100, TimeUnit.MILLISECONDS);//
            }
        });
    }

    public Observable<String> getTitle2(final int id){
        return Observable.create(new Observable.OnSubscribe<String>() {
            @Override
            public void call(final Subscriber<? super String> subscriber) {
                subscriber.onStart();
                updateMax(concurrentInvocations.incrementAndGet());
                Schedulers.computation().createWorker().schedule(new Action0() {
                    @Override
                    public void call() {
                        if (!subscriber.isUnsubscribed()){
                            subscriber.onNext(String.valueOf(id));
                            concurrentInvocations.decrementAndGet();
                            subscriber.onCompleted();
                        }else{
                            concurrentInvocations.decrementAndGet();
                        }

                    }
                });//, 100, TimeUnit.MILLISECONDS
            }
        });
    }

    public int getMax(){
        return maxInvocations.get();
    }

    private void updateMax(int invocations) {
        for(int previousMax = maxInvocations.get();
            previousMax<invocations && !maxInvocations.compareAndSet(previousMax, invocations);
            previousMax = maxInvocations.get());
    }
}
