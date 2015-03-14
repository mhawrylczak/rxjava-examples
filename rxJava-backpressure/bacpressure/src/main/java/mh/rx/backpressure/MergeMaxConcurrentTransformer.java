package mh.rx.backpressure;

import rx.Observable;
import rx.functions.Func1;

/**
* Created by wladyslaw.hawrylczak on 2015-03-14.
*/
class MergeMaxConcurrentTransformer<T,R> implements Observable.Transformer<T, R> {
    public final Func1<T, Observable<R>> transformer;
    public final int maxConcurrent;

    MergeMaxConcurrentTransformer(Func1<T, Observable<R>> transformer, int maxConcurrent) {
        this.transformer = transformer;
        this.maxConcurrent = maxConcurrent;
    }

    @Override
    public Observable<R> call(Observable<T> integerObservable) {
        return Observable.merge(integerObservable.map(transformer), maxConcurrent);
    }
}
