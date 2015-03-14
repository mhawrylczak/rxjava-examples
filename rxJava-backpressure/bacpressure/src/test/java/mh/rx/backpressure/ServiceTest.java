package mh.rx.backpressure;

import org.junit.Before;
import org.junit.Test;
import rx.Observable;
import rx.Subscriber;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.LongAdder;

import static org.junit.Assert.*;

public class ServiceTest {

    private Service service;
    @Before
    public void setUp() throws Exception {
        service = new Service();
    }

    @Test
    public void simpleFlatMap(){
        List<String> result = service.getIdentifiers(100)
                .flatMap( id -> service.getTitle(id) )
                .toList().toBlocking().single();

        assertEquals(result.size(), 100);
        assertEquals(service.getMax(), 100);
    }

    @Test
    public void simpleFlatMapLimit(){
        List<String> result = service.getIdentifiers(100)
                .flatMap(id->service.getTitle(id),4)
                .toList().toBlocking().single();

        assertEquals(100, result.size());
        assertEquals(4, service.getMax());
    }

    @Test
    public void backpressureCompose(){
            List<String> result = service
                    .getIdentifiers(100)
                    .compose(BackpressureMaxConcurrentTransformer.create(id -> service.getTitle(id), 4))
                    .serialize()
                    .toList().toBlocking().single();

            assertEquals(100, result.size());
            assertEquals(4, service.getMax());
    }

    @Test
    public void backpressureLift(){
        List<String> result = service
                .getIdentifiers(100)
                .lift(BackpressureMaxConcurrentOperator.create(id -> service.getTitle(id), 4))
                .serialize()
                .toList().toBlocking().single();

        assertEquals(100, result.size());
        assertEquals(4, service.getMax());
    }

    @Test
    public void backpressureComposeSem(){
        for(int i = 0; i < 10; i++) {
            List<String> result = service
                    .getIdentifiers(100)
                    .compose(BackpressureMaxConcurrentTransformerSemaphore.create(id -> service.getTitle(id), 4))
                    .serialize()
                    .toList().toBlocking().single();

            assertEquals(100, result.size());
            assertEquals(4, service.getMax());
        }
    }

    @Test
    public void backpressurePerf(){
        long millis = System.currentTimeMillis();
        for(int i = 0; i < 10000 ; i++) {
            List<String> result = service
                    .getIdentifiers(100)
                    .compose(BackpressureMaxConcurrentTransformer.create(id -> service.getTitle2(id), 4))
                    .serialize()
                    .toList().toBlocking().single();
        }
        System.out.println("perf synchronized (System.currentTimeMillis() - millis) = " + (System.currentTimeMillis() - millis));
    }

    @Test
    public void backpressurePerfSemaphore(){
        long millis = System.currentTimeMillis();
        for(int i = 0; i < 10000 ; i++) {
            List<String> result = service
                    .getIdentifiers(100)
                    .compose(BackpressureMaxConcurrentTransformerSemaphore.create(id -> service.getTitle2(id), 4))
                    .serialize()
                    .toList().toBlocking().single();
        }
        System.out.println("perf semaphore (System.currentTimeMillis() - millis) = " + (System.currentTimeMillis() - millis));
    }


    @Test
    public void backpressureCompose2() throws InterruptedException {
        CountDownLatch completed = new CountDownLatch(1);
        LongAdder count = new LongAdder();
        LongAdder starts = new LongAdder();
        service
                .getIdentifiers(100)
                .onBackpressureBuffer()
                .compose(BackpressureMaxConcurrentTransformer.create(id -> service.getTitle(id), 4))
                .subscribe(new Subscriber<String>() {
                    @Override
                    public void onStart() {
                        super.onStart();
                        starts.increment();
                    }

                    @Override
                    public void onCompleted() {
                        completed.countDown();
                    }

                    @Override
                    public void onError(Throwable e) {
                        completed.countDown();
                    }

                    @Override
                    public void onNext(String s) {
                        count.increment();
                    }
                });

        completed.await();

        assertEquals(1, starts.intValue());
        assertEquals(100, count.intValue());
        assertEquals(4, service.getMax());
    }

    @Test
    public void backpressureCompose3() throws InterruptedException {
        CountDownLatch completed = new CountDownLatch(1);
        final List<String>[] result = new List[1];
        final int[] size = new int[1];
        service
                .getIdentifiers(100)
                .onBackpressureBuffer()
                .compose(BackpressureMaxConcurrentTransformer.create(id -> service.getTitle(id), 4))
                .toList()
                .subscribe(s -> {
                    result[0] = s;
                    size[0] = s.size();
                }, throwable -> {
                    completed.countDown();
                }, () -> {
                    completed.countDown();
                });

        completed.await();

        assertEquals(100, size[0]);
        assertEquals(4, service.getMax());
    }

    @Test
    public void backpressureCompose4() throws InterruptedException {
        List<String> result = service
                .getIdentifiers(100)
                .compose(BackpressureMaxConcurrentTransformer.create(id -> service.getTitle(id), 4))
                .toList().toBlocking().first();

        assertEquals(100, result.size());
        assertEquals(4, service.getMax());

    }

    @Test
    public void mergeLimit(){
        List<String> result = Observable.merge(service.getIdentifiers(100).map(id-> service.getTitle(id)), 4 ).toList().toBlocking().single();

        assertEquals(100, result.size());
        assertEquals(4, service.getMax());
    }

    @Test
    public void mergeLimitOperator(){
        List<String> result = service.getIdentifiers(100)
                .compose(new MergeMaxConcurrentTransformer<Integer, String>(id -> service.getTitle(id), 4))
                .toList()
                .toBlocking().single();

        assertEquals(100, result.size());
        assertEquals(4, service.getMax());
    }

    @Test
    public void simpleConcatMap(){
        List<String> result = service.getIdentifiers(100)
                .concatMap(id->service.getTitle(id)).toList().toBlocking().single();

        assertEquals(100, result.size());
        assertEquals(1,service.getMax());
    }

    @Test
    public void concatWindow(){
        List<String> result = service.getIdentifiers(100)
                .window(4)
                .concatMap(integerObservable -> integerObservable.flatMap(id -> service.getTitle(id)))
                .toList().toBlocking().single();

        assertEquals(100, result.size());
        assertEquals(4,service.getMax());
    }

}