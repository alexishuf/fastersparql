package com.github.alexishuf.fastersparql.client.util.reactive;

import com.github.alexishuf.fastersparql.client.util.async.Async;
import com.github.alexishuf.fastersparql.client.util.async.AsyncTask;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.github.alexishuf.fastersparql.client.util.async.Async.asyncThrowing;
import static org.junit.jupiter.api.Assertions.*;

class ReactiveEventLoopPoolTest {
    private void doTest() throws ExecutionException {
        int threads = 2 * Runtime.getRuntime().availableProcessors() + 4;
        int length = 32768;
        List<List<Integer>> lists = new ArrayList<>();
        List<AsyncTask<?>> tasks = new ArrayList<>();
        try (BoundedEventLoopPool pool = new BoundedEventLoopPool("test", threads)) {
            for (int taskNumber = 0; taskNumber < threads * 4; taskNumber++) {
                ArrayList<Integer> destination = new ArrayList<>(length);
                lists.add(destination);
                tasks.add(Async.async(() -> {
                    BoundedEventLoopPool.LoopExecutor loopExecutor = pool.chooseExecutor();
                    for (int i = 0; i < length; i++) {
                        final int number = i;
                        loopExecutor.execute(() -> destination.add(number));
                    }
                }));
            }
            for (AsyncTask<?> task : tasks) task.get();
        }
        List<Integer> expected = IntStream.range(0, length).boxed().collect(Collectors.toList());
        for (int i = 0; i < lists.size(); i++)
            assertEquals(expected, lists.get(i), "i="+i);
    }

    private final AsyncTask<?> testTask = asyncThrowing(this::doTest);
    @Test
    public void test() {
        testTask.fetch();
    }

    @SuppressWarnings("SameParameterValue") private static void sleep(int ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            fail(e);
        }
    }

    private void doTestOneWorkerBlocked() throws ExecutionException, InterruptedException {
        try (BoundedEventLoopPool pool = new BoundedEventLoopPool("test", 2)) {
            BoundedEventLoopPool.LoopExecutor s1 = pool.chooseExecutor();
            BoundedEventLoopPool.LoopExecutor s2 = pool.chooseExecutor();
            Semaphore warmup = new Semaphore(0);
            s1.execute(warmup::release);
            s2.execute(warmup::release);
            warmup.acquireUninterruptibly(2);

            s1.execute(() -> sleep(100));
            CompletableFuture<Integer> future = new CompletableFuture<>();
            s2.execute(() -> future.complete(23));
            long start = System.nanoTime();
            assertEquals(23, future.get());
            double elapsedMs = (System.nanoTime()-start)/1000000.0;
            assertTrue(elapsedMs < 100, "elapsedMs="+elapsedMs);
        }
    }

    private final AsyncTask<?> testOneWorkerBlockedTask = asyncThrowing(this::doTestOneWorkerBlocked);
    @Test
    void testOneWorkerBlocked() {
        testOneWorkerBlockedTask.fetch();
    }
}