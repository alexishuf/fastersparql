package com.github.alexishuf.fastersparql.client.util;

import com.github.alexishuf.fastersparql.client.util.async.Async;
import com.github.alexishuf.fastersparql.client.util.async.AsyncTask;
import com.github.alexishuf.fastersparql.client.util.async.SafeAsyncTask;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.jupiter.api.Assertions.*;

class AsyncTest {
    private static final int N_PROCESSORS = Runtime.getRuntime().availableProcessors();

    @Test
    void testSchedule() throws ExecutionException, InterruptedException {
        long start = System.nanoTime();
        List<Future<Long>> futures = IntStream.range(0,  N_PROCESSORS)
                .mapToObj(i -> Async.schedule(200, MILLISECONDS, System::nanoTime))
                .collect(Collectors.toList());
        for (Future<Long> f : futures) {
            long elapsedMs = (f.get() - start)/1000000;
            assertTrue(elapsedMs < 2000, "executed too late");
            assertTrue(elapsedMs > 100, "executed too soon");
        }
    }

    private long throwingTimestamp() throws IOException {
        long ts = System.nanoTime();
        if (ts == -1) throw new IOException("NOT REALLY");
        return ts;
    }

    @Test
    void testScheduleConcurrency() throws ExecutionException, InterruptedException {
        int nTasks = 128 * N_PROCESSORS;
        long[] start = new long[nTasks];
        List<CompletableFuture<Long>> futures = new ArrayList<>();
        List<AsyncTask<?>> schedFutures = new ArrayList<>();
        for (int i = 0; i < nTasks; i++) {
            CompletableFuture<Long> future = new CompletableFuture<>();
            futures.add(future);
            start[i] = System.nanoTime();
            if ((i % 2) == 0) {
                schedFutures.add(Async.scheduleThrowing(200, MILLISECONDS,
                        () -> future.complete(throwingTimestamp())));
            } else {
                schedFutures.add(Async.schedule(200, MILLISECONDS,
                        () -> { future.complete(System.nanoTime()); }));
            }
        }
        for (int i = 0; i < nTasks; i++) {
            long elapsedMs = (futures.get(i).get() - start[i])/1000000;
            assertTrue(elapsedMs > 100, "executed too soon");
            assertTrue(elapsedMs < 5000, "executed too late");
            long getStart = System.nanoTime();
            Object nil = schedFutures.get(i).get();
            assertTrue(System.nanoTime()-getStart < 100*1000000L,
                       "ScheduledFuture.get() is too slow");
        }
    }

    @Test
    void testAsyncConcurrency() throws ExecutionException, InterruptedException {
        int waitMs = 200;
        int nTasks = 4*N_PROCESSORS;
        List<Future<Integer>> intFutures = new ArrayList<>(nTasks);
        List<Future<?>> voidFutures = new ArrayList<>(nTasks);
        for (int i = 0; i < nTasks; i++) {
            int id = i;
            if (i % 3 == 0) {
                intFutures.add(Async.async(() -> {Thread.sleep(waitMs); return id;}));
            } else if (i % 3 == 1) {
                CompletableFuture<Integer> future = new CompletableFuture<>();
                intFutures.add(future);
                voidFutures.add(Async.async(() -> {
                    try { Thread.sleep(waitMs); } catch (InterruptedException ignored) { }
                    future.complete(id);
                }));
            } else {
                CompletableFuture<Integer> future = new CompletableFuture<>();
                intFutures.add(future);
                voidFutures.add(Async.asyncThrowing(() -> {
                    Thread.sleep(waitMs);
                    future.complete(id);
                }));
            }
        }
        long getStart = System.nanoTime();
        for (int i = 0; i < nTasks; i++)
            assertEquals(i, intFutures.get(i).get());
        long elapsedMs = (System.nanoTime()-getStart)/1000000;
        int maxMs = waitMs + 400;
        assertTrue(maxMs < (nTasks/N_PROCESSORS)*waitMs); //sanity
        assertTrue(elapsedMs < maxMs, "Too slow, missing concurrency!");

        getStart = System.nanoTime();
        for (Future<?> future : voidFutures)
            assertNull(future.get());
        elapsedMs = (System.nanoTime()-getStart)/1000000;
        assertTrue(elapsedMs < 200, "get() on near-complete Futures is too slow");
    }

    @Test
    void testCancelScheduled() throws InterruptedException {
        AtomicInteger zero = new AtomicInteger(0);
        AsyncTask<Integer> f0 = Async.schedule(150, MILLISECONDS, zero::getAndIncrement);
        AsyncTask<?> f1 = Async.schedule(150, MILLISECONDS, () -> zero.set(23));
        AsyncTask<?> f2 = Async.scheduleThrowing(150, MILLISECONDS,
                                                 () -> zero.set((int)throwingTimestamp()));
        assertTrue(f0.cancel(false));
        assertTrue(f1.cancel(false));
        assertTrue(f2.cancel(false));
        assertTrue(f0.isCancelled());
        assertTrue(f1.isCancelled());
        assertTrue(f2.isCancelled());
        Thread.sleep(300);
        assertEquals(0, zero.get());
    }

    private int sleepyGet(AtomicInteger zero, int id) throws InterruptedException {
        Thread.sleep(200);
        zero.addAndGet(id);
        return id;
    }

    private void safeSleepyGet(AtomicInteger zero, int id) {
        try { sleepyGet(zero, id); } catch (InterruptedException ignored) { }
    }

    @Test
    void testCancelInterrupting() throws InterruptedException {
        int nTasks = 4*N_PROCESSORS;
        AtomicInteger zero = new AtomicInteger(0);
        List<AsyncTask<?>> tasks = IntStream.range(0, nTasks).mapToObj(i -> {
            if      (i % 3 == 0) return Async.async(        () -> sleepyGet(zero, i));
            else if (i % 3 == 1) return Async.async(        () -> safeSleepyGet(zero, i));
            else                 return Async.asyncThrowing(() -> sleepyGet(zero, i));
        }).collect(Collectors.toList());
        tasks.forEach(t -> t.cancel(true));
        for (int i = 0; i < tasks.size(); i++)
            assertTrue(tasks.get(i).isCancelled(), "i=" + i);
        Thread.sleep(300);
        assertEquals(0, zero.get());
    }

    @Test
    void testNullRunnable() throws ExecutionException {
        List<AsyncTask<?>> tasks = Arrays.asList(
                Async.async((Runnable) null),
                Async.async((Callable<String>) null),
                Async.asyncThrowing(null),
                Async.schedule(1, MILLISECONDS, (Runnable) null),
                Async.schedule(1, MILLISECONDS, (Callable<Integer>) null),
                Async.scheduleThrowing(1, MILLISECONDS, null)
        );
        for (int i = 0; i < tasks.size(); i++) {
            assertFalse(tasks.get(i).isCancelled(), "i="+i);
            assertTrue(tasks.get(i).isDone(), "i="+i);
            assertNull(tasks.get(i).get(), "i="+i);
        }
    }

    @Test
    void testPoll() throws ExecutionException, InterruptedException {
        AtomicInteger integer = new AtomicInteger(0);
        AsyncTask<?> task = Async.poll(5, () -> integer.get() == 1);
        assertFalse(task.isDone());
        integer.set(1);
        CompletableFuture<Long> msFuture = new CompletableFuture<>();
        Thread thread = new Thread(() -> {
            try {
                long start = System.nanoTime();
                assertNull(task.get());
                msFuture.complete((System.nanoTime() - start) / 1000000);
            } catch (Throwable t) { msFuture.completeExceptionally(t); }
        });
        thread.start();
        assertTrue(msFuture.get() < 100,
                  "get() took " + msFuture.get() + "ms, expected 10");
    }

    @Test
    void testWrapFuture() throws ExecutionException {
        CompletableFuture<Integer> ok = new CompletableFuture<>();
        CompletableFuture<Integer> fail = new CompletableFuture<>();
        AsyncTask<Integer> okTask = Async.wrap((Future<Integer>) ok);
        SafeAsyncTask<Integer> safeOkTask = Async.wrapSafe((Future<Integer>) ok);
        AsyncTask<Integer> failTask = Async.wrap((Future<Integer>) fail);
        CompletableFuture<Integer> okResult = new CompletableFuture<>();
        CompletableFuture<Integer> safeOkResult = new CompletableFuture<>();
        CompletableFuture<Integer> failResult = new CompletableFuture<>();
        new Thread(() -> okTask.handle((i, t) -> t == null ? okResult.complete(i) : okResult.completeExceptionally(t))).start();
        new Thread(() -> safeOkTask.handle((i, t) -> t == null ? safeOkResult.complete(i) : safeOkResult.completeExceptionally(t))).start();
        new Thread(() -> failTask.handle((i, t) -> t == null ? failResult.complete(i) : failResult.completeExceptionally(t))).start();

        ok.complete(23);
        long start = System.nanoTime();
        assertEquals(23, okTask.get());
        long ms = (System.nanoTime() - start) / 1000000;
        assertTrue(ms < 50, "okTask.get() too slow: "+ms+"ms");

        ok.complete(23);
        start = System.nanoTime();
        assertEquals(23, safeOkTask.get());
        ms = (System.nanoTime() - start) / 1000000;
        assertTrue(ms < 50, "okTask.get() too slow: "+ms+"ms");

        IllegalStateException cause = new IllegalStateException();
        fail.completeExceptionally(cause);
        start = System.nanoTime();
        try {
            assertEquals(23, failTask.get());
            Assertions.fail("Expected ExecutionException to be throw");
        } catch (ExecutionException e) {
            assertSame(cause, e.getCause());
        }
        ms = (System.nanoTime() - start) / 1000000;
        assertTrue(ms < 50, "failTask.get() too slow: "+ms+"ms");
    }
}