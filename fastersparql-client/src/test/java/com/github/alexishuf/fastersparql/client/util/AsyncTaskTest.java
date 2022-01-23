package com.github.alexishuf.fastersparql.client.util;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.jupiter.api.Assertions.*;

@Timeout(2)
class AsyncTaskTest {
    @Test
    void testGetUninterruptible() throws InterruptedException, ExecutionException {
        CompletableAsyncTask<String> task = new CompletableAsyncTask<>();
        CompletableFuture<String> future = new CompletableFuture<>();
        Semaphore semaphore = new Semaphore(0);
        Thread thread = new Thread(() -> {
            try {
                semaphore.release();
                future.complete(task.get());
            } catch (Throwable t) {
                future.completeExceptionally(t);
            }
        });
        thread.start();

        //deliver interrupt while thread waits on get()
        semaphore.acquireUninterruptibly();
        Thread.sleep(50);
        thread.interrupt();
        Thread.sleep(10);

        //complete task and expect no sign of the interrupt
        task.complete("value");
        assertEquals("value", future.get());
    }

    @Test
    void testGetUninterruptibleTimeout() throws ExecutionException, InterruptedException {
        CompletableAsyncTask<String> task = new CompletableAsyncTask<>();
        CompletableFuture<Object> future = new CompletableFuture<>();
        Semaphore semaphore = new Semaphore(0);
        Thread thread = new Thread(() -> {
            semaphore.release();
            try {
                future.complete(task.get(1, TimeUnit.SECONDS));
            } catch (Throwable t) { future.completeExceptionally(t); }
        });
        thread.start();

        //deliver interrupt during get() wait
        semaphore.acquireUninterruptibly();
        Thread.sleep(50);
        thread.interrupt();
        Thread.sleep(10);

        //test was not interrupted
        task.complete("pass");
        assertEquals("pass", future.get());
    }

    @Test
    void testFetchException() {
        CompletableAsyncTask<String> task = new CompletableAsyncTask<>();
        IllegalArgumentException cause = new IllegalArgumentException();
        task.completeExceptionally(cause);
        assertTrue(task.isDone());
        assertTrue(task.isCompletedExceptionally());
        assertFalse(task.isCancelled());
        try {
            task.fetch();
            fail("Expected exception to be thrown");
        } catch (RuntimeExecutionException e) {
            assertSame(cause, e.getCause());
        } catch (Throwable t) {
            fail("Expected RuntimeExecutionException to be thrown, got "+t);
        }
    }

    @Test
    void testFetch() {
        CompletableAsyncTask<String> task = new CompletableAsyncTask<>();
        String value = "value";
        task.complete(value);
        assertTrue(task.isDone());
        assertFalse(task.isCompletedExceptionally());
        assertFalse(task.isCancelled());
        assertSame(value, task.fetch());
    }

    @Test
    void testCancel() {
        CompletableAsyncTask<String> task = new CompletableAsyncTask<>();
        task.cancel(false);
        assertTrue(task.isDone());
        assertTrue(task.isCancelled());
        assertThrows(CancellationException.class, task::fetch);
        assertThrows(CancellationException.class, task::get);
        assertThrows(CancellationException.class, () -> task.fetchOrElse(""));
        assertThrows(CancellationException.class, () -> task.orElse(""));
    }

    @Test
    void testOrElse() throws ExecutionException {
        CompletableAsyncTask<String> task = new CompletableAsyncTask<>();
        assertEquals("fallback", task.orElse("fallback"));
        assertEquals("fallback", task.fetchOrElse("fallback"));
    }

    @Test
    void testOrElseException() {
        CompletableAsyncTask<String> task = new CompletableAsyncTask<>();
        IllegalStateException cause = new IllegalStateException();
        task.completeExceptionally(cause);
        try {
            task.orElse("fallback");
            fail("Expected ExecutionException to be thrown");
        } catch (ExecutionException e) {
            assertSame(cause, e.getCause());
        } catch (Throwable t) {
            fail("Expected ExecutionException to be thrown, got "+t);
        }
    }

    @Test
    void testFetchOrElseException() {
        CompletableAsyncTask<String> task = new CompletableAsyncTask<>();
        IllegalStateException cause = new IllegalStateException();
        task.completeExceptionally(cause);
        try {
            task.fetchOrElse("fallback");
            fail("Expected ExecutionException to be thrown");
        } catch (RuntimeExecutionException e) {
            assertSame(cause, e.getCause());
        } catch (Throwable t) {
            fail("Expected ExecutionException to be thrown, got "+t);
        }
    }

    @Test
    void testTimeout() throws InterruptedException, ExecutionException {
        CompletableAsyncTask<Long> task = new CompletableAsyncTask<>();
        ExecutorService exec = Executors.newCachedThreadPool();
        List<Future<Long>> futures = new ArrayList<>();

        // create threads in executor
        for (int i = 0; i < 4; i++)
            futures.add(exec.submit(System::nanoTime));
        for (Future<Long> f : futures)
            assertTrue(f.get() != 0);
        futures.clear();


        // call timeout methods in parallel
        long start = System.nanoTime();
        boolean terminated;
        try {
            futures.add(exec.submit(() -> task.get(100, MILLISECONDS)));
            futures.add(exec.submit(() -> task.fetch(100, MILLISECONDS)));
            futures.add(exec.submit(() -> task.orElse(2L, 100, MILLISECONDS)));
            futures.add(exec.submit(() -> task.fetchOrElse(3L, 100, MILLISECONDS)));
            for (int i = 0; i < 2; i++) {
                try {
                    futures.get(i).get();
                    fail("Expected ExecutionException to be thrown. i="+i);
                } catch (ExecutionException e) {
                    assertTrue(e.getCause() instanceof TimeoutException, "i="+i);
                }
            }
            for (int i = 2; i < 4; i++)
                assertEquals(i, futures.get(i).get(), "i="+i);
        } finally {
            exec.shutdown();
            terminated = exec.awaitTermination(1, MINUTES);
        }
        long elapsedMs = (System.nanoTime() - start) / 1000000;
        assertTrue(elapsedMs > 75, "Only "+elapsedMs+" ms since start, ");
        assertTrue(terminated);
    }

    @Test
    void testThenAcceptAfterCompletion() throws ExecutionException, InterruptedException {
        CompletableAsyncTask<String> task = new CompletableAsyncTask<>();
        String value = "value";
        assertTrue(task.complete(value));
        CompletableFuture<String> future = new CompletableFuture<>();
        task.thenAccept(future::complete);
        assertTrue(future.isDone());
        assertSame(value, future.get());
    }

    @Test
    void testThenAcceptBeforeCompletion() throws ExecutionException, InterruptedException {
        String value = "value";
        CompletableAsyncTask<String> task = new CompletableAsyncTask<>();
        CompletableFuture<String> future = new CompletableFuture<>();
        task.thenAccept(future::complete);
        task.complete(value);
        assertSame(value, future.get());
    }

    @Test
    void testHandle() throws ExecutionException, InterruptedException {
        String value = "value";
        IllegalArgumentException cause = new IllegalArgumentException();
        for (int i = 0; i < 2; i++) {
            CompletableAsyncTask<String> task = new CompletableAsyncTask<>();
            CompletableFuture<String> future = new CompletableFuture<>();
            task.handle((v, e) -> e == null ? future.complete(v) : future.completeExceptionally(e));
            if (i % 2 == 0) {
                task.complete(value);
                assertSame(value, future.get());
            } else {
                task.completeExceptionally(cause);
                try {
                    future.get();
                    fail("Expected ExecutionException to be thrown");
                } catch (ExecutionException e) {
                    assertSame(cause, e.getCause());
                }
            }
        }
    }

}