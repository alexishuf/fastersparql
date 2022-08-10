package com.github.alexishuf.fastersparql.client.netty.util;

import com.github.alexishuf.fastersparql.client.util.async.Async;
import com.github.alexishuf.fastersparql.client.util.async.AsyncTask;
import io.netty.channel.EventLoopGroup;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.*;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.*;

class EventLoopGroupHolderTest {
    static AsyncTask<?> keepAlive1Second;
    static AsyncTask<?> noKeepAlive;
    static AsyncTask<?> concurrency;

    @BeforeAll
    static void beforeAll() {
        EventLoopGroupHolderTest instance = new EventLoopGroupHolderTest();
        keepAlive1Second = Async.asyncThrowing(instance::doTestKeepAlive1Second);
        noKeepAlive = Async.asyncThrowing(instance::doTestNoKeepAlive);
        concurrency = Async.asyncThrowing(instance::doTestConcurrency);
    }

    @Test
    void testKeepAlive1Second() throws ExecutionException {
        keepAlive1Second.get();
    }

    private void doTestKeepAlive1Second() throws InterruptedException {
        EventLoopGroupHolder holder = new EventLoopGroupHolder(NettyTransport.NIO, 500, MILLISECONDS);
        EventLoopGroup elg = holder.acquire();
        assertFalse(elg.isShutdown());
        assertFalse(elg.isShuttingDown());
        assertFalse(elg.isTerminated());
        holder.release();

        EventLoopGroup elg2 = holder.acquire();
        assertSame(elg, elg2);
        assertFalse(elg2.isShutdown());
        assertFalse(elg2.isShuttingDown());
        assertFalse(elg2.isTerminated());
        Thread.sleep(600); //wait for rogue keepAlive killer
        assertFalse(elg.isShuttingDown()); // keepAlive

        holder.release();
        assertFalse(elg.isShuttingDown()); // keepAlive

        Thread.sleep(600); // wait keepAlive expire
        assertTrue(elg.isShuttingDown()); //shutdown started

        EventLoopGroup elg3 = holder.acquire();
        assertNotSame(elg, elg3);
        assertFalse(elg3.isShutdown());
        assertFalse(elg3.isShuttingDown());
        assertFalse(elg3.isTerminated());
        holder.release();
    }

    @Test
    void testNoKeepAlive() throws ExecutionException {
        noKeepAlive.get();
    }

    private void doTestNoKeepAlive() {
        EventLoopGroupHolder holder = new EventLoopGroupHolder(NettyTransport.NIO, 0, SECONDS);
        EventLoopGroup elg1 = holder.acquire();
        assertFalse(elg1.isShutdown());
        assertFalse(elg1.isShuttingDown());
        assertFalse(elg1.isTerminated());

        EventLoopGroup elg2 = holder.acquire();
        assertSame(elg1, elg2);
        assertFalse(elg2.isShuttingDown());

        holder.release();
        assertFalse(elg2.isShuttingDown());

        holder.release(10, TimeUnit.SECONDS);
        assertTrue(elg1.isShuttingDown());
        assertTrue(elg1.isShutdown());
        assertTrue(elg1.isTerminated());

        EventLoopGroup elg3 = holder.acquire();
        assertNotSame(elg1, elg3);
        assertFalse(elg3.isShutdown());
        assertFalse(elg3.isShuttingDown());
        assertFalse(elg3.isTerminated());
    }

    @Test
    void testConcurrency() throws ExecutionException {
        concurrency.get();
    }

    private void doTestConcurrency() throws InterruptedException, ExecutionException {
        EventLoopGroupHolder holder = new EventLoopGroupHolder(null, 0, SECONDS);
        int tasks = Runtime.getRuntime().availableProcessors() * 64;
        List<Future<Integer>> futures = new ArrayList<>(tasks);
        ExecutorService executor = Executors.newCachedThreadPool();
        boolean onTime;
        try {
            for (int i = 0; i < tasks; i++) {
                int id = i;
                futures.add(executor.submit(() -> {
                    EventLoopGroup elg = holder.acquire();
                    try {
                        assertFalse(elg.isShutdown());
                        assertFalse(elg.isShuttingDown());
                        assertFalse(elg.isTerminated());
                        return elg.submit(() -> id).get();
                    } finally {
                        holder.release();
                    }
                }));
            }
        } finally {
            executor.shutdown();
            onTime = executor.awaitTermination(1, TimeUnit.SECONDS);
        }
        BitSet seen = new BitSet();
        for (Future<Integer> future : futures)
            seen.set(future.get());
        assertEquals(tasks, seen.cardinality());
        assertEquals(tasks, seen.nextClearBit(0));
        assertTrue(onTime);
    }
}