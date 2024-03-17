package com.github.alexishuf.fastersparql.client.netty.util;

import com.github.alexishuf.fastersparql.client.util.TestTaskSet;
import io.netty.channel.EventLoopGroup;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.*;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.*;

@SuppressWarnings("SameReturnValue")
class EventLoopGroupHolderTest {

    @Test
    void test() throws Exception {
        try (var tasks = TestTaskSet.virtualTaskSet(getClass().getSimpleName())) {
            tasks.add(this::doTestKeepAlive1Second);
            tasks.add(this::doTestNoKeepAlive);
            tasks.add(this::doTestConcurrency);
        }
    }

    private Object doTestKeepAlive1Second() throws InterruptedException {
        EventLoopGroupHolder holder = new EventLoopGroupHolder("test", NettyTransport.NIO, 500, MILLISECONDS, 1);
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
        Thread.sleep(900); //wait for rogue keepAlive killer
        assertFalse(elg.isShuttingDown()); // keepAlive

        holder.release();
        assertFalse(elg.isShuttingDown()); // keepAlive

        Thread.sleep(900); // wait keepAlive expire
        assertTrue(elg.isShuttingDown()); //shutdown started

        EventLoopGroup elg3 = holder.acquire();
        assertNotSame(elg, elg3);
        assertFalse(elg3.isShutdown());
        assertFalse(elg3.isShuttingDown());
        assertFalse(elg3.isTerminated());
        holder.release();
        return null;
    }

    private Object doTestNoKeepAlive() {
        EventLoopGroupHolder holder = new EventLoopGroupHolder("test", NettyTransport.NIO, 0, SECONDS, 1);
        EventLoopGroup elg1 = holder.acquire();
        assertFalse(elg1.isShutdown());
        assertFalse(elg1.isShuttingDown());
        assertFalse(elg1.isTerminated());

        EventLoopGroup elg2 = holder.acquire();
        assertSame(elg1, elg2);
        assertFalse(elg2.isShuttingDown());

        holder.release();
        assertFalse(elg2.isShuttingDown());

        holder.release();
        assertTrue(elg1.isShuttingDown());
        assertTrue(elg1.isShutdown());
        assertTrue(elg1.isTerminated());

        EventLoopGroup elg3 = holder.acquire();
        assertNotSame(elg1, elg3);
        assertFalse(elg3.isShutdown());
        assertFalse(elg3.isShuttingDown());
        assertFalse(elg3.isTerminated());
        return null;
    }

    private Object doTestConcurrency() throws InterruptedException, ExecutionException {
        var holder = new EventLoopGroupHolder("test", null, 0, SECONDS, 1);
        int tasks = Runtime.getRuntime().availableProcessors() * 64;
        List<Future<Integer>> futures = new ArrayList<>(tasks);
        boolean onTime;
        try (ExecutorService executor = Executors.newCachedThreadPool()) {
            try {
                for (int i = 0; i < tasks; i++) {
                    int id = i;
                    futures.add(executor.submit(() -> {
                        try { //noinspection resource
                            var elg = holder.acquire();
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
        }
        BitSet seen = new BitSet();
        for (Future<Integer> future : futures)
            seen.set(future.get());
        assertEquals(tasks, seen.cardinality());
        assertEquals(tasks, seen.nextClearBit(0));
        assertTrue(onTime);
        return null;
    }
}