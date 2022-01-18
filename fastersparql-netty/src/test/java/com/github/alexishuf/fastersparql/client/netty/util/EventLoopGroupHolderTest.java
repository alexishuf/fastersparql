package com.github.alexishuf.fastersparql.client.netty.util;

import io.netty.channel.EventLoopGroup;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

class EventLoopGroupHolderTest {
    @Test
    void testKeepAlive1Second() throws InterruptedException {
        EventLoopGroupHolder holder = EventLoopGroupHolder.builder().keepAlive(500)
                .keepAliveTimeUnit(TimeUnit.MILLISECONDS).transport(NettyTransport.NIO).build();
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
    void testNoKeepAlive() {
        EventLoopGroupHolder holder = EventLoopGroupHolder.builder().transport(NettyTransport.NIO).keepAlive(0).build();
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
    void testConcurrency() throws InterruptedException, ExecutionException {
        EventLoopGroupHolder holder = EventLoopGroupHolder.builder().build();
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