package com.github.alexishuf.fastersparql.util.concurrent;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor;
import static org.junit.jupiter.api.Assertions.*;

class AffinityShallowPoolTest {
    private static final int COL = AffinityShallowPool.reserveColumn();
    private static final String A = "a", B = "b";

    @Test
    void testOfferAndGet() throws InterruptedException {
        AffinityShallowPool.offer(COL, A);
        assertSame(A, AffinityShallowPool.get(COL));
        assertNull(AffinityShallowPool.get(COL));

        Thread.startVirtualThread(() -> AffinityShallowPool.offer(COL, B, 10)).join();
        assertSame(B, AffinityShallowPool.get(COL, 11));
    }

    @Test
    void testConcurrent() throws ExecutionException, InterruptedException {
        int nTasks = Runtime.getRuntime().availableProcessors()*20_000;
        List<String> strings = new ArrayList<>(nTasks), taken = new ArrayList<>(nTasks);
        for (int i = 0; i < nTasks; i++)
            strings.add(Integer.toString(i));
        AtomicInteger acceptedOffers = new AtomicInteger();
        try (var ex = newVirtualThreadPerTaskExecutor()) {
            List<Future<String>> tasks = new ArrayList<>(nTasks);
            for (int i = 0; i < nTasks; i++) {
                int thread = i;
                tasks.add(ex.submit(() -> {
                    boolean accepted = AffinityShallowPool.offer(COL, strings.get(thread)) == null;
                    String s = AffinityShallowPool.get(COL);
                    if (accepted)
                        acceptedOffers.getAndIncrement();
                    return s;
                }));
            }
            for (Future<String> t : tasks) {
                String s = t.get();
                if (s != null)
                    taken.add(s);
            }
            tasks.clear();
            for (int i = 0; i < nTasks*2; i++)
                tasks.add(ex.submit(() -> AffinityShallowPool.get(COL)));
            for (Future<String> t : tasks)
                assertNull(t.get(), "non-null get() after pool had been drained");
        }
        assertEquals(acceptedOffers.get(), taken.size(), "Since each thread get()s after offer(), all accepted offers should be taken and only what was offered can be taken");
    }

}