package com.github.alexishuf.fastersparql.util.concurrent;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
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
            // wait for all tasks
            for (Future<String> t : tasks) Optional.ofNullable(t.get()).ifPresent(taken::add);
            tasks.clear();

            //collect any leftovers
            for (int thread = 0; thread < nTasks; thread++)
                tasks.add(ex.submit(() -> AffinityShallowPool.get(COL)));
            for (Future<String> t : tasks) Optional.ofNullable(t.get()).ifPresent(taken::add);
            tasks.clear();
        }
        assertEquals(acceptedOffers.get(), taken.size(), "Since each thread get()s after offer(), all accepted offers should be taken and only what was offered can be taken");
        assertTrue(new HashSet<>(strings).containsAll(taken), "Some taken strings were never offered");
        assertEquals(new HashSet<>(taken).size(), taken.size(), "Some strings were taken more than once");
    }

}