package com.github.alexishuf.fastersparql.util.concurrent;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class AffinityPoolTest {
    record D(int id) { }

    @Test
    void testOfferAndGet() {
        AffinityPool<D> p = new AffinityPool<>(D.class, 1);
        for (int i = 0; i < 10; i++) {
            D d = new D(i);
            assertNull(p.offer(d));
            assertSame(d, p.get());
            assertNull(p.get());
        }
    }

    static Stream<Arguments> testConcurrentOfferAndGet() {
        Supplier<ExecutorService> virtual, two, platform;
        virtual = new Supplier<>() {
            @Override public ExecutorService get() {
                return Executors.newVirtualThreadPerTaskExecutor();
            }
            @Override public String toString() {return "virtual";}
        };
        two = new Supplier<>() {
            @Override public ExecutorService get() {return Executors.newFixedThreadPool(2);}
            @Override public String toString() {return "two";}
        };
        platform = new Supplier<>() {
            @Override public ExecutorService get() {
                return Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
            }
            @Override public String toString() {return "platform";}
        };
        return Stream.of(
                arguments(virtual),
                arguments(two),
                arguments(platform)
        );
    }

    @ParameterizedTest @MethodSource
    void testConcurrentOfferAndGet(Supplier<ExecutorService> supplier) throws Exception {
        int rounds = 1_000_000;
        List<D> offers = new ArrayList<>(rounds), taken = new ArrayList<>(rounds);
        for (int i = 0; i < rounds; i++) {
            offers.add(new D(i));
            taken.add(new D(i));
        }
        AffinityPool<D> p = new AffinityPool<>(D.class, rounds);
        try (var ex = supplier.get()) {
            List<Future<?>> tasks = new ArrayList<>();
            for (int taskIdx = 0; taskIdx < rounds; taskIdx++) {
                int task = taskIdx;
                tasks.add(ex.submit(() -> {
                    assertNull(p.offer(offers.get(task)));
                    taken.set(task, p.get());
                }));
            }
            for (Future<?> t : tasks)
                t.get();
        }

        List<D> nonNull = new ArrayList<>(rounds);
        for (D d : taken) {
            if (d != null) nonNull.add(d);
        }

        HashSet<D> takenSet = new HashSet<>(nonNull);
        HashSet<D> offerSet = new HashSet<>(offers);
        assertEquals(rounds, nonNull.size(), "Some items were offered but never taken");
        assertEquals(takenSet.size(), nonNull.size(),
                     "Some items were taken more than once");
        assertEquals(offerSet, takenSet, "Some taken items were not offered");
    }

}