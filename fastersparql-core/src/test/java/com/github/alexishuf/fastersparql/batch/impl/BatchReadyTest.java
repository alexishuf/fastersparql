package com.github.alexishuf.fastersparql.batch.impl;

import com.github.alexishuf.fastersparql.client.model.Vars;
import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.Batch;
import com.github.alexishuf.fastersparql.batch.base.AbstractBIt;
import org.checkerframework.common.returnsreceiver.qual.This;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Collection;
import java.util.stream.Stream;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public class BatchReadyTest {
    static Stream<Arguments> testReady() {
        long ms = 1_000_000L;
        return Stream.of(
                // (size, min, max, atLeast, atMost, startOffset, expected)
                // max size causes completion whatever the timestamps
                /*  1 */arguments(3, 1, 2, 5*ms, 10*ms,  0,     true), // max reached before atLeast,atMost
                /*  2 */arguments(3, 1, 2, 5*ms, 10*ms, -6*ms,  true), // max reached before atMost
                /*  3 */arguments(3, 1, 2, 5*ms, 10*ms, -11*ms, true), // max reached after atLeast and atMost
                /*  4 */arguments(2, 1, 2, 5*ms, 10*ms,  0,     true), // max reached before atLeast,atMost
                /*  5 */arguments(2, 1, 2, 5*ms, 10*ms,  6*ms,  true), // max reached before atMost
                /*  6 */arguments(2, 1, 2, 5*ms, 10*ms, -11*ms, true), // max reached after atLeast and atMost
                // wait atLeast for min
                /*  7 */arguments(1, 2, 4, 5*ms, 10*ms,  0,    false), // size < min
                /*  8 */arguments(2, 2, 4, 5*ms, 10*ms,  0,    false), // size ok, but atLeast not reached
                /*  9 */arguments(3, 2, 4, 5*ms, 10*ms,  0,    false), // size ok, but atLeast not reached
                /* 10 */arguments(4, 2, 4, 5*ms, 10*ms,  0,    true),  // size >= max
                /* 11 */arguments(1, 2, 4, 5*ms, 10*ms, -6*ms, false), // atLeast reached but not atMost
                /* 12 */arguments(1, 2, 4, 0,    10*ms,  0,    false), // atLeast reached but not atMost
                /* 13 */arguments(2, 2, 4, 5*ms, 10*ms, -6*ms, true), // min and atLeast reached
                /* 14 */arguments(3, 2, 4, 5*ms, 10*ms, -6*ms, true), // min and atLeast reached
                /* 15 */arguments(2, 2, 4, 0,    10*ms,  0,    true), // min and atLeast reached
                /* 16 */arguments(3, 2, 4, 0,    10*ms, -6*ms, true), // min and atLeast reached
                // wait atMost for min
                /* 17 */arguments(1, 2, 4, 5*ms, 10*ms,  0,    false),
                /* 18 */arguments(1, 2, 4, 5*ms, 10*ms, -1*ms, false),
                /* 18 */arguments(1, 2, 4, 5*ms, 10*ms, -11*ms, true), // atMost expired
                /* 19 */arguments(1, 2, 4, 5*ms, 10*ms, -10*ms, true), // atMost expired
                /* 20 */arguments(1, 2, 4, 0   , 10*ms, -11*ms, true), // atMost expired
                /* 21 */arguments(2, 2, 4, 5*ms, 10*ms, -11*ms, true), // min, atLeast and atMost reached
                /* 22 */arguments(3, 2, 4, 5*ms, 10*ms, -11*ms, true), // min, atLeast and atMost reached
                /* 23 */arguments(4, 2, 4, 5*ms, 10*ms, -11*ms, true), // all conditions reached,
                // an empty batch is never ready
                /* 24 */arguments(0, 1, 4,  5*ms, 10*ms,  0,    false), // empty, before atLeast
                /* 25 */arguments(0, 1, 4,  5*ms, 10*ms, -5*ms, false), // empty, at atLeast
                /* 26 */arguments(0, 1, 4,  5*ms, 10*ms, -6*ms, false), // empty, after atLeast
                /* 27 */arguments(0, 1, 4,  5*ms, 10*ms, -10*ms, false), // empty, at atMost
                /* 28 */arguments(0, 1, 4,  5*ms, 10*ms, -11*ms, false) // empty, after atMost
        );
    }

    @ParameterizedTest @MethodSource
    void testReady(int size, int min, int max, long atLeast, long atMost, long startOffset, boolean expected) {
        try (var helper = new ReadyHelper()) {
            helper.minBatch(min).maxBatch(max);
            helper.minWait(atLeast, NANOSECONDS).maxWait(atMost, NANOSECONDS);
            assertEquals(expected, helper.ready(size, System.nanoTime() + startOffset));
        }
    }

    private static final class ReadyHelper extends AbstractBIt<Integer> {
        public ReadyHelper() { super(Integer.class, Vars.EMPTY); }
        @Override public boolean ready(int size, long start) { return super.ready(size, start); }
        @Override public @This BIt<Integer> tempEager() { return this; }
        @Override public Batch<Integer> nextBatch() { throw new UnsupportedOperationException(); }
        @Override public int nextBatch(Collection<? super Integer> destination) { throw new UnsupportedOperationException(); }
        @Override public boolean recycle(Batch<Integer> batch) { return false; }
        @Override protected void cleanup(boolean interrupted) { }
        @Override public boolean hasNext() { throw new UnsupportedOperationException(); }
        @Override public Integer next() { throw new UnsupportedOperationException(); }
        @Override public String toString() { return "test"; }
    }
}
