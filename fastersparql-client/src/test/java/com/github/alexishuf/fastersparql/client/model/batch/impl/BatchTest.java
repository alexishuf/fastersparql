package com.github.alexishuf.fastersparql.client.model.batch.impl;

import com.github.alexishuf.fastersparql.client.model.batch.Batch;
import com.github.alexishuf.fastersparql.client.model.batch.EmptyBatchIt;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class BatchTest {
    private static Batch.Builder<Integer> builder(int min, int max, long minNs, int size) {
        try (var it = new EmptyBatchIt<>(Integer.class)) {
            it.minBatch(min).maxBatch(max).minWait(minNs, TimeUnit.NANOSECONDS);
            var builder = new Batch.Builder<>(it);
            for (int i = 0; i < size; i++)
                builder.add(i);
            return builder;
        }
    }

    public static Stream<Arguments> testReadySize() {
        return Stream.of(
        /* 1 */ arguments(builder(1, 3, 2_000_000_000, 0), false),
        /* 2 */ arguments(builder(1, 3, 2_000_000_000, 1), false),
        /* 3 */ arguments(builder(1, 3, 2_000_000_000, 2), false),
        /* 5 */ arguments(builder(1, 3, 2_000_000_000, 3), true),
        /* 6 */ arguments(builder(2, 3, 2_000_000_000, 1), false),
        /* 7 */ arguments(builder(2, 3, 2_000_000_000, 2), false),
        /* 8 */ arguments(builder(2, 3, 2_000_000_000, 3), true)
        );
    }

    @ParameterizedTest @MethodSource
    void testReadySize(Batch.Builder<Integer> batch, boolean expected) {
        assertEquals(expected, batch.ready());
    }

    @Test
    void testReadyOnIncrement() {
        var batch = builder(1, 2, 5_000_000, 0);
        assertFalse(batch.ready());
        batch.add(23);
        assertFalse(batch.ready());
        batch.add(27);
        assertTrue(batch.ready());
    }

    @Test
    void testTimeDoesNotMakeReady() {
        var batch = builder(1, 2, 1_000_000, 0);
        long ts = System.nanoTime() + 1_000_000;
        while (System.nanoTime() < ts)
            Thread.yield();
        assertFalse(batch.ready());
        batch.add(23);
        assertTrue(batch.ready());
    }

    @Test
    void testTimeMakesReady() {
        var batch = builder(2, 3, 5_000_000, 0);
        long start = System.nanoTime();
        assertFalse(batch.ready());
        batch.add(23);
        assertFalse(batch.ready());
        batch.add(23);
        assertFalse(batch.ready());
        //noinspection StatementWithEmptyBody
        while (System.nanoTime() < start+2_500_000) { }
        assertFalse(batch.ready());

        while (System.nanoTime() < start+5_000_000) Thread.yield();
        assertTrue(batch.ready());
    }

    static Stream<Arguments> testEquals() {
        return Stream.of(
                arguments(new Batch<>(Integer.class), new Batch<>(Integer.class), true),
                arguments(new Batch<>(new Integer[] {23}, 1),
                          new Batch<>(new Integer[] {23}, 1), true),
                arguments(new Batch<>(new Integer[] {23}, 1),
                          new Batch<>(new Integer[] {27}, 1), false),
                arguments(new Batch<>(new Integer[] {23, 27}, 2),
                          new Batch<>(new Integer[] {23, 27}, 2), true),
                arguments(new Batch<>(new Integer[] {23, 27}, 2),
                          new Batch<>(new Integer[] {23, 31}, 2), false),
                arguments(new Batch<>(new Integer[] {23, 27}, 2),
                          new Batch<>(new Integer[] {27, 23}, 2), false),
                arguments(new Batch<>(new Integer[] {23, 27}, 1),
                          new Batch<>(new Integer[] {23, 31}, 1), true),
                arguments(new Batch<>(new Integer[] {23, 27}, 2),
                          new Batch<>(new Integer[] {23, 27}, 1), false)
        );
    }

    @ParameterizedTest @MethodSource
    void testEquals(Batch<Integer> left, Batch<Integer> right, boolean expected) {
        assertEquals(expected, left.equals(right));
        assertEquals(expected, right.equals(left));
    }

}