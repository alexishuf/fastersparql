package com.github.alexishuf.fastersparql.client.model.batch.impl;

import com.github.alexishuf.fastersparql.client.model.batch.Batch;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.stream.Stream;

import static java.util.stream.IntStream.range;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class BatchTest {
    static Stream<Arguments> testEquals() {
        return Stream.of(
                arguments(new Batch<>(Integer.class, 0), new Batch<>(Integer.class, 0), true),
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

    @ParameterizedTest @ValueSource(ints = {0, 1, 3, 8, 9, 10, 11, 14, 15, 16, 21, 22, 23})
    void testGrow(int capacity) {
        Batch<Integer> batch = new Batch<>(Integer.class, capacity);
        for (int i = 0; i < capacity; i++)
            batch.add(1+i);
        assertArrayEquals(range(1, 1+capacity).boxed().toArray(Integer[]::new), batch.array);

        batch.add(capacity+1);
        assertEquals(capacity+1, batch.size());
        assertTrue(batch.array.length >= capacity+1);
        for (int i = 0; i < capacity + 1; i++)
            assertEquals(1+i, batch.array[i], "Mismatch at i="+i);
    }

    @ParameterizedTest @ValueSource(ints = {1, 2, 3, 4, 10, 14, 15, 16, 21, 22, 23, 512})
    void testAddArray(int total) {
        Integer[] src = range(1, 1+total+1).boxed().toArray(Integer[]::new);
        for (int mid = 0; mid < total; mid++) {
            Batch<Integer> b = new Batch<>(Integer.class, mid == 0 ? 0 : Math.min(total, 10));

            b.add(src, 0, mid);
            Integer[] expected = range(1, 1 + mid).boxed().toArray(Integer[]::new);
            assertEquals(new Batch<>(expected, expected.length), b, "mid="+mid);

            int rem = total - mid;
            b.add(src, mid, rem);
            expected = range(1, 1 + total).boxed().toArray(Integer[]::new);
            assertEquals(new Batch<>(expected, expected.length), b);

            if (b.array.length >= 10) {
                int expectedCapacity = 10;
                while (expectedCapacity < expected.length)
                    expectedCapacity += expectedCapacity/2;
                assertEquals(expectedCapacity, b.array.length, "mid="+mid);
            }
        }
    }

    @ParameterizedTest @ValueSource(ints = {0, 1, 2, 3, 4, 9, 10, 16, 4096})
    void testReduce(int size) {
        Batch<Integer> b = new Batch<>(Integer.class, 10);
        int expected = 0;
        for (int i = 0; i < size; i++) {
            expected += i;
            b.add(i);
        }
        assertEquals(expected, b.reduce(0, Integer::sum));
        if (size == 0)
            assertNull(b.reduce(Integer::sum));
        else
            assertEquals(expected, b.reduce(Integer::sum));
    }
}