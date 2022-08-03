package com.github.alexishuf.fastersparql.client.util;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class CircularBufferTest {

    public static Stream<Arguments> testFill() {
        List<Arguments> list = new ArrayList<>();
        for (Integer capacity : asList(0, 1, 10)) {
            for (Integer size : asList(0, 1, 3, 15, 16, 17, 31, 32, 33, 1024))
                list.add(arguments(capacity, size));
        }
        return list.stream();
    }

    @ParameterizedTest @MethodSource
    void testFill(int capacity, int size) {
        CircularBuffer<Integer> buffer = new CircularBuffer<>(capacity);
        assertEquals(0, buffer.size());
        assertTrue(buffer.isEmpty());
        for (int i = 0; i < size; i++) {
            buffer.add(i);
            assertFalse(buffer.isEmpty());
            assertEquals(i+1, buffer.size());
            for (int j = 0; j <= i; j++)
                assertEquals(j, buffer.get(j));
        }
    }

    @Test
    void testEmptySize() {
        CircularBuffer<Object> buffer = new CircularBuffer<>();
        assertEquals(0, buffer.size());
        assertTrue(buffer.isEmpty());
    }

    @Test
    void testWrapAroundSize() {
        CircularBuffer<Integer> buffer = new CircularBuffer<>(10);
        for (int i = 0; i < 10; i++) buffer.add(i);
        assertEqualsToRange(buffer, 0, 10);

        buffer.removeFirst(1);
        assertEqualsToRange(buffer, 1, 9);

        buffer.add(10);
        assertEquals(10, buffer.size());
        assertFalse(buffer.isEmpty());
        assertEqualsToRange(buffer, 1, 10);

        buffer.removeFirst(1);
        assertEqualsToRange(buffer, 2, 9);

        buffer.add(11);
        assertEquals(10, buffer.size());
        assertFalse(buffer.isEmpty());
        assertEqualsToRange(buffer, 2, 10); // 2 3 4 5 6 7 8 9 10 11

        buffer.removeFirst(8);  // _ _ _ _ _ _ _ _ 10 11
        assertEquals(2, buffer.size());
        assertFalse(buffer.isEmpty());
        assertEqualsToRange(buffer, 10, 2);
    }

    static Stream<Arguments> testMovingWindow() {
        return Stream.of(
                arguments(1, 1),
                arguments(1, 4),
                arguments(4, 3),
                arguments(4, 4),
                arguments(16, 1),
                arguments(16, 4),
                arguments(16, 8)
        );
    }

    private void assertEqualsToRange(CircularBuffer<Integer> buffer, int first, int size) {
        assertEquals(size, buffer.size());
        for (int i = 0; i < size; i++)
            assertEquals(first+i, buffer.get(i), "i="+i);
    }

    @ParameterizedTest @MethodSource
    void testMovingWindow(int capacity, int window) {
        CircularBuffer<Integer> buffer = new CircularBuffer<>(capacity);
        for (int i = 0; i < window; i++)
            buffer.add(i);
        int first = 0, next = window;
        assertEqualsToRange(buffer, first, window);
        for (int i = 0; i < capacity; i++) {
            buffer.removeFirst(1);
            assertEqualsToRange(buffer, ++first, window-1);
            buffer.add(next++);
            assertEqualsToRange(buffer, first, window);
        }
    }

    @Test
    void testZeroCapacity() {
        CircularBuffer<Integer> buffer = new CircularBuffer<>(0);
        buffer.add(1);
        assertEqualsToRange(buffer, 1, 1);
        for (int i = 2; i < 33; i++) {
            buffer.add(i);
            assertEqualsToRange(buffer, 1, i);
        }
    }

    @Test
    void testTrivialGrow() {
        CircularBuffer<Integer> buffer = new CircularBuffer<>(2);
        buffer.add(1);
        assertEqualsToRange(buffer, 1, 1);
        buffer.add(2);
        assertEqualsToRange(buffer, 1, 2);
        buffer.add(3);
        assertEqualsToRange(buffer, 1, 3);
    }

    @Test
    void testTrivialBatchRemove() {
        CircularBuffer<Integer> buffer = new CircularBuffer<>(4);
        for (int i = 0; i < 4; i++) buffer.add(i);
        buffer.removeFirst(2);
        assertEqualsToRange(buffer, 2, 2);
    }

    @Test
    void testBatchRemoveWrapAround() {
        CircularBuffer<Integer> buffer = new CircularBuffer<>(3);

        for (int i = 0; i < 3; i++) buffer.add(i); // buffer: >0  1  2
        assertEqualsToRange(buffer, 0, 3);

        buffer.removeFirst(2);                  // buffer:  _  _ >2
        assertEqualsToRange(buffer, 2, 1);

        for (int i = 3; i < 5; i++) buffer.add(i); // buffer:  3  4 >2
        assertEqualsToRange(buffer, 2, 3);

        buffer.removeFirst(2);                  // buffer:  _ >4  _
        assertEqualsToRange(buffer, 4, 1);
    }

    @ParameterizedTest @ValueSource(ints = {1, 2, 4, 8, 16, 32})
    void testClear(int size) {
        CircularBuffer<Integer> buffer = new CircularBuffer<>();
        for (int i = 0; i < size; i++) buffer.add(i);
        assertEqualsToRange(buffer, 0, size);

        buffer.clear();
        assertEquals(buffer.size(), 0);
        assertTrue(buffer.isEmpty());
    }

    @ParameterizedTest @ValueSource(ints = {1, 2, 3, 15, 16, 32})
    void testRemoveAllOnce(int size) {
        CircularBuffer<Integer> buffer = new CircularBuffer<>();
        for (int i = 0; i < size; i++) buffer.add(i);
        assertEqualsToRange(buffer, 0, size);

        buffer.removeFirst(buffer.size());
        assertEquals(0, buffer.size());
        assertTrue(buffer.isEmpty());
    }

    @ParameterizedTest @ValueSource(ints = {1, 2, 3, 15, 16, 32})
    void testRemoveAllBySteps(int size) {
        CircularBuffer<Integer> buffer = new CircularBuffer<>();
        for (int i = 0; i < size; i++) buffer.add(i);
        assertEqualsToRange(buffer, 0, size);

        for (int i = 0; i < size; i++) {
            buffer.removeFirst(1);
            assertEqualsToRange(buffer, i+1, size-i-1);
        }
        assertTrue(buffer.isEmpty());
    }
}