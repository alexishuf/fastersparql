package com.github.alexishuf.fastersparql.client.model.batch;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SingletonBatchItTest {

    @Test
    void testPlain() {
        try (var it = new SingletonBatchIterator<>(23)) {
            assertTrue(it.hasNext());
            assertEquals(23, it.next());
            assertFalse(it.hasNext());
        }
    }

    @ParameterizedTest @ValueSource(ints = {1, 2, 4})
    void testArray(int min) {
        try (var it = new SingletonBatchIterator<>(23)) {
            it.minBatch(min).minWait(Duration.ofSeconds(30));
            long start = System.nanoTime();
            assertEquals(new Batch<>(new Integer[] {23}, 1), it.nextBatch());
            assertEquals(new Batch<>(Integer.class), it.nextBatch());
            assertTrue(System.nanoTime()-start < Duration.ofSeconds(1).toNanos());
        }
    }

    @ParameterizedTest @ValueSource(ints = {1, 2, 4})
    void testCollection(int min) {
        try (var it = new SingletonBatchIterator<>(23)) {
            it.minBatch(min).minWait(Duration.ofSeconds(30));
            var list = new ArrayList<>(List.of(27));
            long start = System.nanoTime();
            assertEquals(1, it.nextBatch(list));
            assertEquals(0, it.nextBatch(list));
            assertEquals(List.of(27, 23), list);
            assertTrue(System.nanoTime()-start < Duration.ofSeconds(1).toNanos());
        }
    }

}