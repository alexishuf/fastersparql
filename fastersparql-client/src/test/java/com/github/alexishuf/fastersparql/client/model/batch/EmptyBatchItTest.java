package com.github.alexishuf.fastersparql.client.model.batch;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.*;

class EmptyBatchItTest {
    @Test
    void testPlain() {
        try (var it = new EmptyBatchIt<>(Integer.class)) {
            assertFalse(it.hasNext());
            assertThrows(NoSuchElementException.class, it::next);
        }
    }

    @ParameterizedTest @ValueSource(ints = {1, 2, 4})
    void testArrayBatch(int min) {
        try (var it = new EmptyBatchIt<>(Integer.class)) {
            assertEquals(new Batch<>(Integer.class), it.minBatch(min).nextBatch());
        }
    }

    @ParameterizedTest @ValueSource(ints = {1, 2, 4})
    void testCollectionBatch(int min) {
        try (var it = new EmptyBatchIt<>(Integer.class)) {
            var list = new ArrayList<>(List.of(23));
            assertEquals(0, it.minBatch(min).nextBatch(list));
            assertEquals(List.of(23), list);
        }
    }
}