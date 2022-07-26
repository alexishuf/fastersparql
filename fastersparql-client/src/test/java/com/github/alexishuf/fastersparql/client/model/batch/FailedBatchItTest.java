package com.github.alexishuf.fastersparql.client.model.batch;

import com.github.alexishuf.fastersparql.client.util.async.RuntimeExecutionException;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FailedBatchItTest {
    @Test
    void testPlain() {
        RuntimeException ex = new RuntimeException("test");
        try (var it = new FailedBatchIt<>(Integer.class, ex)) {
            assertTrue(it.hasNext());
            boolean caught = false;
            try {
                it.next();
            } catch (RuntimeExecutionException e) {
                assertSame(ex, e.getCause());
                caught = true;
            }
            assertTrue(caught);
        }
    }

    @Test
    void testArray() {
        var ex = new RuntimeException("test");
        try (var it = new FailedBatchIt<>(Integer.class, ex)) {
            it.minWait(Duration.ofSeconds(30)).minBatch(2);
            long start = System.nanoTime();
            boolean caught = false;
            try {
                it.nextBatch();
            } catch (RuntimeExecutionException e) {
                assertSame(ex, e.getCause());
                caught = true;
            }
            assertTrue(caught);
            assertTrue(System.nanoTime()-start < Duration.ofSeconds(1).toNanos());
        }
    }

    @Test
    void testCollection() {
        var ex = new RuntimeException("test");
        try (var it = new FailedBatchIt<>(Integer.class, ex)) {
            it.minBatch(3).minWait(Duration.ofSeconds(30));
            long start = System.nanoTime();
            boolean caught = false;
            var list = new ArrayList<>(List.of(23));
            try {
                it.nextBatch(list);
            } catch (RuntimeExecutionException e) {
                assertSame(ex, e.getCause());
                caught = true;
            }
            assertTrue(caught);
            assertTrue(System.nanoTime()-start < Duration.ofSeconds(1).toNanos());
        }
    }


}