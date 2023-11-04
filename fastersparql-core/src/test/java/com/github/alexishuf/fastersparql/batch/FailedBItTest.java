package com.github.alexishuf.fastersparql.batch;

import com.github.alexishuf.fastersparql.batch.type.TermBatchType;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.fail;

class FailedBItTest {
    @Test void test() {
        RuntimeException ex = new RuntimeException("test");
        try (var it = new FailedBIt<>(TermBatchType.TERM, ex)) {
            it.minBatch(2);
            try {
                it.nextBatch(null);
                fail("Expected exception to be thrown");
            } catch (Exception e) {
                assertSame(ex, e);
            }
        }
    }

    @Test void testWait() {
        RuntimeException ex = new RuntimeException("test");
        try (var it = new FailedBIt<>(TermBatchType.TERM, ex)) {
            it.minWait(1, TimeUnit.MINUTES).maxWait(2, TimeUnit.MINUTES);
            try {
                it.nextBatch(null);
                fail("Expected exception to be thrown");
            } catch (Exception e) {
                assertSame(ex, e);
            }
        }
    }

}