package com.github.alexishuf.fastersparql.batch;

import com.github.alexishuf.fastersparql.batch.adapters.BatchGetter;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.fail;

class FailedBItTest {
    static Stream<Arguments> getters() {
        return BatchGetter.all().stream().map(Arguments::arguments);
    }

    @ParameterizedTest @MethodSource("getters")
    void test(BatchGetter getter) {
        RuntimeException ex = new RuntimeException("test");
        try (var it = new FailedBIt<>(Integer.class, ex)) {
            it.minBatch(2);
            try {
                getter.getBatch(it);
                fail("Expected exception to be thrown");
            } catch (Exception e) {
                assertSame(ex, e);
            }
        }
    }

    @ParameterizedTest @MethodSource("getters")
    void testWait(BatchGetter getter) {
        RuntimeException ex = new RuntimeException("test");
        try (var it = new FailedBIt<>(Integer.class, ex)) {
            it.minWait(1, TimeUnit.MINUTES).maxWait(2, TimeUnit.MINUTES);
            try {
                getter.getBatch(it);
                fail("Expected exception to be thrown");
            } catch (Exception e) {
                assertSame(ex, e);
            }
        }
    }

}