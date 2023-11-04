package com.github.alexishuf.fastersparql.batch;


import com.github.alexishuf.fastersparql.batch.type.TermBatchType;
import com.github.alexishuf.fastersparql.model.Vars;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.jupiter.api.Assertions.assertNull;

class EmptyBItTest {
    @ParameterizedTest @ValueSource(ints = {1, 2, 3})
    void test(int min) {
        try (var it = new EmptyBIt<>(TermBatchType.TERM, Vars.of("x"))) {
            assertNull(it.minBatch(min).nextBatch(null));
        }
    }

    @ParameterizedTest @ValueSource(ints = {1, 2, 3})
    void testMinWait(int minBatch) {
        try (var it = new EmptyBIt<>(TermBatchType.TERM, Vars.of("x"))) {
            it.minBatch(minBatch).minWait(2, MINUTES);
            assertNull(it.nextBatch(null));
        }
    }

    @ParameterizedTest @ValueSource(ints = {1, 2, 3})
    void testMaxWait(int minBatch) {
        try (var it = new EmptyBIt<>(TermBatchType.TERM, Vars.of("x"))) {
            it.minBatch(minBatch).minWait(1, MINUTES).maxWait(2, MINUTES);
            assertNull(it.nextBatch(null));
        }
    }
}