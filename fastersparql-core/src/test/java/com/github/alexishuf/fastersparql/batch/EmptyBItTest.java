package com.github.alexishuf.fastersparql.batch;

import com.github.alexishuf.fastersparql.batch.adapters.BatchGetter;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class EmptyBItTest {
    static Stream<Arguments> test() {
        return Stream.of(1, 2, 3).flatMap(min
                -> BatchGetter.all().stream().map(getter -> arguments(getter, min))
        );
    }

    @ParameterizedTest @MethodSource
    void test(BatchGetter getter, int min) {
        try (var it = new EmptyBIt<>(Integer.class)) {
            assertEquals(List.of(), getter.getList(it.minBatch(min)));
        }
    }

    @ParameterizedTest() @MethodSource("test")
    void testMinWait(BatchGetter getter, int minBatch) {
        try (var it = new EmptyBIt<>(Integer.class)) {
            it.minBatch(minBatch).minWait(2, MINUTES);
            assertEquals(List.of(), getter.getList(it));
        }
    }

    @ParameterizedTest() @MethodSource("test")
    void testMaxWait(BatchGetter getter, int minBatch) {
        try (var it = new EmptyBIt<>(Integer.class)) {
            it.minBatch(minBatch).minWait(1, MINUTES).maxWait(2, MINUTES);
            assertEquals(List.of(), getter.getList(it));
        }
    }
}