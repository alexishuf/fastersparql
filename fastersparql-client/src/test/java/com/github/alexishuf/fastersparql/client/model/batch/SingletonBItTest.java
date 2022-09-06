package com.github.alexishuf.fastersparql.client.model.batch;

import com.github.alexishuf.fastersparql.client.model.batch.adapters.BatchGetter;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class SingletonBItTest {
    static Stream<Arguments> test() {
        return Stream.of(1, 2, 3).flatMap(min
                -> BatchGetter.all().stream().map(getter -> arguments(getter, min))
        );
    }

    @ParameterizedTest @MethodSource
    void test(BatchGetter getter, int min) {
        try (var it = new SingletonBIt<>(23)) {
            assertEquals(List.of(23), getter.getList(it.minBatch(min)));
        }
    }

    static Stream<Arguments> testNull() {
        return BatchGetter.all().stream().map(Arguments::arguments);
    }

    @ParameterizedTest @MethodSource
    void testNull(BatchGetter getter) {
        try (var it = new SingletonBIt<>(null, Integer.class)) {
            assertEquals(singletonList((Integer) null), getter.getList(it));
        }
    }

}