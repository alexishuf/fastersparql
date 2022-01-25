package com.github.alexishuf.fastersparql.client.util;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class IterablePublisherTest {
    static Stream<Arguments> test() {
        return Stream.of(
                arguments(Collections.emptyList()),
                arguments(Collections.singletonList(23)),
                arguments(Arrays.asList(1, 2)),
                arguments(Arrays.asList(1, 2, 3)),
                arguments(Arrays.asList(1, 2, 3, 4)),
                arguments(IntStream.range(0, 65536).boxed().collect(Collectors.toList()))
        );
    }

    @ParameterizedTest @MethodSource
    void test(List<Integer> list) {
        IterablePublisher<Integer> pub = new IterablePublisher<>(list);
        assertEquals(new IterableAdapter<>(pub).stream().collect(Collectors.toList()), list);
        // subscribe twice
        assertEquals(new IterableAdapter<>(pub).stream().collect(Collectors.toList()), list);
    }

}