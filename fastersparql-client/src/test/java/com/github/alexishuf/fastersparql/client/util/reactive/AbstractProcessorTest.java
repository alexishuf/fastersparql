package com.github.alexishuf.fastersparql.client.util.reactive;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import reactor.core.publisher.Flux;

import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class AbstractProcessorTest {
    private static class Forward<T> extends AbstractProcessor<T, T> {
        public Forward(FSPublisher<? extends T> source) {
            super(source);
        }

        @Override protected void handleOnNext(T item) { emit(item); }
    }

    static Stream<Arguments> testForward() {
        return Stream.of(
                arguments(emptyList()),
                arguments(Collections.singletonList(1)),
                arguments(asList(1, 2)),
                arguments(asList(1, 2, 3)),
                arguments(IntStream.range(0, 65536).boxed().collect(toList()))
        );
    }

    @ParameterizedTest @MethodSource
    void testForward(List<Integer> list) {
        Forward<Integer> processor = new Forward<>(FSPublisher.bindToAny(Flux.fromIterable(list)));
        assertEquals(list, new IterableAdapter<>(processor).stream().collect(toList()));
    }

    static class RemoveEven extends AbstractProcessor<Integer, Integer> {
        public RemoveEven(FSPublisher<? extends Integer> source) { super(source); }

        @Override protected void handleOnNext(Integer item) {
            if (item % 2 != 0) emit(item);
        }
    }

    static Stream<Arguments> testRemoveEven() {
        return Stream.of(
                arguments(emptyList(), emptyList()),
                arguments(singletonList(1), singletonList(1)),
                arguments(asList(1, 2, 3), asList(1, 3)),
                arguments(asList(1, 2), singletonList(1)),
                arguments(asList(2, 3), singletonList(3)),
                arguments(asList(1, 2, 3, 5, 6, 7, 8, 9), asList(1, 3, 5, 7, 9)),
                arguments(asList(1, 2, 3, 5, 6, 7, 8), asList(1, 3, 5, 7))
        );
    }

    @ParameterizedTest @MethodSource
    void testRemoveEven(List<Integer> in, List<Integer> expected) {
        RemoveEven processor = new RemoveEven(FSPublisher.bindToAny(Flux.fromIterable(in)));
        assertEquals(expected, new IterableAdapter<>(processor).stream().collect(toList()));
    }

}