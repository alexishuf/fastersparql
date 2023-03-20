package com.github.alexishuf.fastersparql.batch;

import com.github.alexishuf.fastersparql.batch.adapters.BItDrainer;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.batch.IntsBatch.ints;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public class BItGeneratorTest {

    static Stream<Arguments> test() {
        List<Arguments> list = new ArrayList<>();
        for (var gen : BItGenerator.GENERATORS) {
            for (Integer minBatch : List.of(1, 2)) {
                for (Number minWait : List.of(0, 1_000_000_000L)) {
                    for (var range : List.of(List.of(0, 1), List.of(0, 2), List.of(1, 5),
                                             List.of(0, 0), List.of(5, 5))) {
                        int begin = range.get(0), end = range.get(1);
                        list.add(arguments(gen, begin, end-begin, minBatch, minWait));
                    }
                }
            }
        }
        return list.stream();
    }

    @ParameterizedTest @MethodSource
    void test(BItGenerator generator, int begin, int size, int minBatch, long minWait) {
        try (var it = generator.asBIt(i -> i.minBatch(minBatch).minWait(minWait, NANOSECONDS),
                                      ints(begin, size))) {
            int[] expected = ints(begin, size);
            BItDrainer.RECYCLING.drainOrdered(it, expected, null);
        }

    }

}
