package com.github.alexishuf.fastersparql.operators;

import com.github.alexishuf.fastersparql.client.model.Results;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.operators.OperatorFlags.ALL_LARGE;
import static com.github.alexishuf.fastersparql.operators.OperatorFlags.ASYNC;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class SliceTest {
    static Stream<Arguments> test() {
        List<List<String>> base = asList(
                asList("_:1", "_:b1"),
                asList("_:2", "_:b2"),
                asList("_:3", "_:b3"),
                asList("_:4", "_:b4"),
                asList("_:5", "_:b5")
        );
        return Stream.of(
                arguments(emptyList(), 0, 0, emptyList()),
                arguments(emptyList(), 0, 1024, emptyList()),
                arguments(emptyList(), 512, 1024, emptyList()),
                arguments(base, 0, 1024, base),
                arguments(base, 0, 5, base),
                arguments(base, 0, 4, base.subList(0, 4)),
                arguments(base, 0, 3, base.subList(0, 3)),
                arguments(base, 0, 1, base.subList(0, 1)),
                arguments(base, 1, 0, emptyList()),
                arguments(base, 1, 1, base.subList(1, 2)),
                arguments(base, 1, 2, base.subList(1, 3)),
                arguments(base, 1, 4, base.subList(1, 5)),
                arguments(base, 1, 5, base.subList(1, 5))
        );
    }

    @ParameterizedTest @MethodSource
    void test(List<List<String>> in, int offset, int limit, List<List<String>> expected) {
        for (long flags : asList(0L, ASYNC, ASYNC|ALL_LARGE)) {
            Slice op = FasterSparqlOps.create(Slice.class, flags, List.class);
            Results<List<String>> out = op.checkedRun(TestHelpers.asPlan(in), offset, limit);
            TestHelpers.assertExpectedRows(expected, null, out, true);
        }
    }
}