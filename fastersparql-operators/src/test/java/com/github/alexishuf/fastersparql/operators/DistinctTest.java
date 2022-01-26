package com.github.alexishuf.fastersparql.operators;

import com.github.alexishuf.fastersparql.operators.impl.InMemoryHashDistinct;
import com.github.alexishuf.fastersparql.operators.impl.WindowHashDistinct;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import com.github.alexishuf.fastersparql.operators.providers.DistinctProvider;
import com.github.alexishuf.fastersparql.operators.row.RowOperations;
import com.github.alexishuf.fastersparql.operators.row.RowOperationsRegistry;
import org.checkerframework.checker.index.qual.NonNegative;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.operators.OperatorFlags.*;
import static com.github.alexishuf.fastersparql.operators.TestHelpers.checkRows;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class DistinctTest {
    public static Stream<Arguments> data() {
        List<DistinctProvider> providers = asList(
                new InMemoryHashDistinct.Provider(),
                new WindowHashDistinct.Provider(),
                new DistinctProvider() {
                    @Override public @NonNegative int bid(long flags) { return 0; }
                    @Override public Distinct create(long flags, RowOperations rowOperations) {
                        return new WindowHashDistinct(rowOperations, 2);
                    }
                }
        );
        List<Long> flags = Stream.of(0L, ASYNC, ALLOW_DUPLICATES, ASYNC|ALLOW_DUPLICATES)
                .flatMap(base -> Stream.of(base, LARGE_FIRST, ALL_LARGE, SMALL_FIRST, ALL_SMALL))
                .collect(Collectors.toList());
        List<List<List<String>>> inputs = asList(
                // no duplicates
                emptyList(),
                singletonList(singletonList("_:b00")),
                singletonList(asList("_:b00", "_:b01")),
                asList(singletonList("_:b00"), singletonList("_:b10")),
                asList(asList("_:x", "_:x0"), asList("_:x", "_:x1")),

                // two rows, one duplicate
                asList(singletonList("_:x"), singletonList("_:x")),
                asList(asList("_:x", "_:y"), asList("_:x", "_:y")),

                // three rows, second is duplicate
                asList(asList("_:x", "_:y"), asList("_:x", "_:y"), asList("_:x", "_:z")),
                asList(asList("_:x", "_:y"), asList("_:x", "_:y"), asList("_:z", "_:y")),

                // three rows, third is duplicate
                asList(asList("_:x", "_:y"), asList("_:x", "_:z"), asList("_:x", "_:y")),
                asList(asList("_:x", "_:y"), asList("_:z", "_:y"), asList("_:z", "_:y"))
        );
        return providers.stream()
                .flatMap(p -> flags.stream()
                        .flatMap(f -> inputs.stream().map(
                                in -> arguments(p, in, f, distinct(in)))));
    }

    private static List<List<String>> distinct(List<List<String>> in) {
        return new ArrayList<>(new LinkedHashSet<>(in));
    }

    @ParameterizedTest @MethodSource("data")
    void test(DistinctProvider provider, List<List<String>> inputs, long flags,
              List<List<String>> expected) {
        if (provider.bid(flags) == BidCosts.UNSUPPORTED)
            return; //silently skip
        List<String> expectedVars = TestHelpers.generateVars(expected);
        Distinct op = provider.create(flags, RowOperationsRegistry.get().forClass(List.class));
        Plan<List<String>> plan = op.asPlan(TestHelpers.asPlan(inputs));
        checkRows(expected, expectedVars, null, plan, true);
    }

    @ParameterizedTest @ValueSource(longs = {
            0L, ASYNC, ALLOW_DUPLICATES,
            LARGE_FIRST, ALL_LARGE, SMALL_FIRST, ALL_SMALL,
            ASYNC|LARGE_FIRST, ASYNC|ALL_LARGE, ASYNC|SMALL_FIRST, ASYNC|ALL_SMALL,
            LARGE_FIRST|ALLOW_DUPLICATES, ALL_LARGE|ALLOW_DUPLICATES, SMALL_FIRST|ALLOW_DUPLICATES, ALL_SMALL|ALLOW_DUPLICATES,
            LARGE_FIRST|ASYNC|ALLOW_DUPLICATES, ALL_LARGE|ASYNC|ALLOW_DUPLICATES, SMALL_FIRST|ASYNC|ALLOW_DUPLICATES, ALL_SMALL|ASYNC|ALLOW_DUPLICATES,
    })
    void testHasProvider(long flags) {
        Class<Distinct> opClass = Distinct.class;
        assertDoesNotThrow(() -> FasterSparqlOps.create(opClass, flags, List.class));
        assertDoesNotThrow(() -> FasterSparqlOps.create(opClass, flags, String[].class));
        assertDoesNotThrow(() -> FasterSparqlOps.create(opClass, flags, CharSequence[].class));
    }

}