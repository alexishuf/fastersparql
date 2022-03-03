package com.github.alexishuf.fastersparql.operators;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.util.reactive.IterablePublisher;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import lombok.AllArgsConstructor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.provider.Arguments;
import org.opentest4j.AssertionFailedError;

import java.util.*;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.operators.OperatorFlags.*;
import static com.github.alexishuf.fastersparql.operators.TestHelpers.checkRows;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;

public class UnionTest {

    @AllArgsConstructor
    private static final class TestData {
        List<List<List<String>>> inputs;
        List<List<String>> inputsVars;
        long flags;
        List<List<String>> expected;
        List<String> expectedVars;

        TestData withFlags(long flags) {
            return new TestData(inputs, inputsVars, flags, expected, expectedVars);
        }

        Arguments asArguments() {
            return Arguments.arguments(inputs, inputsVars, flags, expected, expectedVars);
        }
    }
    @SuppressWarnings({"rawtypes", "unchecked"})
    private static TestData data(List... lists) {
        List<List<List<String>>> inputs = new ArrayList<>();
        List<List<String>> inputsVars = new ArrayList<>();
        for (int i = 0; i < lists.length-2; i += 2) {
            inputs.add(lists[i]);
            inputsVars.add(lists[i+1]);
        }
        return new TestData(inputs, inputsVars, 0L,
                            lists[lists.length-2], lists[lists.length-1]);
    }


    static Stream<Arguments> test() {
        List<TestData> base = new ArrayList<>(asList(
                //empty inputs, test union of var names
        /*  1 */data(emptyList(), singletonList("x"), emptyList(), singletonList("x")),
        /*  2 */data(emptyList(), singletonList("x"),
                        emptyList(), asList("x", "y"),
                        emptyList(), asList("x", "y")),
        /*  3 */data(emptyList(), singletonList("x"),
                        emptyList(), asList("x", "y"),
                        emptyList(), asList("y", "z"),
                        emptyList(), asList("x", "y", "z")),
                // two inputs with single row, overlapping var names
        /*  4 */data(singletonList(singletonList("_:x")), singletonList("x"),
                     singletonList(asList("_:y", "_:z")), asList("x", "y"),
                     asList(asList("_:x", null), asList("_:y", "_:z")), asList("x", "y")),
                //two inputs with two rows, non-overlapping var names
        /*  5 */data(asList(asList("_:a", "_:b"), asList(null, "_:c")), asList("x", "y"),
                     asList(singletonList("_:d"), singletonList(null)), singletonList("z"),
                     asList(asList("_:a", "_:b", null),
                            asList(null, "_:c", null),
                            asList(null, null, "_:d"),
                            asList(null, null, null)),
                     asList("x", "y", "z")),
                //four inputs with two rows, all with the same single var
        /*  6 */data(asList(singletonList("_:a"), singletonList("_:b")), singletonList("x"),
                     asList(singletonList("_:c"), singletonList("_:d")), singletonList("x"),
                     asList(singletonList("_:e"), singletonList("_:f")), singletonList("x"),
                     asList(singletonList("_:g"), singletonList("_:h")), singletonList("x"),
                     Stream.of("a", "b", "c", "d", "e", "f", "g", "h")
                             .map(s -> singletonList("_:"+s)).collect(toList()),
                     singletonList("x"))
        ));

        List<List<List<String>>> longInputs = range(0, 16)
                .mapToObj(i -> range(0, 2048).mapToObj(r -> singletonList("_:" + i + "." + r))
                                             .collect(toList()))
                .collect(toList());
        List<List<String>> longVars = range(0, 16).mapToObj(i -> singletonList("x")).collect(toList());
        List<List<String>> longExpected = range(0, 16).boxed().flatMap(i -> range(0, 2048).mapToObj(r -> singletonList("_:" + i + "." + r))).collect(toList());
        base.add(new TestData(longInputs, longVars, 0L, longExpected, singletonList("x")));

        return Stream.of(0L, ASYNC)
                .flatMap(async -> Stream.of(ALL_LARGE, ALL_SMALL, 0L).map(size -> async|size))
                .flatMap(flags -> base.stream().map(d -> d.withFlags(flags)))
                .map(TestData::asArguments);
    }

//    @ParameterizedTest @MethodSource
    private void test(List<List<List<String>>> inputs, List<List<String>> varsLists,
                     long flags,
                     List<List<String>> expected, List<String> expectedVars) {
        Union op = FasterSparqlOps.create(Union.class, flags, List.class);
        List<Plan<List<String>>> inPlans = new ArrayList<>();
        for (int i = 0; i < inputs.size(); i++) {
            List<List<String>> rows = inputs.get(i);
            List<String> vars = varsLists.get(i);
            inPlans.add(new Plan<List<String>>() {
                @Override public String name() { return "test"; }
                @Override public Class<? super List<String>> rowClass() { return List.class; }
                @Override public List<String> publicVars() { return vars; }
                @Override public List<String> allVars() { return vars; }
                @Override public Results<List<String>> execute() {
                    return new Results<>(vars, List.class, new IterablePublisher<>(rows));
                }
                @Override public Plan<List<String>> bind(Map<String, String> var2ntValue) {
                    throw new UnsupportedOperationException();
                }
            });
        }
        Plan<List<String>> plan = op.asPlan(inPlans);
        checkRows(expected, expectedVars, null, plan, false);
    }
    @Test @SuppressWarnings("unchecked")
    void parallelTest() {
        List<AssertionFailedError> errors = test().parallel().map(Arguments::get).map(a -> {
            List<List<List<String>>> inputs = (List<List<List<String>>>) a[0];
            List<List<String>> varsLists = (List<List<String>>) a[1];
            long flags = (long) a[2];
            List<List<String>> expected = (List<List<String>>) a[3];
            List<String> expectedVars = (List<String>) a[4];
            try {
                test(inputs, varsLists, flags, expected, expectedVars);
                return null;
            } catch (Throwable t) {
                return new AssertionFailedError("Failed for test case " + Arrays.toString(a), t);
            }
        }).filter(Objects::nonNull).collect(toList());
        if (!errors.isEmpty())
            throw errors.get(0);
    }

}
