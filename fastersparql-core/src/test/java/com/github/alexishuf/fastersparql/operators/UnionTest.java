package com.github.alexishuf.fastersparql.operators;

import com.github.alexishuf.fastersparql.client.model.Vars;
import com.github.alexishuf.fastersparql.client.util.VThreadTaskSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.operators.TestHelpers.asPlan;
import static com.github.alexishuf.fastersparql.operators.TestHelpers.checkRows;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;

public class UnionTest {

    record TestData(List<List<List<String>>> inputs,
                    List<Vars> inputsVars,
                    List<List<String>> expected,
                    List<String> expectedVars) { }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static TestData data(List... lists) {
        List<List<List<String>>> inputs = new ArrayList<>();
        List<Vars> inputsVars = new ArrayList<>();
        for (int i = 0; i < lists.length-2; i += 2) {
            inputs.add(lists[i]);
            inputsVars.add(Vars.from(lists[i+1]));
        }
        return new TestData(inputs, inputsVars, lists[lists.length-2], lists[lists.length-1]);
    }


    static List<TestData> testData() {
        List<TestData> list = new ArrayList<>(asList(
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
                .toList();
        List<Vars> longVars = range(0, 16).mapToObj(i -> Vars.of("x")).collect(toList());
        List<List<String>> longExpected = range(0, 16).boxed().flatMap(i -> range(0, 2048).mapToObj(r -> singletonList("_:" + i + "." + r))).collect(toList());
        list.add(new TestData(longInputs, longVars, longExpected, singletonList("x")));

        return list;
    }

//    static Stream<Arguments> test() { return testData().stream().map(Arguments::arguments); }
//    @ParameterizedTest @MethodSource
    void test(TestData d) {
        var inPlans = range(0, d.inputs.size())
                .mapToObj(i -> asPlan(d.inputsVars.get(i), d.inputs.get(i))).toList();
        var plan = FSOps.union(inPlans);
        checkRows(d.expected, d.expectedVars, null, plan, false);
    }

    @Test
    void parallelTest() throws Exception {
        try (var tasks = new VThreadTaskSet(getClass().getSimpleName())) {
            testData().forEach(d -> tasks.add(() -> test(d)));
        }
    }
}
