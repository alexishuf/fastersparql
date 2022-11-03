package com.github.alexishuf.fastersparql.operators;

import com.github.alexishuf.fastersparql.operators.plan.Modifier;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.provider.Arguments;
import org.opentest4j.AssertionFailedError;

import java.util.*;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.operators.TestHelpers.*;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class DistinctTest {
    @FunctionalInterface
    interface Factory<R, I> {
        Modifier<R, I> create(Plan<R, I> in);
    }

    public static Stream<Arguments> data() {
        List<Factory<List<String>, String>> factories = List.of(
                FSOps::distinct,
                FSOps::reduced,
                i -> FSOps.distinct(i, 2)
        );
        List<List<List<String>>> inputs = List.of(
                // no duplicates
                emptyList(),
                singletonList(singletonList("_:b00")),
                singletonList(List.of("_:b00", "_:b01")),
                List.of(singletonList("_:b00"), singletonList("_:b10")),
                List.of(List.of("_:x", "_:x0"), List.of("_:x", "_:x1")),

                // two rows, one duplicate
                List.of(singletonList("_:x"), singletonList("_:x")),
                List.of(List.of("_:x", "_:y"), List.of("_:x", "_:y")),

                // three rows, second is duplicate
                List.of(List.of("_:x", "_:y"), List.of("_:x", "_:y"), List.of("_:x", "_:z")),
                List.of(List.of("_:x", "_:y"), List.of("_:x", "_:y"), List.of("_:z", "_:y")),

                // three rows, third is duplicate
                List.of(List.of("_:x", "_:y"), List.of("_:x", "_:z"), List.of("_:x", "_:y")),
                List.of(List.of("_:x", "_:y"), List.of("_:z", "_:y"), List.of("_:z", "_:y"))
        );
        List<Arguments> list = new ArrayList<>();
        for (var factory : factories) {
            for (List<List<String>> input : inputs) {
                list.add(arguments(factory, input, distinct(input)));
            }
        }
        return list.stream();
    }

    private static List<List<String>> distinct(List<List<String>> in) {
        return new ArrayList<>(new LinkedHashSet<>(in));
    }

//    @ParameterizedTest @MethodSource("data")
    private void test(Factory<List<String>, String> factory, List<List<String>> inputs,
                      List<List<String>> expected) {
        List<String> expectedVars = generateVars(expected);
        var distinct = factory.create(asPlan(inputs));
        checkRows(expected, expectedVars, null, distinct, true);
    }
    @Test
    void parallelTest() {
        List<AssertionFailedError> errors = data().parallel().map(Arguments::get).map(a -> {
            //noinspection unchecked
            Factory<List<String>, String> fac = (Factory<List<String>, String>) a[0];
            @SuppressWarnings("unchecked") List<List<String>> inputs = (List<List<String>>) a[1];
            @SuppressWarnings("unchecked") List<List<String>> expected = (List<List<String>>) a[2];
            try {
                test(fac, inputs, expected);
                return null;
            } catch (Throwable t) {
                return new AssertionFailedError("Test Failed for " + Arrays.toString(a), t);
            }
        }).filter(Objects::nonNull).toList();
        if (!errors.isEmpty())
            fail(errors.get(0));
    }
}