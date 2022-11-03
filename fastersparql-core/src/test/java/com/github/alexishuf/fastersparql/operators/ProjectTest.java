package com.github.alexishuf.fastersparql.operators;

import com.github.alexishuf.fastersparql.client.model.Vars;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.operators.TestHelpers.asPlan;
import static com.github.alexishuf.fastersparql.operators.TestHelpers.checkRows;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public class ProjectTest {

    static Stream<Arguments> test() {
        List<List<String>> base = asList(
                asList("_:b00", "_:b01", "_:b02"),
                asList("_:b10", "_:b11", "_:b12")
        );
        return Stream.of(
        /*  1 */arguments(emptyList(), Vars.of("x"), emptyList()),
        /*  2 */arguments(emptyList(), Vars.of("x", "y"), emptyList()),
        /*  3 */arguments(singletonList(singletonList("<a>")), Vars.of("x0"),
                          singletonList(singletonList("<a>"))),
        /*  4 */arguments(base.subList(0, 1), Vars.of("x0"),
                          singletonList(singletonList("_:b00"))),
        /*  5 */arguments(base.subList(0, 1), Vars.of("x2"),
                          singletonList(singletonList("_:b02"))),
        /*  6 */arguments(base.subList(0, 1), Vars.of("x0", "x1"),
                          singletonList(asList("_:b00", "_:b01"))),
        /*  7 */arguments(base.subList(0, 1), Vars.of("x0", "y"),
                          singletonList(asList("_:b00", null))),
        /*  8 */arguments(base.subList(0, 1), Vars.of("y", "x2"),
                          singletonList(asList(null, "_:b02"))),
        /*  9 */arguments(base.subList(0, 1), Vars.of("x1", "y", "x2"),
                        singletonList(asList("_:b01", null, "_:b02"))),
        /* 10 */arguments(base.subList(0, 2), Vars.of("x0"),
                          asList(singletonList("_:b00"), singletonList("_:b10"))),
        /* 11 */arguments(base.subList(0, 2), Vars.of("x1", "y", "x0"),
                          asList(asList("_:b01", null, "_:b00"), asList("_:b11", null, "_:b10")))
        );
    }

    @ParameterizedTest @MethodSource
    void test(List<List<String>> in, Vars vars, List<List<String>> expected) {
        var plan = FSOps.project(asPlan(in), vars);
        checkRows(expected, vars, null, plan, true);
    }

}