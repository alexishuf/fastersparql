package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.client.model.row.types.ArrayRow;
import com.github.alexishuf.fastersparql.operators.DummySparqlClient;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.operators.FSOps.query;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class PlanTest {

    static Stream<Arguments> varsUnion() {
        DummySparqlClient<String[], String, ?> c = new DummySparqlClient<>(ArrayRow.STRING);
        var xy = query(c, "SELECT * WHERE {?x a ?y}");
        var x = query(c, "SELECT * WHERE {?x a <http://example.org/C>}");
        var y = query(c, "SELECT * WHERE {?y a <http://example.org/C>}");
        var x_y = query(c, "SELECT ?x WHERE {?x a ?y}");
        var yx_z = query(c, "SELECT ?y ?x WHERE {?x ?z ?y}");

        return Stream.of(
        /*  1 */arguments(emptyList(), emptyList(), emptyList()),

        /*  2 */arguments(singletonList(x), singletonList("x"), singletonList("x")),
        /*  3 */arguments(singletonList(xy), asList("x", "y"), asList("x", "y")),
        /*  4 */arguments(singletonList(x_y), singletonList("x"), asList("x", "y")),
        /*  5 */arguments(singletonList(yx_z), asList("y", "x"), asList("y", "x", "z")),

        /*  6 */arguments(asList(x, xy), asList("x", "y"), asList("x", "y")),
        /*  7 */arguments(asList(y, xy), asList("y", "x"), asList("y", "x")),

        /*  8 */arguments(asList(x, x_y), singletonList("x"), asList("x", "y")),
        /*  9 */arguments(asList(y, x_y), asList("y", "x"), asList("y", "x")),
        /* 10 */arguments(asList(yx_z, x, y), asList("y", "x"), asList("y", "x", "z"))
        );
    }

    private static final class HelperPlan extends Plan<String[], String> {
        public HelperPlan(List<? extends Plan<String[], String>> operands) {
            super(ArrayRow.STRING, operands, null, null);
        }

        @Override public BIt<String[]> execute(boolean canDedup) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Plan<String[], String> with(List<? extends Plan<String[], String>> replacement,
                                           @Nullable Plan<String[], String> unbound,
                                           @Nullable String name) {
            throw new UnsupportedOperationException();
        }
    }

    @ParameterizedTest @MethodSource("varsUnion")
    void testPublicVarsUnion(List<Plan<String[], String>> plans, List<String> publicVars, List<String> ignored) {
        assertEquals(publicVars, new HelperPlan(plans).publicVars());
    }

    @ParameterizedTest @MethodSource("varsUnion")
    void testAllVarsUnion(List<Plan<String[], String>> plans, List<String> ignored, List<String> allVars) {
        assertEquals(allVars, new HelperPlan(plans).allVars());
    }
}