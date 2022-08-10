package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.operators.DummySparqlClient;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.operators.FasterSparqlOps.query;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class PlanHelpersTest {

    static Stream<Arguments> varsUnion() {
        DummySparqlClient<String[], ?> c = new DummySparqlClient<>(String[].class);
        LeafPlan<String[]> xy = query(c, "SELECT * WHERE {?x a ?y}").build();
        LeafPlan<String[]> x = query(c, "SELECT * WHERE {?x a <http://example.org/C>}").build();
        LeafPlan<String[]> y = query(c, "SELECT * WHERE {?y a <http://example.org/C>}").build();
        LeafPlan<String[]> x_y = query(c, "SELECT ?x WHERE {?x a ?y}").build();
        LeafPlan<String[]> yx_z = query(c, "SELECT ?y ?x WHERE {?x ?z ?y}").build();

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

    @ParameterizedTest @MethodSource("varsUnion")
    void testPublicVarsUnion(List<Plan<?>> plans, List<String> publicVars, List<String> ignored) {
        assertEquals(publicVars, PlanHelpers.publicVarsUnion(plans));
    }

    @ParameterizedTest @MethodSource("varsUnion")
    void testAllVarsUnion(List<Plan<?>> plans, List<String> ignored, List<String> allVars) {
        assertEquals(allVars, PlanHelpers.allVarsUnion(plans));
    }
}