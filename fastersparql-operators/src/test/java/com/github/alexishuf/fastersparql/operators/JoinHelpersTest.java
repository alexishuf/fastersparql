package com.github.alexishuf.fastersparql.operators;

import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.operators.plan.LeafPlan;
import com.github.alexishuf.fastersparql.operators.reorder.AvoidCartesianJoinReorderStrategy;
import com.github.alexishuf.fastersparql.operators.reorder.JoinReorderStrategy;
import com.github.alexishuf.fastersparql.operators.reorder.NullJoinReorderStrategy;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Collection;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class JoinHelpersTest {
    private static final SparqlClient<String[], byte[]> client = new DummySparqlClient<>();

    @SuppressWarnings("unused") static Stream<Arguments> testIsProduct() {
        return Stream.of(
        /*  1 */arguments(singleton("x"), "SELECT ?x ?y { ?x ?p ?y}", false, false),
        /*  2 */arguments(singletonList("x"), "SELECT ?x ?y { ?x ?p ?y}", false, false),
        /*  3 */arguments(singleton("x"), "SELECT ?y { ?x ?p ?y}", false, true),
        /*  4 */arguments(singleton("x"), "SELECT ?y { ?x ?p ?y}", true, false),
        /*  5 */arguments(emptySet(), "SELECT ?y { ?x ?p ?y}", false, true),
        /*  6 */arguments(emptySet(), "SELECT ?y { ?x ?p ?y}", true, true),
        /*  7 */arguments(asList("x", "y"), "SELECT * {?s ?p ?o}", false, true),
        /*  8 */arguments(asList("x", "y"), "SELECT * {?s ?p ?o}", true, true),
        /*  9 */arguments(asList("x", "o"), "SELECT ?s {?s ?p ?o}", false, true),
        /* 10 */arguments(asList("x", "o"), "SELECT ?s {?s ?p ?o}", true, false),
        /* 11 */arguments(asList("o", "x"), "SELECT ?s {?s ?p ?o}", true, false)
        );
    }

    @ParameterizedTest @MethodSource
    public void testIsProduct(Collection<String> acc, String rightSparql, boolean useBind,
                              boolean expected) {
        LeafPlan<String[]> right = LeafPlan.builder(client, rightSparql).build();
        assertEquals(expected, JoinHelpers.isProduct(acc, right, useBind));
    }

    @SuppressWarnings("unused") static Stream<Arguments> testLoadStrategy() {
        Class<?> n = NullJoinReorderStrategy.class;
        Class<?> a = AvoidCartesianJoinReorderStrategy.class;
        return Stream.of(
        /*  1 */arguments("null", n),
        /*  2 */arguments("null ", n),
        /*  3 */arguments(" null ", n),
        /*  4 */arguments("Null", n),
        /*  5 */arguments("Null ", n),
        /*  6 */arguments("\tNull\n", n),
        /*  7 */arguments("NullJoinReorderStrategy", n),
        /*  8 */arguments("NullJoinReorderStrategy\n", n),
        /*  9 */arguments("\tNullJoinReorderStrategy\n", n),
        /* 10 */arguments("\tnullJoinReorderStrategy\n", n),
        /* 11 */arguments(n.getName(), n),
        /* 12 */arguments(n.getName().toLowerCase(), n),
        /* 13 */arguments(n.getName().toUpperCase(), n),
        /* 14 */arguments("reorder.NullJoinReorderStrategy", n),
        /* 15 */arguments("reorder.NullJoinReorderStrategy\n", n),
        /* 16 */arguments(" reorder.NullJoinReorderStrategy\n", n),
        /* 17 */arguments("reorder.null", n),
        /* 18 */arguments("reorder.null\n", n),
        /* 19 */arguments(" reorder.null\n", n),
        /* 20 */arguments("AvoidCartesian", a),
        /* 21 */arguments("AvoidCartesian\n", a),
        /* 22 */arguments("AvoidCartesianJoinReorderStrategy\n", a),
        /* 23 */arguments("\toperators.reorder.AvoidCartesianJoinReorderStrategy\n", a),
        /* 24 */arguments("bullshit", null),
        /* 25 */arguments("com.github.alexishuf.fastersparql.operators.reorder", null),
        /* 26 */arguments("com.github.alexishuf.fastersparql.operators.reorder.", null),
        /* 27 */arguments("com.github.alexishuf.fastersparql.operators.reorder.*", null)
        );
    }

    @ParameterizedTest @MethodSource
    public void testLoadStrategy(String name, Class<? extends JoinReorderStrategy> cls) {
        JoinReorderStrategy strategy = JoinHelpers.loadStrategy(name);
        if (cls == null) {
            assertNull(strategy);
        } else {
            assertNotNull(strategy);
            assertEquals(cls, strategy.getClass());
        }
    }
}