package com.github.alexishuf.fastersparql.operators.reorder;

import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.operators.DummySparqlClient;
import com.github.alexishuf.fastersparql.operators.plan.LeafPlan;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class AvoidCartesianJoinReorderStrategyTest {
    private static final SparqlClient<String[], byte[]> client = new DummySparqlClient<>();

    private Plan<String[]> asPlan(String sparql) {
        return LeafPlan.builder(client, sparql).build();
    }

    @SuppressWarnings("unused") static Stream<Arguments> test() {
        String x = "SELECT ?x WHERE {?s ?p ?x}";
        String z = "SELECT ?z WHERE {?s ?p ?z}";
        String y = "SELECT ?y WHERE {?s ?p ?y}";
        String w = "SELECT ?w WHERE {?s ?p ?w}";
        String xy = "SELECT ?x ?y WHERE {?s ?x ?y}";
        String yz = "SELECT ?y ?z WHERE {?s ?y ?z}";
        String x_y = "SELECT ?x WHERE {?s ?x ?y}";
        String _xy = "SELECT ?y WHERE {?s ?x ?y}";

        return Stream.of(
                arguments(emptyList(), false, emptyList()),
                arguments(singletonList(x), false, singletonList(0)),
                arguments(asList(x, xy), false, asList(0, 1)),
                arguments(asList(xy, x), false, asList(0, 1)),
                arguments(asList(x, z), false, asList(0, 1)),
                arguments(asList(x, _xy), false, asList(0, 1)),
                arguments(asList(x, _xy), true, asList(0, 1)),
                arguments(asList(_xy, x), false, asList(0, 1)),
                arguments(asList(x, xy, yz, z), false, asList(0, 1, 2, 3)),
                arguments(asList(x, xy, yz, z), true, asList(0, 1, 2, 3)),
                // invert first pair
                arguments(asList(_xy, x), true, asList(1, 0)),
                arguments(asList(_xy, x, xy), true, asList(1, 0, 2)),
                arguments(asList(_xy, x, yz), true, asList(1, 0, 2)),
                // major reordering
                arguments(asList(z, x, y, xy, yz), false, asList(0, 4, 2, 3, 1)),
                arguments(asList(z, x, y, x_y, yz), true, asList(0, 4, 2, 3, 1)),
                // major reordering plus first cannot be first
                arguments(asList(x_y, w, y, x_y, yz), true, asList(2, 0, 3, 4, 1))
        );
    }

    @ParameterizedTest @MethodSource
    void test(List<String> operands, boolean useBind, List<Integer> expected) {
        AvoidCartesianJoinReorderStrategy s = AvoidCartesianJoinReorderStrategy.INSTANCE;
        List<Plan<String[]>> plans = operands.stream().map(this::asPlan).collect(toList());
        List<Plan<String[]>> plansCopy = new ArrayList<>(plans);
        List<Plan<String[]>> exPlans = expected.stream().map(plans::get).collect(toList());
        List<Plan<String[]>> reorder = s.reorder(plans, useBind);
        assertEquals(exPlans, reorder);
        assertEquals(plansCopy, plans);
    }
}