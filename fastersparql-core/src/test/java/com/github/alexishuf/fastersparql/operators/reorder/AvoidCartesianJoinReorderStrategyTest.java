package com.github.alexishuf.fastersparql.operators.reorder;

import com.github.alexishuf.fastersparql.FS;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.client.DummySparqlClient.DUMMY;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class AvoidCartesianJoinReorderStrategyTest {
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
                arguments(asList(x, _xy), asList(0, 1)),
                arguments(asList(x, xy, yz, z), asList(0, 1, 2, 3)),
                // invert first pair
                arguments(asList(_xy, x), asList(1, 0)),
                arguments(asList(_xy, x, xy), asList(1, 0, 2)),
                arguments(asList(_xy, x, yz), asList(1, 0, 2)),
                // major reordering
                arguments(asList(z, x, y, x_y, yz), asList(0, 4, 2, 1, 3)),
                // major reordering plus first cannot be first
                arguments(asList(x_y, w, y, x_y, yz), asList(2, 0, 3, 1, 4))
        );
    }

    @ParameterizedTest @MethodSource
    void test(List<String> operands, List<Integer> expected) {
        var s = AvoidCartesianJoinReorderStrategy.INSTANCE;
        var plans = operands.stream().map(sparql -> FS.query(DUMMY, sparql)).toArray(Plan[]::new);
        Vars expectedProjection = expected.equals(IntStream.range(0, plans.length).boxed().toList())
                                ? null : Plan.publicVars(plans);
        var expectedOrder = expected.stream().map(i -> plans[i]).toArray(Plan[]::new);
        assertEquals(expectedProjection, s.reorder(plans));
        assertArrayEquals(expectedOrder, plans);
    }
}