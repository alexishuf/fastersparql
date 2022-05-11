package com.github.alexishuf.fastersparql.operators.impl;

import com.github.alexishuf.fastersparql.client.BindType;
import com.github.alexishuf.fastersparql.client.model.row.RowOperations;
import com.github.alexishuf.fastersparql.client.model.row.impl.StringArrayOperations;
import com.github.alexishuf.fastersparql.operators.DummySparqlClient;
import com.github.alexishuf.fastersparql.operators.plan.LeafPlan;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class PlanMergerTest {
    private static final DummySparqlClient<String[], byte[]> client = new DummySparqlClient<>();

    @SuppressWarnings("unused") static Stream<Arguments> testBind() {
        return Stream.of(
                // use left[0] without changing right projection
        /* 1 */ asList("SELECT ?x WHERE {?x ?p ?in}", "in,y", "_:l0,_:l1",
                       "SELECT ?x WHERE {?x ?p _:l0}"),
                // use left[1] without changing right projection
        /* 2 */ asList("SELECT ?x WHERE {?x ?p ?in}", "y,in", "_:l0,_:l1",
                       "SELECT ?x WHERE {?x ?p _:l1}"),
                // use left[1] changing right projection from ?x ?in to ?x
        /* 3 */ asList("SELECT * WHERE {?x :p ?in}", "y,in", "_:l0,_:l1",
                       "SELECT * WHERE {?x :p _:l1}"),
                // use left[1] changing right projection from ?in ?x to ?x
        /* 4 */ asList("SELECT * WHERE {?in :p ?x}", "y,in", "_:l0,_:l1",
                       "SELECT * WHERE {_:l1 :p ?x}"),
                // use left[1] changing explicit right projection from ?in ?x to ?x
        /* 5 */ asList("SELECT ?in ?x WHERE {?x :p ?in}", "y,in", "_:l0,_:l1",
                       "SELECT  ?x WHERE {?x :p _:l1}")
        ).map(l -> arguments(
                l.get(0), // rightSparql
                asList(l.get(1).split(",")), //leftVars
                l.get(2).split(","), //leftRow
                l.get(3) //expectedSparql
        ));
    }

    @ParameterizedTest @MethodSource
    void testBind(String rightSparql, List<String> leftVars, String[] leftRow,
                  String expectedSparql) {
        LeafPlan<String[]> right = LeafPlan.builder(client, rightSparql).build();
        RowOperations rowOps = StringArrayOperations.get();
        PlanMerger<String[]> merger = new PlanMerger<>(rowOps, leftVars, right, BindType.JOIN);
        Plan<String[]> bound = merger.bind(leftRow);
        assertEquals(expectedSparql, ((LeafPlan<String[]>)bound).query().toString());
    }
}