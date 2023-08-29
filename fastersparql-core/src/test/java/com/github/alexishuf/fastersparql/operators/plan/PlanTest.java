package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.FS;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.sparql.OpaqueSparqlQuery;
import com.github.alexishuf.fastersparql.sparql.SparqlQuery;
import com.github.alexishuf.fastersparql.util.Results;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.FS.query;
import static com.github.alexishuf.fastersparql.client.DummySparqlClient.DUMMY;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class PlanTest {

    static Stream<Arguments> varsUnion() {

        var xy = query(DUMMY, "SELECT * WHERE {?x a ?y}");
        var x = query(DUMMY, "SELECT * WHERE {?x a <http://example.org/C>}");
        var y = query(DUMMY, "SELECT * WHERE {?y a <http://example.org/C>}");
        var x_y = query(DUMMY, "SELECT ?x WHERE {?x a ?y}");
        var yx_z = query(DUMMY, "SELECT ?y ?x WHERE {?x ?z ?y}");

        assertEquals(Vars.of("x"), x_y.publicVars());
        assertEquals(Vars.of("x", "y"), x_y.allVars());
        assertEquals(Vars.of("y", "x"), yx_z.publicVars());
        assertEquals(Vars.of("y", "x", "z"), yx_z.allVars());

        return Stream.of(
                arguments(singletonList(x), Vars.of("x"), Vars.of("x")),
                arguments(singletonList(xy), Vars.of("x", "y"), Vars.of("x", "y")),
                arguments(singletonList(x_y), Vars.of("x"), Vars.of("x", "y")),
                arguments(singletonList(yx_z), Vars.of("y", "x"), Vars.of("y", "x", "z")),

                arguments(asList(x, xy), Vars.of("x", "y"), Vars.of("x", "y")),
                arguments(asList(y, xy), Vars.of("y", "x"), Vars.of("y", "x")),

                arguments(asList(x, x_y), Vars.of("x"), Vars.of("x", "y")),
                arguments(asList(y, x_y), Vars.of("y", "x"), Vars.of("y", "x")),
                arguments(asList(yx_z, x, y), Vars.of("y", "x"), Vars.of("y", "x", "z"))
        );
    }

    @ParameterizedTest @MethodSource("varsUnion")
    void testPublicVarsUnion(List<Plan> plans, Vars expected, Vars ignored) {
        assertEquals(expected, FS.union(plans.toArray(Plan[]::new)).publicVars());
    }

    @ParameterizedTest @MethodSource("varsUnion")
    void testAllVarsUnion(List<Plan> plans, Vars ignored, Vars expected) {
        assertEquals(expected, FS.union(plans.toArray(Plan[]::new)).allVars());
    }

    @Test void testToAsk() {
        SparqlQuery s0 = new OpaqueSparqlQuery("SELECT * WHERE { ?x a ?y }");
        SparqlQuery s1 = new OpaqueSparqlQuery("SELECT ?x WHERE { ?x a ?y }");
        SparqlQuery s2 = new OpaqueSparqlQuery("SELECT ?y WHERE { ?x a ?y } LIMIT 10");

        assertEquals(new OpaqueSparqlQuery("ASK WHERE { ?x a ?y }"), s0.toAsk());
        assertEquals(new OpaqueSparqlQuery("ASK WHERE { ?x a ?y }"), s1.toAsk());
        assertEquals(new OpaqueSparqlQuery("ASK WHERE { ?x a ?y }"), s2.toAsk());

        Query q0 = query("SELECT * WHERE { ?x a ?y }");
        Query q1 = query("SELECT ?x WHERE { ?x a ?y }");
        Query q2 = query("SELECT ?y WHERE { ?x a ?y } LIMIT 10");

        assertEquals(query("ASK WHERE { ?x a ?y }"), q0.toAsk());
        assertEquals(query("ASK WHERE { ?x a ?y }"), q1.toAsk());
        assertEquals(query("ASK WHERE { ?x a ?y }"), q2.toAsk());

        Plan p0 = Results.parseTP("?x a ?y");
        Plan p1 = FS.limit(Results.parseTP("?x a ?y"), 1);
        Plan p2 = FS.distinct(Results.parseTP("?x a ?y"));
        Plan p3 = FS.project(Results.parseTP("?x a ?y"), Vars.of("?y"));

        var ask = FS.limit(FS.project(Results.parseTP("?x a ?y"), Vars.EMPTY), 1);
        assertEquals(ask, p0.toAsk());
        assertEquals(ask, p1.toAsk());
        assertEquals(ask, p2.toAsk());
        assertEquals(ask, p3.toAsk());
    }
}