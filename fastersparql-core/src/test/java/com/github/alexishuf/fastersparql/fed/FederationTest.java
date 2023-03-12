package com.github.alexishuf.fastersparql.fed;

import com.github.alexishuf.fastersparql.FS;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.operators.plan.*;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class FederationTest {
    static Stream<Arguments> testSanitize() {
        var tp1 = new TriplePattern("?x1", "<p>", "?x2");
        var tp2 = new TriplePattern("?x2", "<p>", "?x3");
        var tp3 = new TriplePattern("?x3", "<p>", "?x4");
        var tp4 = new TriplePattern("?x4", "<p>", "?x5");
        var sac = new TriplePattern("?s", Term.RDF_TYPE, "<c>");
        var spx = new TriplePattern("?s", "<p>", "?x");
        List<Plan> ok = List.of(
                new TriplePattern("<s>", "<p>", "<o>"),
                new Empty(Vars.of("x"), Vars.of("x", "y")),
                new Join(tp1, tp2),
                new Join(tp1, tp2, tp3),
                FS.union(tp1, FS.union(23, tp2, tp3))
        );

        List<Arguments> list = new ArrayList<>();
        for (Plan p : ok)
            list.add(arguments(p, p));
        list.addAll(List.of(
                // replace queries to UNBOUND_CLIENT with the queries themselves
                arguments(FS.exists(FS.query("SELECT * WHERE {?s a <c>}"),
                                    FS.project(FS.query("SELECT * WHERE {?s <p> ?x}"), Vars.of("s"))),
                          FS.exists(sac, FS.project(spx, Vars.of("s")))),
                arguments(new Join(tp1, new Join(tp2, tp3)),
                          new Join(tp1, tp2, tp3)),
                arguments(new Join(Vars.of("x2", "x3", "x4", "x1"),
                                    tp1, new Join(tp2, tp3)),
                          new Join(Vars.of("x2", "x3", "x4", "x1"), tp1, tp2, tp3)),
                arguments(FS.union(tp1, tp2, FS.union(tp3, tp4)),
                          FS.union(tp1, tp2, tp3, tp4))
        ));
        return list.stream();
    }

    @ParameterizedTest @MethodSource
    void testSanitize(Plan in, Plan expected) {
        Plan inCopy = in.deepCopy();
        assertEquals(inCopy, in);
        Plan actual = Federation.copySanitize(in);
        assertEquals(inCopy, in);
        assertEquals(expected, actual);
        if (in.type != Operator.TRIPLE && in.type != Operator.EMPTY)
            assertNotSame(in, actual);

        assertEquals(expected, Federation.mutateSanitize(inCopy));
    }
}