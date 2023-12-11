package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.FS;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.SharedRopes;
import com.github.alexishuf.fastersparql.sparql.DistinctType;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import org.junit.jupiter.api.Test;

import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class PlanSparqlTest {
    record D(Plan in, String expected) {}

    static List<D> data() {
        var t0 = new TriplePattern( "?s", "<http://example.org/p>", "\"23\"");
        var t1 = new TriplePattern( "<s>", "$p", "\"27\"");
        var t2 = new TriplePattern( "<s>", "$p", "\"31\"");
        var t4 = new TriplePattern( "?s", "?p", "<s>");
        return List.of(
                new D(t0, """
                        SELECT *
                        {
                         ?s <http://example.org/p> "23" .
                        }"""),
                new D(t1, """
                        SELECT *
                        {
                         <s> $p "27" .
                        }"""),
                new D(new Join(t0, t1), """
                        SELECT *
                        {
                         ?s <http://example.org/p> "23" .
                         <s> $p "27" .
                        }"""),
                new D(new Join(new Join(t0, t1), t2), """
                        SELECT *
                        {
                         ?s <http://example.org/p> "23" .
                         <s> $p "27" .
                         <s> $p "31" .
                        }"""),
                new D(FS.union(t1, t2), """
                        SELECT *
                        {
                         {
                          <s> $p "27" .
                         } UNION
                         {
                          <s> $p "31" .
                         }
                        }"""),
                new D(FS.offset(FS.limit(t1, 23), 7), """
                        SELECT *
                        {
                         <s> $p "27" .
                        } OFFSET 7 LIMIT 23"""),
                new D(FS.distinct(FS.project(FS.leftJoin(t0, t1), Vars.of("s"))), """
                        SELECT DISTINCT ?s
                        {
                         ?s <http://example.org/p> "23" .
                         OPTIONAL
                         {
                          <s> $p "27" .
                         }
                        }"""),
                new D(FS.distinct(FS.exists(FS.filter(t1, "?p < 23", "NOT EXISTS {?p a rdf:Property}"), false, t4), DistinctType.REDUCED), """
                        SELECT REDUCED *
                        {
                         <s> $p "27" .
                         FILTER(?p < 23)
                         FILTER NOT EXISTS
                         {
                          ?p a <http://www.w3.org/1999/02/22-rdf-syntax-ns#Property> .
                         }
                         FILTER EXISTS
                         {
                          ?s ?p <s> .
                         }
                        }"""),
                new D(FS.join(FS.values(Vars.of("p", "x"), List.of(asList(Term.valueOf("<p>"), null), List.of(Term.valueOf("<q>"), Term.typed(23, SharedRopes.DT_integer)))),
                              new Join(t1, t2)), """
                        SELECT *
                        {
                         VALUES (?p ?x) {
                          ( <p> UNDEF )
                          ( <q> 23 )
                         }
                         <s> $p "27" .
                         <s> $p "31" .
                        }""")
        );
    }

    @Test
    void test() {
        List<D> data = data();
        for (int i = 0; i < data.size(); i++) {
            String actual, ctx = "at data.get("+i+")="+data.get(i);
            try {
                actual = data.get(i).in.sparql().toString();
            } catch (Throwable t) { fail(ctx, t);  throw t; }
            assertEquals(data.get(i).expected, actual, ctx);
        }
    }
}
