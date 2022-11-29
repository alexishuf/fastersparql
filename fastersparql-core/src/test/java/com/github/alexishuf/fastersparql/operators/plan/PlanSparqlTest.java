package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.client.model.Vars;
import com.github.alexishuf.fastersparql.client.model.row.types.ListRow;
import com.github.alexishuf.fastersparql.operators.FSOps;
import com.github.alexishuf.fastersparql.operators.FSOpsProperties;
import com.github.alexishuf.fastersparql.operators.bind.BindJoin;
import com.github.alexishuf.fastersparql.sparql.RDF;
import com.github.alexishuf.fastersparql.sparql.RDFTypes;
import com.github.alexishuf.fastersparql.sparql.parser.TriplePattern;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.github.alexishuf.fastersparql.operators.FSOps.*;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class PlanSparqlTest {
    record D(Plan<List<String>, String> in, String expected) {}

    static List<D> data() {
        var t0 = new TriplePattern<>(ListRow.STRING, "?s", "<http://example.org/p>", "\"23\"");
        var t1 = new TriplePattern<>(ListRow.STRING, "<s>", "$p", "\"27\"");
        var t2 = new TriplePattern<>(ListRow.STRING, "<s>", "$p", "\"31\"");
        var t4 = new TriplePattern<>(ListRow.STRING, "?s", "?p", "<s>");
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
                new D(new Join<>(List.of(t0, t1), null, null), """
                        SELECT *
                        {
                         ?s <http://example.org/p> "23" .
                         <s> $p "27" .
                        }"""),
                new D(new BindJoin<>(new BindJoin<>(t0, t1, null, null, null), t2, null, null, null), """
                        SELECT *
                        {
                         ?s <http://example.org/p> "23" .
                         <s> $p "27" .
                         <s> $p "31" .
                        }"""),
                new D(union(List.of(t1, t2)), """
                        SELECT *
                        {
                         {
                          <s> $p "27" .
                         } UNION
                         {
                          <s> $p "31" .
                         }
                        }"""),
                new D(offset(limit(t1, 23), 7), """
                        SELECT *
                        {
                         <s> $p "27" .
                        } OFFSET 7 LIMIT 23"""),
                new D(distinct(project(FSOps.leftJoin(t0, t1), Vars.of("s"))), """
                        SELECT DISTINCT ?s
                        {
                         ?s <http://example.org/p> "23" .
                         OPTIONAL
                         {
                          <s> $p "27" .
                         }
                        }"""),
                new D(distinct(exists(filter(t1, "?p < 23", "NOT EXISTS {?p a rdf:Property}"), false, t4), FSOpsProperties.reducedCapacity()), """
                        SELECT REDUCED *
                        {
                         <s> $p "27" .
                         FILTER (?p < 23)
                         FILTER NOT EXISTS
                         {
                          ?p a <http://www.w3.org/1999/02/22-rdf-syntax-ns#Property> .
                         }
                         FILTER EXISTS
                         {
                          ?s ?p <s> .
                         }
                        }"""),
                new D(join(values(ListRow.STRING, Vars.of("p", "x"), List.of(asList("<p>", null), List.of("<q>", "\"23\"^^<"+RDFTypes.integer +">"))), new BindJoin<>(t1, t2, null, null, null)), """
                        SELECT *
                        {
                         VALUES ( ?p ?x ) {
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
                actual = data.get(i).in.sparql();
            } catch (Throwable t) { fail(ctx, t);  throw t; }
            assertEquals(data.get(i).expected, actual, ctx);
        }
    }
}
