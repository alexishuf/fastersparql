package com.github.alexishuf.fastersparql.sparql.parser;

import com.github.alexishuf.fastersparql.client.model.Vars;
import com.github.alexishuf.fastersparql.client.model.row.types.ListRow;
import com.github.alexishuf.fastersparql.operators.FSOps;
import com.github.alexishuf.fastersparql.operators.plan.Join;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import com.github.alexishuf.fastersparql.sparql.InvalidSparqlException;
import com.github.alexishuf.fastersparql.sparql.RDF;
import com.github.alexishuf.fastersparql.sparql.RDFTypes;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SparqlParserTest {

    record D(String in, Plan<List<String>, String> expected) {}

    static TriplePattern<List<String>, String> tp(String s, String p, String o) {
        return new TriplePattern<>(ListRow.STRING, s, p, o);
    }

    static List<D> data() {
        //incomplete queries
        List<D> list = new ArrayList<>(List.of(
                new D("", null),
                new D("PREFIX : <>", null),
                new D("PREFIX : <http://example.org>\n", null),
                new D("#ASK {?s ?p ?o}\n", null),
                new D("ASK", null),
                new D("ASK {", null),
                new D("ASK WHERE", null),
                new D("ASK WHERE {", null),
                new D("BASE<>\nSELECT", null),
                new D("BASE<>\nSELECT *", null),
                new D("BASE<>\nSELECT * WHERE ", null),
                new D("BASE<>\nSELECT * WHERE {", null)
        ));

        // simplest ASK query
        var ask0 = FSOps.limit(FSOps.reduced(FSOps.project(tp("?s", "?p", "?o"), Vars.EMPTY)), 1);
        var ask1 = FSOps.limit(FSOps.reduced(FSOps.project(tp("?s", "?p", "$o"), Vars.EMPTY)), 1);
        var ask2 = FSOps.limit(FSOps.reduced(FSOps.project(tp("$s", "?p", "?o"), Vars.EMPTY)), 1);
        list.addAll(List.of(
                new D("ASK WHERE { ?s ?p ?o }", ask0),
                new D("ASK { ?s ?p ?o }", ask0),
                new D("ASK { ?s ?p $o . }", ask1),
                new D("ASK\nWHERE{ $s ?p ?o. }", ask2),
                new D("ASK{?s ?p $o}", ask0),
                new D("ASK{?s ?p $o.}", ask1)
        ));

        var sel0 = tp("?s", "?p", "?o");
        var sel1 = tp("?s", "?p", "$o");
        list.addAll(List.of(
                new D("SELECT * WHERE { ?s ?p ?o }", sel0),
                new D("SELECT * WHERE { ?s ?p ?o. }", sel0),
                new D("SELECT * WHERE {?s ?p ?o.}", sel0),
                new D("SELECT * WHERE {?s ?p ?o}", sel0),

                new D("SELECT * WHERE{ ?s ?p ?o }", sel0),
                new D("SELECT * { ?s ?p ?o }", sel0),
                new D("SELECT *{ ?s ?p ?o }", sel0),
                new D("SELECT ?s ?p ?o { ?s ?p ?o }", sel0),
                new D("SELECT ?s ?p ?o { ?s ?p $o }", sel1),
                new D("SELECT ?s ?p ?o { ?s ?p $o. }", sel1),
                new D("SELECT ?s ?p ?o { ?s ?p $o.}", sel1),
                new D("SELECT ?s ?p ?o { ?s ?p $o}", sel1)
        ));

        //projection
        var proj0 = FSOps.project(tp("?x", "?p", "?o"), Vars.of("x"));
        var proj1 = FSOps.project(tp("$x", "$p", "$o"), Vars.of("o", "p", "x"));
        list.addAll(List.of(
                new D("SELECT ?x WHERE { ?x ?p ?o }", proj0),
                new D("SELECT ?x { ?x ?p ?o }", proj0),
                new D("SELECT ?x{ ?x ?p ?o }", proj0),
                new D("SELECT ?x{ ?x ?p ?o. }", proj0),
                new D("SELECT ?x{?x ?p ?o.}", proj0),
                new D("SELECT ?x{?x ?p ?o}", proj0),

                new D("SELECT ?o ?p ?x WHERE { $x $p $o }", proj1),
                new D("SELECT ?o ?p ?x{$x $p $o}", proj1)
        ));

        //limit
        var lim0 = FSOps.limit(tp("?s", "?p", "?o"), 23);
        list.addAll(List.of(
                new D("SELECT * WHERE { ?s ?p ?o } LIMIT 23", lim0),
                new D("SELECT * WHERE { ?s ?p ?o } LIMIT\n23", lim0),
                new D("SELECT * WHERE { ?s ?p ?o }\nLIMIT 23", lim0),
                new D("SELECT * WHERE { ?s ?p ?o }\n\tLIMIT 23", lim0),
                new D("SELECT*{?s ?p ?o.}\n\tLIMIT 23", lim0)
        ));

        //offset
        var off0 = FSOps.offset(tp("?s", "?p", "?o"), 5);
        list.addAll(List.of(
                new D("SELECT * WHERE { ?s ?p ?o } OFFSET 5", off0),
                new D("SELECT * WHERE {?s ?p ?o}\nOFFSET 5", off0),
                new D("SELECT *{?s ?p ?o.}\n\tOFFSET \n#23\n 5", off0)
        ));

        //limit + offset
        var lo0 = FSOps.modifiers(tp("?s", "?p", "?o"), null, 0, 2, 3, List.of());
        list.addAll(List.of(
                new D("SELECT * WHERE { ?s ?p ?o } LIMIT 3 OFFSET 2", lo0),
                new D("SELECT * WHERE { ?s ?p ?o } OFFSET 2 LIMIT 3 ", lo0),
                new D("SELECT * WHERE {?s ?p ?o} LIMIT 3 OFFSET 2", lo0),
                new D("SELECT * {?s ?p ?o.} OFFSET 2 LIMIT 3 ", lo0),
                new D("SELECT * {?s ?p ?o.}\nOFFSET\n2\nLIMIT\n3\n", lo0),
                new D("SELECT * {?s ?p ?o.}\nLIMIT\n3\nOFFSET\n2\n", lo0),
                new D("SELECT * {?s ?p ?o.}#OFFSET 2 LIMIT 3\nLIMIT 3 OFFSET 2", lo0)
        ));

        // terms syntax, ',' and ';'
        list.add(new D("""
                PREFIX :<http://example.org/>
                SELECT * { <s> a :Class. :o-1 rdf:value "23", '''27''', 33.0. } OFFSET 23
                """,
                FSOps.offset(new Join<>(List.of(
                        tp("<s>", '<'+RDF.type+'>', "<http://example.org/Class>"),
                        tp("<http://example.org/o-1>", '<'+RDF.value+'>', "\"23\""),
                        tp("<http://example.org/o-1>", '<'+RDF.value+'>', "\"27\""),
                        tp("<http://example.org/o-1>", '<'+RDF.value+'>', "\"33.0\"^^<"+RDFTypes.decimal+">")
                ), null, null), 23)));
        list.add(new D("""
                        PREFIX : <http://example.org/>
                        SELECT ?x WHERE {
                            ?x :p <o>, 23, 'bob' .
                            ?y <q> <a> ;
                               :r  :Bob
                        }""",
                        FSOps.project(new Join<>(List.of(
                                tp("?x", "<http://example.org/p>", "<o>"),
                                tp("?x", "<http://example.org/p>", "\"23\"^^<"+RDFTypes.integer+">"),
                                tp("?x", "<http://example.org/p>", "\"bob\""),
                                tp("?y", "<http://example.org/q>", "<a>"),
                                tp("?y", "<http://example.org/r>", "<http://example.org/Bob>")
                        ), null, null), Vars.of("x"))));

        //FILTER EXISTS/NOT EXISTS
        list.add(new D("""
                PREFIX : <http://example.org>
                SELECT * WHERE { ?s :p ?o FILTER EXISTS { ?o :p ?s, $x } }
                """,
                FSOps.exists(
                        tp("?s", "<http://example.org/p>", "?o"),
                        false,
                        new Join<>(List.of(tp("?o", "<http://example.org/p>", "?s"),
                                           tp("?o", "<http://example.org/p>", "?x")),
                                  null, null))));
        list.add(new D("""
                PREFIX : <http://example.org>
                SELECT * WHERE { ?s :p ?o. FILTER NOT EXISTS {?o :p ?s, $x} }
                """,
                FSOps.exists(
                        tp("?s", "<http://example.org/p>", "?o"),
                        true,
                        new Join<>(List.of(tp("?o", "<http://example.org/p>", "?s"),
                                tp("?o", "<http://example.org/p>", "?x")),
                                null, null))));

        return list;
    }

    @Test
    void test() {
        var p = new SparqlParser<>(ListRow.STRING);
        List<D> data = data();
        for (int i = 0; i < data.size(); i++) {
            D d = data.get(i);
            var baseCtx = "at data()["+i+"]="+d;
            for (String prefix : List.of("", "#ASK {?x $p \"?s\"}\n#\t\n#\n \t")) {
                for (String suffix : List.of("", "\r\n", "#LIMIT 23", "#OFFSET 5\n")) {
                    var ctx = baseCtx + ", prefix=" + prefix + ", suffix=" + suffix;
                    var in = prefix+d.in+suffix;
                    if (d.expected == null)
                        assertThrows(InvalidSparqlException.class, () -> p.parse(in, 0));
                    else {
                        try {
                            assertEquals(d.expected, p.parse(in, 0), ctx);
                        } catch (InvalidSparqlException e) { fail(ctx, e); }
                    }
                }

            }
            for (String prefix : List.of("ASK {?s $p <?o>}", "SELECT * WHERE {}")) {
                String ctx = baseCtx + ", prefix=" + prefix;
                String in = prefix+d.in;
                if (d.expected == null)
                    assertThrows(InvalidSparqlException.class, () -> p.parse(in, prefix.length()));
                else
                    assertEquals(d.expected, p.parse(in, prefix.length()), ctx);
            }
        }

    }

}