package com.github.alexishuf.fastersparql.sparql.parser;

import com.github.alexishuf.fastersparql.FS;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.Rope;
import com.github.alexishuf.fastersparql.operators.plan.Join;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import com.github.alexishuf.fastersparql.operators.plan.TriplePattern;
import com.github.alexishuf.fastersparql.sparql.InvalidSparqlException;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SparqlParserTest {

    record D(String in, Plan expected) {}

    static TriplePattern tp(Object s, Object p, Object o) {
        Term st = s instanceof Term t ? t : Term.valueOf(s.toString());
        Term pt = p instanceof Term t ? t : Term.valueOf(p.toString());
        Term ot = o instanceof Term t ? t : Term.valueOf(o.toString());
        return new TriplePattern(st, pt, ot);
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
        var ask0 = FS.limit(FS.reduced(FS.project(tp("?s", "?p", "?o"), Vars.EMPTY)), 1);
        var ask1 = FS.limit(FS.reduced(FS.project(tp("?s", "?p", "$o"), Vars.EMPTY)), 1);
        var ask2 = FS.limit(FS.reduced(FS.project(tp("$s", "?p", "?o"), Vars.EMPTY)), 1);
        list.addAll(List.of(
                new D("ASK WHERE { ?s ?p ?o }", ask0),
                new D("ASK { ?s ?p ?o }", ask0),
                new D("ASK { ?s ?p $o . }", ask1),
                new D("ASK\nWHERE{ $s ?p ?o. }", ask2),
                new D("ASK{?s ?p $o}", ask1),
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
        var proj0 = FS.project(tp("?x", "?p", "?o"), Vars.of("x"));
        var proj1 = FS.project(tp("$x", "$p", "$o"), Vars.of("o", "p", "x"));
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
        var lim0 = FS.limit(tp("?s", "?p", "?o"), 23);
        list.addAll(List.of(
                new D("SELECT * WHERE { ?s ?p ?o } LIMIT 23", lim0),
                new D("SELECT * WHERE { ?s ?p ?o } LIMIT\n23", lim0),
                new D("SELECT * WHERE { ?s ?p ?o }\nLIMIT 23", lim0),
                new D("SELECT * WHERE { ?s ?p ?o }\n\tLIMIT 23", lim0),
                new D("SELECT*{?s ?p ?o.}\n\tLIMIT 23", lim0)
        ));

        //offset
        var off0 = FS.offset(tp("?s", "?p", "?o"), 5);
        list.addAll(List.of(
                new D("SELECT * WHERE { ?s ?p ?o } OFFSET 5", off0),
                new D("SELECT * WHERE {?s ?p ?o}\nOFFSET 5", off0),
                new D("SELECT *{?s ?p ?o.}\n\tOFFSET \n#23\n 5", off0)
        ));

        //limit + offset
        var lo0 = FS.modifiers(tp("?s", "?p", "?o"), null, 0, 2, 3, List.of());
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
                FS.offset(new Join(
                        tp("<s>", Term.RDF_TYPE, "<http://example.org/Class>"),
                        tp("<http://example.org/o-1>", Term.RDF_VALUE, "\"23\""),
                        tp("<http://example.org/o-1>", Term.RDF_VALUE, "\"27\""),
                        tp("<http://example.org/o-1>", Term.RDF_VALUE, "\"33.0\"^^"+Term.XSD_DECIMAL)
                ), 23)));
        list.add(new D("""
                        PREFIX : <http://example.org/>
                        SELECT ?x WHERE {
                            ?x :p <o>, 23, 'bob' .
                            ?y <q> <a> ;
                               :r  :Bob
                        }""",
                        FS.project(new Join(
                                tp("?x", "<http://example.org/p>", "<o>"),
                                tp("?x", "<http://example.org/p>", "\"23\"^^"+Term.XSD_INTEGER),
                                tp("?x", "<http://example.org/p>", "\"bob\""),
                                tp("?y", "<q>", "<a>"),
                                tp("?y", "<http://example.org/r>", "<http://example.org/Bob>")
                        ), Vars.of("x"))));

        //FILTER EXISTS/NOT EXISTS
        list.add(new D("""
                PREFIX : <http://example.org/>
                SELECT * WHERE { ?s :p ?o FILTER EXISTS { ?o :p ?s, $x } }
                """,
                FS.exists(
                        tp("?s", "<http://example.org/p>", "?o"),
                        false,
                        new Join(tp("?o", "<http://example.org/p>", "?s"),
                                 tp("?o", "<http://example.org/p>", "$x")))));
        list.add(new D("""
                PREFIX : <http://example.org/>
                SELECT * WHERE { ?s :p ?o. FILTER NOT EXISTS {?o :p ?s, $x} }
                """,
                FS.exists(
                        tp("?s", "<http://example.org/p>", "?o"),
                        true,
                        new Join(tp("?o", "<http://example.org/p>", "?s"),
                                 tp("?o", "<http://example.org/p>", "$x")))));

        return list;
    }

    @Test
    void test() {
        var p = new SparqlParser();
        List<D> data = data();
        for (int i = 0; i < data.size(); i++) {
            D d = data.get(i);
            var baseCtx = "at data()["+i+"]="+d;
            for (String prefix : List.of("", "#ASK {?x $p \"?s\"}\n#\t\n#\n \t")) {
                for (String suffix : List.of("", "\r\n", "#LIMIT 23", "#OFFSET 5\n")) {
                    var ctx = baseCtx + ", prefix=\"" + prefix + "\", suffix=\"" + suffix+'"';
                    var in = Rope.of(prefix, d.in, suffix);
                    if (d.expected == null) {
                        assertThrows(InvalidSparqlException.class, () -> p.parse(in));
                    } else {
                        try {
                            assertEquals(d.expected, p.parse(in), ctx);
                        } catch (InvalidSparqlException e) { fail(ctx, e); }
                    }
                }

            }
        }

    }

}