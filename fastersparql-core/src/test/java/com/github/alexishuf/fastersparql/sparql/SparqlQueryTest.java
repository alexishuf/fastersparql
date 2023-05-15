package com.github.alexishuf.fastersparql.sparql;

import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.Rope;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import com.github.alexishuf.fastersparql.sparql.SparqlQuery.DistinctType;
import com.github.alexishuf.fastersparql.sparql.binding.ArrayBinding;
import com.github.alexishuf.fastersparql.sparql.binding.Binding;
import com.github.alexishuf.fastersparql.sparql.parser.SparqlParser;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import static com.github.alexishuf.fastersparql.sparql.SparqlQuery.DistinctType.STRONG;
import static com.github.alexishuf.fastersparql.sparql.SparqlQuery.DistinctType.WEAK;
import static java.util.Comparator.naturalOrder;
import static org.junit.jupiter.api.Assertions.*;

class SparqlQueryTest {
    interface Parser {
        SparqlQuery parse(String sparql) throws SilentSkip;
        @Nullable String preprocess(String sparql);
        boolean isFull();
    }

    public static final class SilentSkip extends Exception {
    }

    private static final Parser OPAQUE = new Parser() {
        @Override public SparqlQuery parse(String sparql) {
            return new OpaqueSparqlQuery(sparql);
        }
        @Override public String toString() { return "OPAQUE"; }
        @Override public @Nullable String preprocess(String sparql) { return sparql; }
        @Override public boolean isFull() { return false; }
    };

    private static final Pattern GRAPH_RX = Pattern.compile("(?mi)^\\s*(?:CONSTRUCT|DESCRIBE)");

    private static SparqlQuery
    parseFull(SparqlParser parser, String sparql) throws SilentSkip {
        try {
            return parser.parse(SegmentRope.of(sparql));
        } catch (InvalidSparqlException e) {
            if (e.getMessage().startsWith("binding vars to expressions"))
                throw new SilentSkip();
            if (e.getMessage().startsWith("FROM clauses are not supported"))
                throw new SilentSkip();
            if (e.getMessage().matches("(?i)Expected SELECT or ASK at position \\d+, got \"CONSTRUCT\""))
                throw new SilentSkip();
            throw e;
        }
    }

    private static final Parser FULL = new Parser() {
        private final SparqlParser parser = new SparqlParser();
        @Override public SparqlQuery parse(String sparql) throws SilentSkip {
            return parseFull(parser, sparql);
        }
        @Override public String toString() { return "FULL"; }
        @Override public @Nullable String preprocess(String sparql) {
            if (GRAPH_RX.matcher(sparql).find()) return null;
            if (sparql.indexOf(':') == -1)
                return sparql;
            return  """
                    PREFIX : <http://example.org/>
                    PREFIX ex: <http://example.org/>
                    """ + sparql;
        }
        @Override public boolean isFull() { return true; }
    };

    private static final List<Parser> PARSERS = List.of(OPAQUE, FULL);

    record D(String sparql, boolean graph, Vars pub, Vars all, Vars strictPub) {
        public D(String sparql, Vars pub, Vars all) {
            this(sparql, false, pub, all, pub);
        }
        public D(String sparql, String pub) {
            this(sparql,
                    Vars.of(Arrays.stream(pub.split(",")).filter(s -> !s.isEmpty()).toArray(String[]::new)),
                    Vars.of(Arrays.stream(pub.split(",")).filter(s -> !s.isEmpty()).toArray(String[]::new))
            );
        }
        public D(String sparql, String pub, String all) {
            this(sparql,
                    Vars.of(Arrays.stream(pub.split(",")).filter(s -> !s.isEmpty()).toArray(String[]::new)),
                    Vars.of(Arrays.stream(all.split(",")).filter(s -> !s.isEmpty()).toArray(String[]::new))
            );
        }
        public D(String sparql, String pub, String all, String strictPub) {
            this(sparql, false,
                    Vars.of(Arrays.stream(pub.split(",")).filter(s -> !s.isEmpty()).toArray(String[]::new)),
                    Vars.of(Arrays.stream(all.split(",")).filter(s -> !s.isEmpty()).toArray(String[]::new)),
                    Vars.of(Arrays.stream(strictPub.split(",")).filter(s -> !s.isEmpty()).toArray(String[]::new))
            );
        }

        public D(String sparql) {
            this(sparql, true, Vars.EMPTY, Vars.EMPTY, Vars.EMPTY);
        }
    }

    static List<D> data() {
        List<D> base = new ArrayList<>();
        base.addAll(List.of( //graph queries
                new D("CONSTRUCT { ?s <has> ?o } WHERE { ?s ?p ?o }"),
                new D("CONSTRUCT { ?s <has> ?o } WHERE { ?s $p ?o }"),
                new D("CONSTRUCT { ?s <has> ?o } WHERE { $s ?p $o }"),
                new D("CONSTRUCT { ?s <has> ?o } WHERE { $s <hasNot> $o FILTER(?o > ?min)}")
        ));
        base.addAll(List.of( //test parsing of var names
                new D("SELECT ?x WHERE {?x :p <o>}", "x"),
                new D("SELECT $x WHERE {$x :p <o>}", "x"), //$
                new D("SELECT ?long WHERE {?long :p <o>}", "long"), // len>1
                new D("SELECT $long WHERE {?long :p <o>}", "long"), // $, then ?
                new D("SELECT ?long WHERE {$long :p <o>}", "long"), // ?, then $
                new D("SELECT ?x1 WHERE {?x1 :p <o>}", "x1"), //allow digits
                new D("SELECT ?1 WHERE {?1 :p <o>}", "1"), //allow digit start
                new D("SELECT ?23_x WHERE {?23_x :p <o>}", "23_x"), //allow _
                // same rules apply on body:
                new D("SELECT * WHERE {?x1 :p <o>}", "x1"),
                new D("SELECT * WHERE {?1 :p <o>}", "1"),
                new D("SELECT * WHERE {?23_x :p <o>}", "23_x"),
                // parse var inside filters
                new D("ASK { <a> :p ?age FILTER (?age < ?max)}", "", "age,max"),
                new D("SELECT * WHERE { <a> :p ?age FILTER(?age<?max)}", "age,max", "age,max", "age"),
                new D("ASK { <a> :p ?y FILTER EXISTS { ?y :q $x }}", "", "y,x"),
                new D("SELECT * WHERE { <a> :p ?y FILTER NOT EXISTS { ?y :q $x }}", "y,x", "y,x", "y"),
                new D("ASK { <a> :p ?y FILTER EXISTS { ?y :q $x FILTER (?x != ?max) }}", "", "y,x,max")
        ));
        base.addAll(List.of(//ignore vars inside <>
                new D("SELECT * WHERE { <a> :p <rel?x>, ?v}", "v"),
                new D("SELECT * WHERE { <a> :p <r$x>, ?v}", "v"),
                new D("SELECT * WHERE { <a> :p <http://example.org/?x=1>, ?v}", "v")
        ));
        base.addAll(List.of(//ignore vars inside quoted literals
                new D("SELECT * WHERE { <a> :p \"?x\", $v. }", "v"),
                new D("SELECT * WHERE { <a> :p '?x', ?v. }", "v"),
                new D("SELECT * WHERE { <a> :p \"\"\"?x\"\"\", $v. }", "v"),
                new D("SELECT * WHERE { <a> :p '''?x''', ?v. }", "v"),
                // now with explicit datatypes:
                new D("SELECT * WHERE { <a> :p \"?x\"^^<http://www.w3.org/2001/XMLSchema#string>, $v. }", "v"),
                new D("SELECT * WHERE { <a> :p '?x'^^<http://www.w3.org/2001/XMLSchema#string>, ?v. }", "v"),
                new D("SELECT * WHERE { <a> :p \"\"\"?x\"\"\"^^<http://www.w3.org/2001/XMLSchema#string>, $v. }", "v"),
                new D("SELECT * WHERE { <a> :p '''?x'''^^<http://www.w3.org/2001/XMLSchema#string>, ?v. }", "v"),
                // now with explicit prefixed datatypes:
                new D("SELECT * WHERE { <a> :p \"?x\"^^xsd:string, $v. }", "v"),
                new D("SELECT * WHERE { <a> :p '?x'^^xsd:string, ?v. }", "v"),
                new D("SELECT * WHERE { <a> :p \"\"\"?x\"\"\"^^xsd:string, $v. }", "v"),
                new D("SELECT * WHERE { <a> :p '''?x'''^^xsd:string, ?v. }", "v")
        ));
        base.addAll(List.of(//test escaped quotes inside quoted literals
                new D("SELECT * WHERE { <a> :p \"\\\"?x\", ?v }", "v"),
                new D("SELECT * WHERE { <a> :p '\\'?x', ?v }", "v"),
                new D("SELECT * WHERE { <a> :p \"\"\"\" \"\"?x\"\"\", ?v }", "v"),
                new D("SELECT * WHERE { <a> :p '''' ''?x''', ?v }", "v")
        ));
        base.addAll(List.of(//test escaped \ as last char in literal
                new D("SELECT * WHERE { <a> :p \"?x\\\\\", ?v}", "v"),
                new D("SELECT * WHERE { <a> :p '?x\\\\', ?v}", "v"),
                new D("SELECT * WHERE { <a> :p \"\"\"?x\\\\\"\"\", ?v}", "v"),
                new D("SELECT * WHERE { <a> :p '''?x\\\\''', ?v}", "v"),
                // sequence of two escaped \'s
                new D("SELECT * WHERE { <a> :p \"?x\\\\\\\\\", ?v}", "v"),
                new D("SELECT * WHERE { <a> :p '?x\\\\\\\\', ?v}", "v"),
                new D("SELECT * WHERE { <a> :p \"\"\"?x\\\\\\\\\"\"\", ?v}", "v"),
                new D("SELECT * WHERE { <a> :p '''?x\\\\\\\\''', ?v}", "v")
        ));
        base.addAll(List.of( /* migrated from SparqlUtilsTest */
                new D("ASK {<s> <p> \"o\"}", "", ""),
                new D("ASK {<s> <p> ?x}", "", "x"),
                new D("ASK {?s ?p ?x}", "", "s,p,x"),

                // explicitly project all varibles
                new D("SELECT ?x WHERE { <s> <p> ?x }", "x"),
                new D("SELECT ?x WHERE {\n  <s> <p> ?x .\n}", "x"),
                new D("SELECT ?x WHERE {\n  <s> ?x 'o' .\n}", "x"),
                new D("SELECT ?x WHERE {\n  ?x ex:p \"o\" .\n}", "x"),
                new D("SELECT ?s ?x WHERE { ?s <p> <o>;\n\tex:p ?x.}", "s,x"),
                new D("SELECT ?s ?x WHERE { ?s <p> '?o';\n\tex:p ?x.}", "s,x"),
                new D("SELECT ?s ?x WHERE { ?s <p> \"?o\";\n\tex:p ?x.}", "s,x"),
                new D("SELECT ?s ?x WHERE { ?s <p> '''?o''';\n\tex:p ?x.}", "s,x"),
                new D("SELECT ?s ?x WHERE { ?s <p> \"\"\"?o\"\"\";\n\tex:p ?x.}", "s,x"),

                // explicitly project all vars, but in changing order
                new D("SELECT ?y ?x WHERE { <http://example.org/?x=$o> ?x ?y}", "y,x"),
                new D("SELECT ?y ?x WHERE { <http://example.org/?x=$o> ex:p ?x, ?y}", "y,x"),
                new D("SELECT $y $x WHERE { <http://example.org/?x=$o> $x $y}", "y,x"),
                new D("SELECT $y $x WHERE { <http://example.org/?x=$o> ex:p $x, $y}", "y,x"),

                // project some vars
                new D("SELECT ?o WHERE { ?s ?p ?o }", "o", "o,s,p"),
                new D("SELECT ?o WHERE { ?s <p> ?o }", "o", "o,s"),
                new D("SELECT ?o WHERE { ?s <p> ?o. <s> ?p '?w' }", "o", "o,s,p"),
                new D("SELECT $o WHERE { $s $p $o }", "o", "o,s,p"),
                new D("SELECT $o WHERE { $s <p> $o }", "o", "o,s"),
                new D("SELECT $o WHERE { $s <p> $o. <s> $p '$w' }", "o", "o,s,p"),

                // AS
                new D("SELECT (?aa AS ?age) WHERE { ?x :age ?aa } GROUP BY ?x", "age", "age,x,aa"),
                new D("SELECT ?x (avg(?aa) AS ?age) WHERE { ?x :age ?aa } GROUP BY ?x", "x,age", "x,age,aa"),
                new D("SELECT (avg(?aa) AS ?age) ?x WHERE { ?x :age ?aa } GROUP BY ?x", "age,x", "age,x,aa"),
                new D("SELECT (avg(?a) AS ?age) WHERE { ?x :age ?a }", "age", "age,x,a"),
                new D("SELECT (avg(?a) AS ?x) WHERE { ?x :age ?a }", "x", "x,a"),

                // SELECT *
                new D("SELECT * WHERE { ?s ?p ?o. }", "s,p,o"),
                new D("SELECT * WHERE { ?s ?p ?o. ?s ?p 'bob' }", "s,p,o"),
                new D("SELECT * WHERE { ?s ?p ?o. ?s ?p '?x' }", "s,p,o"),
                new D("SELECT * WHERE { ?s ?p ?o. ?s ?p '''?x''' }", "s,p,o"),
                new D("SELECT * WHERE { ?s ?p ?o. ?s ?p \"\"\"?x\"\"\" }", "s,p,o"),
                new D("SELECT * WHERE { ?s ?p ?o. ?s ?p '$x' }", "s,p,o"),
                new D("SELECT * WHERE { ?s ?p ?o. ?s ?p '''$x''' }", "s,p,o"),
                new D("SELECT * WHERE { ?s ?p ?o. ?s ?p \"$x\" }", "s,p,o"),

                // SPARQL * DISTINCT
                new D("SELECT DISTINCT * WHERE { ?s ?p ?o }", "s,p,o"),
                new D("SELECT DISTINCT * WHERE { <s> ex:p ?o }", "o"),
                new D("SELECT DISTINCT * WHERE { <s> ex:p $o }", "o"),
                new D("SELECT DISTINCT * WHERE { <s> ex:p ?x, ?other }", "x,other"),
                new D("SELECT DISTINCT * WHERE { <s> ex:p '?x', ?y. }", "y"),
                new D("SELECT DISTINCT * WHERE { <s> ex:p '''?x''', $y. }", "y"),
                new D("SELECT DISTINCT * WHERE { <s> ex:p \"?x\", $y. }", "y"),
                new D("SELECT DISTINCT * WHERE { <s> ex:p \"\"\"?x\"\"\", ?y. }", "y"),
                new D("SELECT DISTINCT * WHERE { <s> ex:p ?x FILTER(?x > 23) }", "x"),
                new D("SELECT DISTINCT * WHERE { <s> ex:p ?x FILTER(?x > ?y) }", "x,y", "x,y", "x"),
                new D("SELECT DISTINCT * WHERE { <s> ex:p ?x FILTER(?x > $y) }", "x,y", "x,y", "x"),
                new D("SELECT DISTINCT * WHERE { ?x ex:p ?y }", "x,y"),
                new D("SELECT DISTINCT * WHERE { $x ex:p $y }", "x,y"),

                // SELECT * with string literal before vars
                new D("SELECT * WHERE { :s :p \"?x\", ?y; :q ?long}", "y,long"),
                new D("SELECT * WHERE { :s :p '?x', ?y; :q ?long}", "y,long"),
                new D("SELECT * WHERE { :s :p '$x', $y; :q $long}", "y,long"),
                new D("SELECT * WHERE { :s :p \"\"\"?x\"\"\", ?y; :q ?long}", "y,long"),
                new D("SELECT * WHERE { :s :p \"\"\"$x\"\"\", ?y; :q $long}", "y,long"),
                new D("SELECT * WHERE { :s :p '''?x''', ?y; :q ?long}", "y,long")
        ));
        base.addAll(List.of( // can find new vars after AS
            new D("SELECT (LCASE(?nm) as ?lnm) ?s WHERE{ ?s :p ?nm }", "lnm,s", "lnm,s,nm"),
            new D("SELECT (LCASE(?nm) as ?lnm\n) ?s WHERE{ ?s :p ?nm }", "lnm,s", "lnm,s,nm"),
            new D("SELECT ( lcase(?nm) as ?lnm\n) ?s WHERE{ ?s :p ?nm; :q ?x FILTER (?x < ?y) }", "lnm,s", "lnm,s,nm,x,y")
        ));
        List<String> prologues = List.of("",
                " ",
                "\n#SELECT\r\n# ASK\n",
                "BASE <http://example.org/>\n",
                "PREFIX foaf: <http://xmlns.com/foaf/0.1/>\n",
                "PREFIX : <http://example.org/?x=$y>\n",
                "PREFIX : <http://example.org/?x=$y> .\n",
                "# comment with ?x and $var\nPREFIX : <http://example.org/$var>\n");
        List<D> list = new ArrayList<>();
        for (String prologue : prologues) {
            for (D d : base)
                list.add(new D(prologue+d.sparql, d.graph, d.pub, d.all, d.strictPub));
        }
        return list;
    }

    @Test //@ParameterizedTest will be annoyingly slow
    void test() {
        for (Parser parser : PARSERS) {
            List<D> data = data();
            for (int row = 0; row < data.size(); row++) {
                D d = data.get(row);
                String sparql = parser.preprocess(d.sparql);
                if (sparql == null)
                    continue;
                var msg = " for parser=" + parser + ", data()["+row+"]=" + d;
                SparqlQuery q;
                try {
                    q = parser.parse(sparql);
                } catch (SilentSkip e) { continue; }
                assertEquals(d.graph, q.isGraph(), "bad isGraph()" + msg);
                if (parser.isFull())
                    assertEquals(d.strictPub, q.publicVars(), "bad publicVars()" + msg);
                else
                    assertEquals(d.pub, q.publicVars(), "bad publicVars()" + msg);
                assertEquals(d.all, q.allVars(), "bad allVars()" + msg);
                assertSame(q, q.bound(ArrayBinding.EMPTY), "bind() failed to detect no-op" + msg);

                if (q instanceof OpaqueSparqlQuery oq) {
                    assertEquals(sparql, oq.sparql().toString(), "bad sparql" + msg);
                    List<Integer> posList = new ArrayList<>();
                    for (Rope name : d.all) {
                        for (String marker : List.of("?", "$")) {
                            int i = -1;
                            while (true) {
                                i = sparql.indexOf(marker + name, i + 1);
                                if (i == -1) break;
                                posList.addAll(List.of(i, i + 1 + name.length()));
                            }
                        }
                    }
                    //posList is naive and has errors:
                    // - sees variables inside quoted literals and IRIs
                    // - sees "?name" where SparqlQuery sees "(... AS ?name)"
                    if (posList.size() <= oq.varPos.length
                            && !Pattern.compile("(?i) AS ?[$?]").matcher(sparql).find()) {
                        posList.sort(naturalOrder());
                        int[] exPositions = posList.stream().mapToInt(Integer::intValue).toArray();
                        assertArrayEquals(exPositions, oq.varPos, "bad varPositions" + msg);
                    }
                }
            }
        }
    }

    record B(String sparql, String expected, Binding binding) {}

    List<B> bindData() {
        List<B> base = new ArrayList<>();
        base.addAll(List.of( // no-op
                new B("CONSTRUCT { ?x ?p true } WHERE { ?x ?p ?o }",
                      "CONSTRUCT { ?x ?p true } WHERE { ?x ?p ?o }",
                      ArrayBinding.of("y", "<a>")),
                new B("SELECT ?x WHERE { ?x <p> '?o'}",
                      "SELECT ?x WHERE { ?x <p> '?o'}",
                      ArrayBinding.of("o", "<a>")),
                new B("SELECT * WHERE { ?x <p> '?o'}",
                      "SELECT * WHERE { ?x <p> '?o'}",
                      ArrayBinding.of("o", "<a>")),
                new B("ASK { ?x <p> '?o'}",
                      "ASK { ?x <p> '?o'}",
                      ArrayBinding.of("o", "<a>"))
        ));
        base.addAll(List.of(//only affect body
                new B("SELECT * WHERE { ?s :p ?o }",
                      "SELECT * WHERE { ?s :p <rel> }",
                      ArrayBinding.of("o", "<rel>", "a", "<a>")),
                new B("ASK { ?s :p ?2 }", // replace  with long value
                      "ASK { ?s :p \"23\"^^<http://www.w3.org/2001/XMLSchema#int> }",
                      ArrayBinding.of("2", "\"23\"^^<http://www.w3.org/2001/XMLSchema#int>")),
                new B("ASK { ?s :p ?2 }",
                      "ASK { \"23\"^^<http://www.w3.org/2001/XMLSchema#int> :p ?2 }",
                      ArrayBinding.of("s", "\"23\"^^<http://www.w3.org/2001/XMLSchema#int>")),
                new B("SELECT * WHERE { ?s :p ?o. ?o :q ?s }",
                      "SELECT * WHERE { <http://example.org/a> :p ?o. ?o :q <http://example.org/a> }",
                      ArrayBinding.of("s", "<http://example.org/a>")),
                new B("SELECT ?x WHERE { ?x :p ?y FILTER(?y < ?max) }",
                      "SELECT ?x WHERE { ?x :p ?y FILTER(?y < 23) }",
                      ArrayBinding.of("max", "23")),
                new B("SELECT ?y WHERE { ?x :p ?y FILTER(?y < ?max)}",
                      "SELECT ?y WHERE { <test> :p ?y FILTER(?y < 23)}",
                      ArrayBinding.of("max", "23", "x", "<test>"))
        ));

        base.addAll(List.of(// remove from projection clause
                new B("SELECT ?x ?y WHERE { ?x :p $y}",
                      "SELECT ?x  WHERE { ?x :p <a>}",
                      ArrayBinding.of("y", "<a>")),
                new B("SELECT ?y ?x WHERE { ?x :p $y }",
                      "SELECT ?y  WHERE { <a> :p $y }",
                      ArrayBinding.of("x", "<a>"))
        ));
        base.addAll(List.of(// remove from AS in projection clause
                new B("SELECT ?s (year(?birth) AS ?yr) WHERE { ?s :p ?birth }",
                      "SELECT  (year(?birth) AS ?yr) WHERE { <a> :p ?birth }",
                      ArrayBinding.of("s", "<a>")),
                new B("SELECT ?s (year(?birth) AS ?yr) WHERE { $s :p ?birth }",
                      "SELECT ?s (year(\"1990-01-13\"^^<http://www.w3.org/2001/XMLSchema#date>) AS ?yr) WHERE { $s :p \"1990-01-13\"^^<http://www.w3.org/2001/XMLSchema#date> }",
                      ArrayBinding.of("birth", "\"1990-01-13\"^^<http://www.w3.org/2001/XMLSchema#date>"))
        ));
        base.addAll(List.of(//ported from SparqlUtilTest
                new B("ASK {?s :p :o}", "ASK {<a> :p :o}",
                      ArrayBinding.of("s", "<a>")),
                new B("ASK {:s ?p :o}", "ASK {:s <http://example.org/knows> :o}",
                      ArrayBinding.of("p", "<http://example.org/knows>")),
                new B("ASK {:s :p ?o}", "ASK {:s :p \"bob\"}",
                      ArrayBinding.of("o", "\"bob\"")),
                new B("SELECT * WHERE {?s ?p :o}", "SELECT * WHERE {<b> ?p :o}",
                      ArrayBinding.of("s", "<b>")),
                new B("SELECT * WHERE {?s ?p ?o}", "SELECT * WHERE {<b> ?p <c>}",
                      ArrayBinding.of("s", "<b>", "o", "<c>")),
                new B("SELECT * WHERE {?en :p \"?en\"@en; :q ?en.}",
                      "ASK WHERE {<a> :p \"?en\"@en; :q <a>.}",
                      ArrayBinding.of("en", "<a>")),
                new B("SELECT ?en WHERE {?en :p \"?en\"@en; :q ?en.}",
                      "ASK WHERE {<a> :p \"?en\"@en; :q <a>.}",
                      ArrayBinding.of("en", "<a>")),
                new B("SELECT ?s {?en :p \"?en\"@en; :q ?en.}",
                      "SELECT ?s {<a> :p \"?en\"@en; :q <a>.}",
                      ArrayBinding.of("en", "<a>")),
                new B("SELECT ?s{?en :p \"?en\"@en; :q ?en.}",
                      "SELECT ?s{<a> :p \"?en\"@en; :q <a>.}",
                      ArrayBinding.of("en", "<a>")),
                new B("SELECT ?en ?s WHERE {?en :p \"?en\"@en; :q ?en; :r ?s.}",
                      "SELECT  ?s WHERE {<a> :p \"?en\"@en; :q <a>; :r ?s.}",
                      ArrayBinding.of("en", "<a>")),
                new B("SELECT ?s ?en WHERE {?en :p \"?en\"@en; :q ?en; :r ?s.}",
                      "SELECT ?s  WHERE {<a> :p \"?en\"@en; :q <a>; :r ?s.}",
                      ArrayBinding.of("en", "<a>")),
                new B("SELECT ?s WHERE {?s :p ?o}", "ASK WHERE {<a> :p ?o}",
                      ArrayBinding.of("s", "<a>")),
                new B("SELECT ?s ?o WHERE {?s :p ?o}", "ASK WHERE {<a> :p <b>}",
                      ArrayBinding.of("s", "<a>", "o", "<b>")),
                new B("SELECT * WHERE {?s :p ?o}", "ASK WHERE {<a> :p <b>}",
                      ArrayBinding.of("s", "<a>", "o", "<b>"))
        ));
        List<B> list = new ArrayList<>();
        for (String prefix : List.of("", "PREFIX : <http://example.org/ex?x=1>\n#c\n#?s\n")) {
            for (B b : base)
                list.add(new B(prefix+b.sparql, prefix+b.expected, b.binding));
        }
        return list;
    }

    @Test
    void testBind() {
        for (Parser parser : PARSERS) {
            List<B> bindData = bindData();
            for (int row = 0; row < bindData.size(); row++) {
                B d = bindData.get(row);
                var ctx = " at parser=" + parser + ", bindData()["+row+"]=" + d;
                SparqlQuery q, ex;
                try {
                    String sparql = parser.preprocess(d.sparql);
                    String expectedSparql = parser.preprocess(d.expected);
                    if (sparql == null) continue;
                    q = parser.parse(sparql);
                    ex = parser.parse(expectedSparql);
                } catch (SilentSkip e) {
                    continue;
                } catch (Throwable t) {
                    fail(t.getClass().getSimpleName()+ctx, t);
                    throw t;
                }
                var b = q.bound(d.binding);

                assertEquals(ex.publicVars(), b.publicVars(), "bad publicVars" + ctx);
                assertEquals(ex.allVars(), b.allVars(), "bad allVars" + ctx);
                assertEquals(ex.isGraph(), b.isGraph(), "bad isGraph" + ctx);
                if (b instanceof OpaqueSparqlQuery ob) {
                    assertEquals(d.expected, ob.sparql.toString(), "bad sparql" + ctx);
                    assertArrayEquals(((OpaqueSparqlQuery) ex).varPos, ob.varPos,
                            "bad varPositions" + ctx);
                }

                if (d.expected.equals(d.sparql))
                    assertSame(q, b);
            }
        }
    }

    record A(String in, String expected) { }

    static List<A> askData() {
        List<A> base = List.of(
                // explicit projection clause
                new A("SELECT ?s WHERE { ?s ?p ?o }", "ASK WHERE { ?s ?p ?o }"),
                new A("SELECT ?s ?o WHERE { ?s ?p ?o }", "ASK WHERE { ?s ?p ?o }"),
                new A("SELECT ?o ?p WHERE { ?s ?p ?o }", "ASK WHERE { ?s ?p ?o }"),
                // no WHERE
                new A("select ?s { ?s ?p ?o }", "ASK { ?s ?p ?o }"),
                new A("select ?s ?o { ?s ?p ?o }", "ASK { ?s ?p ?o }"),
                new A("select ?o ?p { ?s ?p ?o }", "ASK { ?s ?p ?o }"),
                // glue { to last var
                new A("SELECT ?s{ ?s ?p ?o }", "ASK{ ?s ?p ?o }"),
                new A("SELECT ?s ?o{ ?s ?p ?o }", "ASK{ ?s ?p ?o }"),
                new A("SELECT ?o ?p{ ?s ?p ?o }", "ASK{ ?s ?p ?o }"),

                // implicit projection clause
                new A("SELECT * WHERE { ?s ?p ?o }", "ASK WHERE { ?s ?p ?o }"),
                new A("SELECT * WHERE { ?s ?p 'SELECT * {' }", "ASK WHERE { ?s ?p 'SELECT * {' }"),

                //AS
                new A("SELECT (LCASE(?o) AS ?nm) WHERE { ?s ?p ?o }",
                      "ASK WHERE { ?s ?p ?o }"),
                new A("SELECT (LCASE(?o) AS ?nm ) WHERE { ?s ?p ?o }",
                        "ASK WHERE { ?s ?p ?o }"),
                new A("SELECT (LCASE(?o) as ?nm ) WHERE { ?s ?p ?o }",
                        "ASK WHERE { ?s ?p ?o }"),
                new A("SELECT ?p (LCASE(?o) AS ?nm) WHERE { ?s ?p ?o }",
                        "ASK WHERE { ?s ?p ?o }"),
                new A("SELECT (LCASE(?o) AS ?nm) ?p WHERE { ?s ?p ?o }",
                        "ASK WHERE { ?s ?p ?o }"),

                // AS with no WHERE and glued {
                new A("SELECT (LCASE(?o) AS ?nm){ ?s ?p ?o }", "ASK{ ?s ?p ?o }"),
                new A("SELECT ?p (LCASE(?o) AS ?nm){ ?s ?p ?o }", "ASK{ ?s ?p ?o }"),

                //FROM
                new A("SELECT * FROM <a> WHERE { ?s ?p ?o }", "ASK FROM <a> WHERE { ?s ?p ?o }")
        );
        List<A> list = new ArrayList<>();
        for (String prologue : List.of("", "PREFIX : <select>\nBASE <select?s>\n#?s\n# select ?s {\n")) {
            for (A a : base)
                list.add(new A(prologue+a.in, prologue+a.expected));
        }
        return list;
    }

    @Test
    void testToAsk() {
        for (Parser parser : PARSERS) {
            List<A> askData = askData();
            for (int row = 0; row < askData.size(); row++) {
                A d = askData.get(row);
                var ctx = " for askData()["+row+"]=" + d;
                SparqlQuery q, b, e;
                try {
                    q = parser.parse(d.in);
                    b = parser.parse(d.in);
                    e = parser.parse(d.expected);
                } catch (SilentSkip t) {
                    continue;
                } catch (Throwable t) {
                    fail(t.getClass().getSimpleName() + ctx, t);
                    throw t;
                }

                var a = q.toAsk();

                assertEquals(e.isGraph(), a.isGraph(), "bad isGraph" + ctx);
                assertEquals(e.publicVars(), a.publicVars(), "bad publicVars" + ctx);
                assertEquals(e.allVars(), a.allVars(), "bad allVars" + ctx);
                if (a instanceof OpaqueSparqlQuery oa) {
                    var oe = (OpaqueSparqlQuery) e;
                    assertEquals(d.expected, oa.sparql().toString(), "bad sparql" + ctx);
                    assertEquals(oe.aliasVars, oa.aliasVars, "bad aliasVars" + ctx);
                    assertArrayEquals(oe.varPos, oa.varPos, "bad varPos" + ctx);
                }

                assertEquals(b.allVars(), q.allVars(), "q mutated by toAsk()");
                assertEquals(b.publicVars(), q.publicVars(), "q mutated by toAsk()");
                assertEquals(b.isGraph(), q.isGraph(), "q mutated by toAsk()");
                if (q instanceof OpaqueSparqlQuery oq) {
                    var ob = (OpaqueSparqlQuery) b;
                    assertEquals(ob.aliasVars, oq.aliasVars, "q mutated by toAsk()");
                    assertEquals(ob.sparql, oq.sparql, "q mutated by toAsk()");
                    assertArrayEquals(ob.varPos, oq.varPos, "q mutated by toAsk()");
                }


            }
        }
    }

    record T(String in, DistinctType type, String expected) {}

    static List<T> distinctData() {
        List<T> base = List.of(
                new T("ASK { ?s ?p ?o }", WEAK, "ASK { ?s ?p ?o }"),
                new T("ASK { ?s ?p ?o. }", WEAK, "ASK { ?s ?p ?o. }"),
                new T("CONSTRUCT {?o ?p ?s} WHERE {?s ?p ?o}", STRONG,
                      "CONSTRUCT {?o ?p ?s} WHERE {?s ?p ?o}"),
                new T("SELECT REDUCED * WHERE {?s ?p ?o}", STRONG,
                      "SELECT DISTINCT * WHERE {?s ?p ?o}"),
                new T("SELECT\n\treduced * WHERE {?s ?p ?o}", STRONG,
                      "SELECT DISTINCT * WHERE {?s ?p ?o}"),
                new T("SELECT DISTINCT * WHERE {?s ?p ?o}", STRONG,
                      "SELECT DISTINCT * WHERE {?s ?p ?o}"),
                new T("SELECT \tdistinct * WHERE {?s ?p ?o}", STRONG,
                      "SELECT \tdistinct * WHERE {?s ?p ?o}"),
                new T("SELECT ?o WHERE {?s :p ?o}", STRONG,
                      "SELECT DISTINCT ?o WHERE {?s :p ?o}"),
                new T("SELECT ?o WHERE {?s :p ?o}", WEAK,
                      "SELECT REDUCED ?o WHERE {?s :p ?o}"),
                new T("select ?o WHERE {?s :p ?o}", STRONG,
                      "select DISTINCT ?o WHERE {?s :p ?o}"),
                new T("select ?o WHERE {?s :p ?o}", WEAK,
                      "select REDUCED ?o WHERE {?s :p ?o}"),
                new T("Select ?o WHERE {?s :p ?o}", STRONG,
                      "Select DISTINCT ?o WHERE {?s :p ?o}"),
                new T("Select ?o WHERE {?s :p ?o}", WEAK,
                      "Select REDUCED ?o WHERE {?s :p ?o}"),
                new T("SELECT\n?o WHERE {<a> :p ?o}", STRONG,
                      "SELECT DISTINCT\n?o WHERE {<a> :p ?o}"),
                new T("SELECT\n?o WHERE {<a> :p ?o}", WEAK,
                      "SELECT REDUCED\n?o WHERE {<a> :p ?o}")
        );
        List<T> list = new ArrayList<>();
        for (String prologue : List.of("", "PREFIX random: <select>\n#BASE <select?s>\n#?s\n# select ?s {\n")) {
            for (T t : base)
                list.add(new T(prologue+t.in, t.type, prologue+t.expected));
        }
        return list;

    }

    @Test
    void testDistinct() {
        for (Parser parser : PARSERS) {
            List<T> distinctData = distinctData();
            for (int row = 0; row < distinctData.size(); row++) {
                T t = distinctData.get(row);
                var ctx = " for parser=" + parser + ", distinctData()["+row+"]=" + t;
                SparqlQuery q, e;
                try {
                    String inSparql = parser.preprocess(t.in);
                    String expectedSparql = parser.preprocess(t.expected);
                    if (inSparql == null) continue;
                    q = parser.parse(inSparql);
                    e = parser.parse(expectedSparql);
                } catch (SilentSkip ignored) {
                    continue;
                } catch (Throwable error) {
                    fail(error.getClass().getSimpleName()+ctx, error);
                    throw error;
                }
                var a = q.toDistinct(t.type());

                assertEquals(e.isGraph(), a.isGraph(), "bad isGraph" + ctx);
                assertEquals(e.publicVars(), a.publicVars(), "bad publicVars" + ctx);
                assertEquals(e.allVars(), a.allVars(), "bad allVars" + ctx);
                if (a instanceof OpaqueSparqlQuery oa) {
                    var oe = (OpaqueSparqlQuery) e;
                    assertEquals(t.expected, oa.sparql().toString(), "bad sparql" + ctx);
                    assertEquals(oe.aliasVars, oa.aliasVars, "bad aliasVars" + ctx);
                    assertArrayEquals(oe.varPos, oa.varPos, "bad varPos" + ctx);
                }
            }
        }
    }
}