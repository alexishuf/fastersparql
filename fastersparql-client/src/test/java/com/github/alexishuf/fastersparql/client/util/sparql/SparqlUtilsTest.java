package com.github.alexishuf.fastersparql.client.util.sparql;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.client.util.sparql.SparqlUtils.*;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class SparqlUtilsTest {

    static Stream<Arguments> testStringEnd() {
        return Stream.of(
                //empty string
        /*  1 */arguments("", -1),

                //empty literal
        /*  2 */arguments("''", 2),
        /*  3 */arguments("\"\"", 2),
        /*  4 */arguments("''''''", 6),
        /*  5 */arguments("\"\"\"\"\"\"", 6),

                // space after string
        /*  6 */arguments("'' ", 2),
        /*  7 */arguments("\"\" ", 2),
        /*  8 */arguments("'''''' ", 6),
        /*  9 */arguments("\"\"\"\"\"\" ", 6),

                //single letter within
        /* 10 */arguments("'a'", 3),
        /* 11 */arguments("\"a\"", 3),
        /* 12 */arguments("'''a'''", 7),
        /* 13 */arguments("\"\"\"a\"\"\"", 7),

                //escaped quote within
        /* 14 */arguments("\"\\\"\"", 4),
        /* 15 */arguments("'\\''", 4),
        /* 16 */arguments("\"\"\"\\\"\"\"\"", 8),
        /* 17 */arguments("'''\\''''", 8),

                //unclosed string
        /* 18 */arguments("\"a'", -1),
        /* 19 */arguments("'a\"", -1),
        /* 20 */arguments("'''a\"\"\"", -1),
        /* 21 */arguments("'''a''", -1),
        /* 22 */arguments("\"\"\"a\"\"", -1)
        );
    }

    @ParameterizedTest @MethodSource
    void testStringEnd(String in, int expected) {
        String suffixed = in+"\"";
        String prefixed = "\"" + in;
        int expectedNoPrefix = expected == -1 ? in.length() : expected;
        int expectedPrefix = expected == -1 ? prefixed.length() : expected+1;

        assertEquals(expectedNoPrefix, stringEnd(in,       0, in.length()));
        assertEquals(expectedNoPrefix, stringEnd(suffixed, 0, in.length()));
        assertEquals(expectedPrefix,   stringEnd(prefixed, 1, prefixed.length()));
    }

    static Stream<Arguments> testNextVarData() {
        List<String[]> base = Stream.of(
                // cases where input is just before a variable
                "0  | ?x",
                "0  | $x",
                "1  |  ?x",
                "1  |  $x",
                "2  |  \n?x",
                "2  |  \n$x",
                "5  |  .\r\n\t?x",
                "5  |  .\r\n\t$x",

                // variable as the object of a triple pattern
                "10 | ex:s ex:p ?x",
                "11 | <rel> ex:p ?_x1",
                "32 | <http://example.org/Alice> ex:p ?_x1",
                "32 | <http://example.org/Alice> ex:p ?_x1.",
                "32 | <http://example.org/Alice> ex:p ?_x1 .\n",
                "10 | ex:s ex:p ?_x1, ex:other .\n",

                // variable followed by another
                "10 | ex:s ex:p ?_x1, ?x2 .\n",
                "10 | ex:s ex:p ?_x1,?x2 .\n",
                "10 | ex:s ex:p ?_x1 , ?x2 .\n",
                "10 | ex:s ex:p ?_x1\n\t   , ?x2 .\n",
                "11 | ex:s ex:p (?_x1 ?x2) .\n",

                // variable in first triple pattern
                "10 | ex:s ex:p ?v1;\nex:q ?v2 .\n",
                "10 | ex:s ex:p ?v1 ;\nex:q ?v2 .\n",
                "10 | ex:s ex:p ?v1.\nex:q ?v2 .\n",
                "10 | ex:s ex:p ?v1 .\nex:t ex:q ?v2.\n",

                // test with []'s
                "34 | [] ex:p [a foaf:Person; foaf:name ?name] .\n",
                "34 | [] ex:p [a foaf:Person; foaf:name ?name; foaf:age ?age] .\n",

                //variable as predicate of a triple pattern
                "5  | ex:s ?p ex:obj.\n",
                "5  | ex:s ?p ?other.\n",

                //variable as subject of a triple pattern
                "5  | ex:s ?p ex:obj.\n",
                "5  | ex:s ?p ?other.\n",

                // skip over query param
                "30 | <http://example.org?x=1> ex:p ?v",
                "28 | <http://example.org?x> ex:p ?v",
                "27 | <http://example.org?> ex:p ?v",
                "30 | <http://example.org?x=$> ex:p ?v",
                "30 | <http://example.org?x=1> ex:p $v",
                "28 | <http://example.org?x> ex:p $v",
                "27 | <http://example.org?> ex:p $v",
                "30 | <http://example.org?x=$> ex:p $v",

                // skip over literal
                "10 | \"?x\" ex:p ?v",
                "14 | \"\"\"?x\"\"\" ex:p ?v",
                "10 | '?x' ex:p ?v",
                "14 | '''?x''' ex:p ?v",

                "18 | \"<a> <p> ?x\" ex:p ?v",
                "22 | \"\"\"<a> <p> ?x\"\"\" ex:p ?v",
                "18 | '<a> <p> ?x' ex:p ?v",
                "22 | '''<a> <p> ?x''' ex:p ?v",

                "21 | \"ex:s ex:p $x.\" ex:p ?v",
                "25 | \"\"\"ex:s ex:p $x.\"\"\" ex:p ?v",
                "21 | 'ex:s ex:p $x.' ex:p ?v",
                "25 | '''ex:s ex:p $x.''' ex:p ?v",

                // on projection
                "7  | SELECT ?x WHERE {?x ?p $x}",
                "7  | SELECT ?x ?y WHERE {?x ?p $x}",
                "11 | SELECT avg(?x) ?y WHERE {?x ?p $x}",

                // inside filters
                "7  | FILTER(?x > 23)",
                "7  | FILTER(?x > \"23\"^^xsd:int)",
                "18 | FILTER(regex(\".\", $x))",
                "23 | FILTER(regex(\".\\$x?y\", ?x))",
                "12 | FILTER(23 + ?x > 23)",
                "24 | FILTER EXISTS { <s> <p> ?x}",
                "24 | FILTER EXISTS { <s> <p> ?x.}",
                "24 | FILTER EXISTS { <s> <p> $x.}",

                //no variable
                "-1 | ",
                "-1 |  ",
                "-1 | \r\n\t ",
                "-1 | <a> <p> <o>",
                "-1 | [] foaf:name '', \"\"; <p> ()",
                "-1 | [] foaf:name '', \"\"; <p?x> ()",
                "-1 | [] foaf:name '', \"\"; <p?x=$k> ()",
                "-1 | [foaf:name \"?x\"] foaf:name '', \"\"; <p?x=$k> ()",
                "-1 | [foaf:name \".?x\"] foaf:name '', \"\"; <p?x=$k> ()",
                "-1 | [foaf:name \". ?x\"] foaf:name '', \"\"; <p?x=$k> ()",
                "-1 | [foaf:name \"\"\"?x\"\"\"] foaf:name '', \"\"; <p?x=$k> ()",
                "-1 | [foaf:name \"\"\".?x\"\"\"] foaf:name '', \"\"; <p?x=$k> ()",
                "-1 | [foaf:name \"\"\". ?x\"\"\"] foaf:name '', \"\"; <p?x=$k> ()",
                "-1 | [foaf:name '?x'] foaf:name '', \"\"; <p?x=$k> ()",
                "-1 | [foaf:name '.?x'] foaf:name '', \"\"; <p?x=$k> ()",
                "-1 | [foaf:name '. ?x'] foaf:name '', \"\"; <p?x=$k> ()",
                "-1 | [foaf:name '''?x'''] foaf:name '', \"\"; <p?x=$k> ()",
                "-1 | [foaf:name '''.?x'''] foaf:name '', \"\"; <p?x=$k> ()",
                "-1 | [foaf:name '''. ?x'''] foaf:name '', \"\"; <p?x=$k> ()"
        ).map(s -> s.split(" *\\| "))
                .map(a -> a.length < 2 ? new String[]{a[0], ""} : a)
                .collect(Collectors.toList());

        List<String> prefixes = asList(
                "",
                "ASK {",
                "SELECT * WHERE {",
                "PREFIX foaf: <http://xmlns.com/foaf/0.1/>\nSELECT * WHERE {",
                "PREFIX foaf: <http://xmlns.com/foaf/0.1/>\r\n\tSELECT * WHERE {",
                "PREFIX amb: <http://example.org/?x=$y>\r\n\tSELECT * WHERE {",
                "PREFIX amb: <http://example.org/?x=$y#>\nASK {",
                "#comment ?x or $y\nASK {"
        );
        List<String> suffixes = asList(
                "",
                ".\n",
                "}\n"
        );

        return base.stream().flatMap(a -> prefixes.stream().flatMap(prefix -> suffixes.stream()
                .map(suffix -> {
                    int ex = Integer.parseInt(a[0]);
                    int adjustedEx = ex == -1 ? -1 : ex+prefix.length();
                    return arguments(adjustedEx, prefix+a[1]+suffix);
                })));
    }

    @Test
    void testNextVar() {
        List<Arguments> rows = testNextVarData().collect(Collectors.toList());
        List<AssertionError> errors = IntStream.range(0, rows.size()).parallel().mapToObj(i -> {
            Object[] args = rows.get(i).get();
            try {
                doTestNextVar((int) args[0], (String) args[1]);
                return null;
            } catch (Throwable t) {
                return new AssertionError("Failed at " + i + "-th test row " + Arrays.toString(args), t);
            }
        }).filter(Objects::nonNull).collect(Collectors.toList());
        assertEquals(Collections.emptyList(), errors);
    }

    @Test
    void testNextVarAsCharSequence() {
        List<Arguments> rows = testNextVarData().collect(Collectors.toList());
        List<AssertionError> errors = IntStream.range(0, rows.size()).parallel().mapToObj(i -> {
            Object[] args = rows.get(i).get();
            try {
                doTestNextVar((int) args[0], new StringBuilder((String) args[1]));
                return null;
            } catch (Throwable t) {
                return new AssertionError("Failed at "+i+"-th test row "+Arrays.toString(args), t);
            }
        }).filter(Objects::nonNull).collect(Collectors.toList());
        assertEquals(emptyList(), errors);
    }

    private void doTestNextVar(int expected, CharSequence in) {
        int fullExpected = expected == -1 ? in.length() : expected;
        int shortExpected = expected == -1 ? in.length()-1 : expected;
        assertEquals(fullExpected, nextVar(in, 0, in.length()));
        if (in.length() > 0) {
            if ("\"'<#".indexOf(in.charAt(0)) == -1 && expected > 0) {
                assertEquals(fullExpected, nextVar(in, 1, in.length()));
                if (in.length() > 2)
                    assertEquals(shortExpected, nextVar(in, 1, in.length()-1));
            }
            assertEquals(shortExpected, nextVar(in, 0, in.length()-1));
        }
    }

    @ParameterizedTest @ValueSource(strings = {
            "2 | ?x.",
            "2 | $x.",
            "2 | ?x,",
            "2 | ?x ",
            "2 | $x ",
            "1 | ?. ",
            "1 | $. ",
            "2 | ?x ex:p",
            "5 | ?x0_ç ex:p",
            "2 | ?x] ex:p",
            "2 | ?x) ex:p",
            "2 | ?x\" ex:p",
    })
    void testVarEnd(String dataString) {
        String[] data = dataString.split(" *\\| *");
        int expected = Integer.parseInt(data[0]);
        for (int i = 0; i < 4; i++) {
            StringBuilder b = new StringBuilder();
            for (int j = 0; j < i; j++) b.append('.');
            StringBuilder input = b.append(data[1]);
            int actual = SparqlUtils.varEnd(input, i+1, input.length());
            assertEquals(expected+i, actual, "i="+i);
        }
    }

    static Stream<Arguments> testPublicVars() {
        //noinspection JavacQuirks
        List<List<String>> base = asList(
                // ASK queries never expose vars
        /*  1 */asList("ASK {<s> <p> \"o\"}", ""),
        /*  2 */asList("ASK {<s> <p> ?x}", ""),
        /*  3 */asList("ASK {?s ?p ?x}", ""),

                // explicitly project all varibles
        /*  4 */asList("SELECT ?x WHERE { <s> <p> ?x }", "x"),
        /*  5 */asList("SELECT ?x WHERE {\n  <s> <p> ?x .\n}", "x"),
        /*  6 */asList("SELECT ?x WHERE {\n  <s> ?x 'o' .\n}", "x"),
        /*  7 */asList("SELECT ?x WHERE {\n  ?x ex:p \"o\" .\n}", "x"),
        /*  8 */asList("SELECT ?s ?x WHERE { ?s <p> <o>;\n\tex:p ?x.}", "s,x"),
        /*  9 */asList("SELECT ?s ?x WHERE { ?s <p> '?o';\n\tex:p ?x.}", "s,x"),
        /* 10 */asList("SELECT ?s ?x WHERE { ?s <p> \"?o\";\n\tex:p ?x.}", "s,x"),
        /* 11 */asList("SELECT ?s ?x WHERE { ?s <p> '''?o''';\n\tex:p ?x.}", "s,x"),
        /* 12 */asList("SELECT ?s ?x WHERE { ?s <p> \"\"\"?o\"\"\";\n\tex:p ?x.}", "s,x"),

                // explicitly project all vars, but in changing order
        /* 13 */asList("SELECT ?y ?x WHERE { <http://example.org/?x=$o> ?x ?y}", "y,x"),
        /* 14 */asList("SELECT ?y ?x WHERE { <http://example.org/?x=$o> ex:p ?x, ?y}", "y,x"),
        /* 15 */asList("SELECT $y $x WHERE { <http://example.org/?x=$o> $x $y}", "y,x"),
        /* 16 */asList("SELECT $y $x WHERE { <http://example.org/?x=$o> ex:p $x, $y}", "y,x"),

                // project some vars
        /* 17 */asList("SELECT ?o WHERE { ?s ?p ?o }", "o"),
        /* 18 */asList("SELECT ?o WHERE { ?s <p> ?o }", "o"),
        /* 19 */asList("SELECT ?o WHERE { ?s <p> ?o. <s> ?p '?w' }", "o"),
        /* 20 */asList("SELECT $o WHERE { $s $p $o }", "o"),
        /* 21 */asList("SELECT $o WHERE { $s <p> $o }", "o"),
        /* 22 */asList("SELECT $o WHERE { $s <p> $o. <s> $p '$w' }", "o"),

                // AS
        /* 23 */asList("SELECT ?x (avg(?aa) AS ?age) WHERE { ?x :age ?aa } GROUP BY ?x", "x,age"),
        /* 24 */asList("SELECT (avg(?aa) AS ?age) ?x WHERE { ?x :age ?aa } GROUP BY ?x", "age,x"),
        /* 25 */asList("SELECT (avg(?a) AS ?age) WHERE { ?x :age ?a }", "age"),
        /* 26 */asList("SELECT (avg(?a) AS ?x) WHERE { ?x :age ?a }", "x"),

                // SELECT *
        /* 27 */asList("SELECT * WHERE { ?s ?p ?o. }", "s,p,o"),
        /* 28 */asList("SELECT * WHERE { ?s ?p ?o. ?s ?p 'bob' }", "s,p,o"),
        /* 29 */asList("SELECT * WHERE { ?s ?p ?o. ?s ?p '?x' }", "s,p,o"),
        /* 30 */asList("SELECT * WHERE { ?s ?p ?o. ?s ?p '''?x''' }", "s,p,o"),
        /* 31 */asList("SELECT * WHERE { ?s ?p ?o. ?s ?p \"\"\"?x\"\"\" }", "s,p,o"),
        /* 32 */asList("SELECT * WHERE { ?s ?p ?o. ?s ?p '$x' }", "s,p,o"),
        /* 33 */asList("SELECT * WHERE { ?s ?p ?o. ?s ?p '''$x''' }", "s,p,o"),
        /* 34 */asList("SELECT * WHERE { ?s ?p ?o. ?s ?p \"$x\" }", "s,p,o"),

                // SELECT * with string literal before vars
        /* 35 */asList("SELECT * WHERE { :s :p \"?x\", ?y; :q ?long}", "y,long"),
        /* 36 */asList("SELECT * WHERE { :s :p '?x', ?y; :q ?long}", "y,long"),
        /* 37 */asList("SELECT * WHERE { :s :p '$x', $y; :q $long}", "y,long"),
        /* 38 */asList("SELECT * WHERE { :s :p \"\"\"?x\"\"\", ?y; :q ?long}", "y,long"),
        /* 39 */asList("SELECT * WHERE { :s :p \"\"\"$x\"\"\", ?y; :q $long}", "y,long"),
        /* 40 */asList("SELECT * WHERE { :s :p '''?x''', ?y; :q ?long}", "y,long"),

                // SPARQL fragments
        /* 41 */asList("?s ?p ?o", "s,p,o"),
        /* 42 */asList("<s> ex:p ?o", "o"),
        /* 43 */asList("<s> ex:p $o", "o"),
        /* 44 */asList("<s> ex:p ?x, ?other", "x,other"),
        /* 45 */asList("<s> ex:p '?x', ?y.", "y"),
        /* 46 */asList("<s> ex:p '''?x''', $y.", "y"),
        /* 47 */asList("<s> ex:p \"?x\", $y.", "y"),
        /* 48 */asList("<s> ex:p \"\"\"?x\"\"\", ?y.", "y"),
        /* 49 */asList("<s> ex:p ?x FILTER(?x > 23)", "x"),
        /* 50 */asList("<s> ex:p ?x FILTER(?x > ?y)", "x,y"),
        /* 51 */asList("<s> ex:p ?x FILTER(?x > $y)", "x,y"),
        /* 52 */asList("?x > ?y", "x,y"),
        /* 53 */asList("$x > $y", "x,y")
        );
        List<String> prefixes = asList(
                "",
                "PREFIX foaf: <http://xmlns.com/foaf/0.1/>\n",
                "PREFIX : <http://example.org/?x=$y>\n",
                "PREFIX : <http://example.org/?x=$y> .\n",
                "# comment with ?x and $var\nPREFIX : <http://example.org/$var>\n"
        );
        return prefixes.stream().flatMap(prefix -> base.stream()
                .map(l -> {
                    List<String> vars;
                    if (l.get(1).isEmpty()) vars = emptyList();
                    else                    vars = asList(l.get(1).split(" *, *"));
                    return arguments(prefix + l.get(0), vars);
                }));
    }

//    @ParameterizedTest @MethodSource
//    void testPublicVars(String sparql, List<String> expected) {
//        assertEquals(expected, SparqlUtils.publicVars(sparql));
//    }
    @Test
    void parallelTestPublicVars() throws Throwable {
        List<Throwable> errors = testPublicVars().parallel().map(Arguments::get).map(a -> {
            try {
                assertEquals(a[1], publicVars((String) a[0]));
                return null;
            } catch (Throwable t) {
                return new AssertionError("Failed at row " + Arrays.toString(a), t);
            }
        }).filter(Objects::nonNull).collect(Collectors.toList());
        if (!errors.isEmpty())
            throw errors.get(0);
    }

    static Stream<Arguments> testAllVars() {
        List<List<String>> base = asList(
                // simple triple patterns
                asList("?s ?p ?o", "s,p,o"),
                asList("?s <p> ?o", "s,o"),
                asList("?s <?p> ?o", "s,o"),
                asList("?s :p '?o'", "s"),
                asList("?s :p '''?o'''", "s"),
                asList("?s :p \"?o\"", "s"),
                asList("?s :p \"\"\"?o\"\"\"", "s"),
                asList("_:s :p ?o", "o"),

                // no variables
                asList("<s> :p \"?o\"", ""),
                asList("<s> :p \"\"\"?o\"\"\"", ""),
                asList("<s> :p '?o'", ""),
                asList("<s> :p '''?o'''", ""),
                asList("_:s :p '$o'", ""),

                // variables in second triple
                asList("_:s <p> \"?s\\\"$x\", ?y; :q $z", "y,z"),
                asList("_:s <p> \"\"\"?s\"$x\"\"\", ?y; :q $z", "y,z"),
                asList("_:s <p> '?s\\'$x', ?y; :q $z", "y,z"),
                asList("_:s <p> '''?s'$x''', ?y; :q $z", "y,z"),

                // variables in filter clause
                asList("_s :p ?age FILTER(?age > ?limit)", "age,limit"),
                asList("_s :p ?age FILTER EXISTS {[] :q ?age FILTER (?age > ?limit)}", "age,limit")
        );
        List<String> prefixes = asList(
                "",
                "PREFIX : <http://example.org/?not=$aVar>\n",
                "# not ?a $var ?comment\n",
                "SELECT ?x WHERE {",
                "ASK {",
                "PREFIX : <http://example.org/?not=$aVar>\n# not ?a $var ?comment\nASK {"
        );
        return prefixes.stream().flatMap(prefix -> base.stream().map(b -> {
            List<String> expected = b.get(1).isEmpty() ? emptyList()
                                  : asList(b.get(1).split(","));
            String effPrefix = prefix;
            if (prefix.equals("SELECT ?x WHERE {")) {
                if (expected.isEmpty())
                    effPrefix = "SELECT * WHERE {";
                else
                    effPrefix = effPrefix.replace("?x", expected.get(0));
            }
            String sparql = effPrefix + b.get(0) + (effPrefix.isEmpty() ? "" : "}");
            return arguments(sparql, expected);
        }));
    }

    @ParameterizedTest @MethodSource
    void testAllVars(String sparql, List<String> expected) {
        assertEquals(expected, SparqlUtils.allVars(sparql));
    }

    static Stream<Arguments> testReadProjection() {
        List<List<String>> base = asList(
                // single var
                asList("?x", "x"),
                asList("?x ", "x"),

                //single var with $
                asList("$x", "x"),
                asList("$x ", "x"),

                //two vars
                asList("?x ?y", "x,y"),
                asList("?x $y ", "x,y"),

                //two vars, reverse alphabetic order
                asList("$y ?x", "y,x"),
                asList("$y $x ", "y,x"),

                //start with AS
                asList("(avg(?a) + ?o AS ?expr) ?b", "expr,b"),
                asList("( avg(?a) + ?o AS ?expr ) ?b", "expr,b"),
                asList("(\navg(?a) + ?o AS ?expr ) ?b", "expr,b"),
                asList("(\navg(?a) + ?o AS #continues...\n ?expr ) ?b", "expr,b"),
                asList("(\navg(?a) + ?o AS #continues?x...\n ?expr ) ?b", "expr,b"),

                // AS expression in between
                asList("?first (avg(?a) + ?o AS ?x) ?third", "first,x,third"),
                asList("?first ( avg(?a) + ?o AS ?x ) ?third", "first,x,third"),
                asList("?first, ( avg(?a) + ?o AS ?x ), ?third", "first,x,third"),

                // end with AS expression
                asList("?first (avg(?a) + ?o AS ?second)", "first,second"),

                // ignore var in commend
                asList("# not ?x\n?y ", "y"),
                asList("?first # not ?x\n?second ", "first,second"),

                //wildcard
                asList("*", null),
                asList(" * ", null)
        );
        List<String> prefixes = asList("", "\n\t ", "#not ?var\n", " DISTINCT ");
        return prefixes.stream().flatMap(prefix -> base.stream().map(b -> {
                    List<String> vars;
                    if (b.get(1) == null)        vars = null;
                    else if (b.get(1).isEmpty()) vars = emptyList();
                    else                         vars = asList(b.get(1).split(" *, *"));
                    assert vars == null || vars.stream().noneMatch(String::isEmpty);
                    return arguments(prefix + b.get(0), vars);
                }));
    }

    @ParameterizedTest @MethodSource
    void testReadProjection(String string, List<String> expectedVars) {
        if (expectedVars != null) {
            for (String prefix : asList("SELECT ", "ASK ", "ask ", "?ignore select ")) {
                for (String suffix : asList("", "\n{ ?blob", "\nWHERE { ?blob")) {
                    boolean isAsk = prefix.contains("ASK") || prefix.contains("ask");
                    int begin = prefix.startsWith("?ignore ") ? 8 : 0;
                    int end = prefix.length() + string.length();
                    if (suffix.indexOf('{') > 0)
                        end += suffix.indexOf('{');
                    String sparql = prefix + string + suffix;
                    ProjectionInfo ac = readProjection(sparql, begin, isAsk);
                    assertNotNull(ac);
                    assertEquals(isAsk,        ac.isAsk);
                    assertEquals(begin,        ac.begin);
                    assertEquals(end,          ac.end);
                    assertEquals(expectedVars, ac.vars);
                }
            }
        } else {
            assertNull(readProjection("SELECT "+string, 0, false));
            assertNull(readProjection("ASK "+string, 0, true));
            assertNull(readProjection("select "+string, 0, false));
            assertNull(readProjection("ask "+string, 0, true));
        }
    }

    static Stream<Arguments> testFindProjection() {
        List<List<String>> base = asList(
                asList("# ....", null),
                asList("ASK { <s> ?p ?o }", ""),
                asList("SELECT ?s WHERE { ?s ?p ?o }", "s"),
                asList("SELECT ?s ?p WHERE { ?s ?p ?o }", "s,p"),
                asList("SELECT ?s ?p (avg(?a) AS ?x) WHERE { ?s ?p ?o }", "s,p,x"),
                asList("SELECT ?s (avg(?a) AS ?x) ?p  WHERE { ?s ?p ?o }", "s,x,p"),
                asList("SELECT (avg(?a) AS ?x) ?s ?p  WHERE { ?s ?p ?o }", "x,s,p"),
                asList("SELECT * WHERE { ?s ?p ?o }", null),
                asList("?x > 23", null),
                asList("<s> ?p ?o", null),
                asList("<s> ?p ?o", null)
        );
        List<String> prefixes = asList(
                "",
                "# SELECT ?x\n",
                "PREFIX : <http://example.org/?x=$y> .\n"
        );
        return prefixes.stream().flatMap(prefix -> base.stream().map(b -> {
            List<String> vars;
            if (b.get(1) == null) vars = null;
            else if (b.get(1).isEmpty()) vars = emptyList();
            else vars = asList(b.get(1).split(" *, *"));
            assert vars == null || vars.stream().noneMatch(String::isEmpty);
            return arguments(prefix + b.get(0), vars);
        }));
    }

    @ParameterizedTest @MethodSource
    void testFindProjection(String string, @Nullable List<String> expectedVars) {
        ProjectionInfo info = findProjection(string);
        if (expectedVars == null) {
            assertNull(info);
        } else {
            assertNotNull(info);
            assertEquals(expectedVars, info.vars);
        }
    }

    static Stream<Arguments> testFindBodyOpen() {
        List<Arguments> base = asList(
                arguments("SELECT * WHERE {", 15),
                arguments("SELECT DISTINCT * WHERE {", 24),
                arguments("ASK {", 4),
                arguments("ASK ", -1),
                arguments("SELECT ", -1),
                arguments("SELECT *", -1),
                arguments("SELECT * ", -1),
                arguments("SELECT ?s {", 10),
                arguments("SELECT ?s \n{", 11),
                arguments("SELECT ?s $v {", 13),
                arguments("SELECT (avg(?a) AS ?s) $v {", 26),
                arguments("SELECT (regex(\"{\", ?a) AS ?s) $v {", 33),
                arguments("SELECT (regex('{', ?a) AS ?s) $v {", 33),
                arguments("SELECT (regex('''{''', ?a) AS ?s) $v {", 37),
                arguments("SELECT (regex(\"\"\"{\"\"\", ?a) AS ?s) $v {", 37)
        );
        List<String> prefixes = asList(
                "",
                "# { ?x }){\n",
                "PREFIX : <a?x={}>"
        );
        List<String> suffixes = asList(
                "",
                "?s $p \"{o}\"}"
        );
        return prefixes.stream().flatMap(prefix -> suffixes.stream().flatMap(suffix ->
                base.stream().map(b -> {
                    int ex = (Integer)b.get()[1];
                    if (ex >= 0) ex += prefix.length();
                    String string = prefix + b.get()[0].toString() + suffix;
                    return arguments(string, ex);
                })));
    }

//    @ParameterizedTest @MethodSource
//    void testFindBodyOpen(String sparql, int expected) {
//        assertEquals(expected, SparqlUtils.findBodyOpen(sparql));
//    }
    @Test
    void parallelTestFindBodyOpen() throws Throwable {
        List<Throwable> errors = testFindBodyOpen().parallel().map(Arguments::get).map(a -> {
            try {
                assertEquals(a[1], findBodyOpen((String) a[0]));
                return null;
            } catch (Throwable t) {
                return new AssertionError("Failed for " + Arrays.toString(a), t);
            }
        }).filter(Objects::nonNull).collect(Collectors.toList());
        if (!errors.isEmpty())
            throw errors.get(0);
    }

    static Stream<Arguments> testBind() {
        String prefix = "# not $a ?p ?x ?en ?var\n" +
                "PREFIX : <http://example.org/>\n" +
                "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
                "PREFIX dummy: <http://example.org/?s=$en>\n";
        return Stream.of(
        /*  1 */asList("ASK {?s :p :o}", "s=<a>", "ASK {<a> :p :o}"),
        /*  2 */asList("ASK {:s ?p :o}", "p=<http://example.org/knows>",
                       "ASK {:s <http://example.org/knows> :o}"),
        /*  3 */asList("ASK {:s :p ?o}", "o=\"bob\"", "ASK {:s :p \"bob\"}"),
        /*  4 */asList("SELECT * WHERE {?s ?p :o}", "s=<b>",
                       "SELECT * WHERE {<b> ?p :o}"),
        /*  5 */asList("SELECT * WHERE {?s ?p ?o}", "s=<b>,o=<c>",
                       "SELECT * WHERE {<b> ?p <c>}"),
        /*  6 */asList("SELECT * WHERE {?en :p \"?en\"@en; :q ?en.}", "en=<a>",
                       "SELECT * WHERE {<a> :p \"?en\"@en; :q <a>.}"),
        /*  7 */asList("SELECT ?s WHERE {?en :p \"?en\"@en; :q ?en.}", "en=<a>",
                       "SELECT ?s WHERE {<a> :p \"?en\"@en; :q <a>.}"),
        /*  8 */asList("SELECT ?en ?s WHERE {?en :p \"?en\"@en; :q ?en; :r ?s.}", "en=<a>",
                       "SELECT  ?s WHERE {<a> :p \"?en\"@en; :q <a>; :r ?s.}"),
        /*  9 */asList("SELECT ?s ?en WHERE {?en :p \"?en\"@en; :q ?en; :r ?s.}", "en=<a>",
                       "SELECT ?s  WHERE {<a> :p \"?en\"@en; :q <a>; :r ?s.}"),
        /* 10 */asList("SELECT ?s WHERE {?s :p ?o}", "s=<a>",
                        "ASK {<a> :p ?o}"),
        /* 11 */asList("SELECT ?s ?o WHERE {?s :p ?o}", "s=<a>,o=<b>",
                       "ASK {<a> :p <b>}"),
        /* 12 */asList("SELECT * WHERE {?s :p ?o}", "s=<a>,o=<b>",
                       "SELECT * WHERE {<a> :p <b>}")
        ).map(l -> {
            Map<String, String> map = new HashMap<>();
            for (String kvString : l.get(1).split(",")) {
                String[] kv = kvString.split("=");
                map.put(kv[0], kv[1]);
            }
            return arguments(prefix+l.get(0), map, prefix+l.get(2));
        });
    }

    @ParameterizedTest @MethodSource
    void testBind(String string, Map<String, String> map, String expected) {
        assertEquals(expected, bind(string, map).toString());

        StringBuilder cs = new StringBuilder(string);
        assertEquals(expected, bind(cs, map).toString());
        assertEquals(string, cs.toString(), "bind() modified its input");
    }
}