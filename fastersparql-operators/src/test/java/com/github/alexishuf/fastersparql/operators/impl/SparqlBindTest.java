package com.github.alexishuf.fastersparql.operators.impl;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.operators.impl.SparqlBind.nextVar;
import static com.github.alexishuf.fastersparql.operators.impl.SparqlBind.stringEnd;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class SparqlBindTest {

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

    static Stream<Arguments> testNextVar() {
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

    @ParameterizedTest @MethodSource
    void testNextVar(int expected, String in) {
        doTestNextVar(expected, in);
    }

    @ParameterizedTest @MethodSource("testNextVar")
    void testNextVarAsCharSequence(int expected, String in) {
        doTestNextVar(expected, new StringBuilder(in));
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
            int actual = SparqlBind.varEnd(input, i+1, input.length());
            assertEquals(expected+i, actual, "i="+i);
        }
    }
}