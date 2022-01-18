package com.github.alexishuf.fastersparql.client.parser.results;

import com.github.alexishuf.fastersparql.client.parser.results.JsonParser.Field;
import com.google.gson.Gson;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@SuppressWarnings("unused")
class JsonParserTest {
    static Stream<Arguments> testReadString() {
        return Stream.of(
                arguments("", 0, null), // no input
                arguments("\"", 0, null), // unterminated "
                arguments("\"x\"", 0, "x"), // short string
                arguments("\"0123 asd !@#,+-{}[]\"", 0, "0123 asd !@#,+-{}[]"), // long string with symbols
                arguments("\"0123 \uD83E\uDE02\"", 0, "0123 \uD83E\uDE02"), // unicode with surrogate char
                arguments("\"\"", 0, ""), // empty
                arguments("\"0123", 0, null), // unclosed string
                arguments("\"0123 \uD83E\uDE02", 0, null), // unclosed string with surrogate char
                arguments("\"\\n\\t\\\\\\/\\r\\b\"", 0, "\\n\\t\\\\\\/\\r\\b"), // non-" escapes
                arguments("\", \\\"\"", 0, ", \\\""), // escaped " at end
                arguments("\"\\\".\"", 0, "\\\"."), // escaped " at begin
                arguments("\"\\\".\\\"\"", 0, "\\\".\\\""), // escaped " at begin and end
                arguments(", \"x\"", 0, "x"), // leading sep
                arguments(" \n,\t \"x\\n\"", 0, "x\\n"), // leading sep with LF and tab
                arguments(" ,", 0, null), // didn't find start
                arguments(" ,\"", 0, null), // didn't find close
                arguments(" ,\"asd", 0, null), // didn't find close
                arguments(" ,\"asd\\\"", 0, null), // didn't find close (" is escaped)
                arguments("{", 0, "THROW"), // bad start
                arguments(", {", 0, "THROW"), // bad start after sep
                arguments("{\"x\"", 1, "x"), // use start
                arguments("{\"x\"", 2, "THROW"), // bad start causes throw
                arguments("\"x\", \"y\"", 5, "y"), // read second string
                arguments("\"x\", \"y\"", 3, "y") // read second string starting from sep
        );
    }

    @ParameterizedTest @MethodSource
    void testReadString(String input, int start, @Nullable String expected) {
        JsonParser parser = new JsonParser(new TestConsumer());
        parser.setupForTest(input, start, null, null, null, emptyList());
        boolean thrown = false;
        try {
            assertEquals(expected, parser.readString());
            assertNotEquals(expected, "THROW");
        } catch (JsonParser.SyntaxException e) {
            thrown = true;
        }
        assertEquals("THROW".equals(expected), thrown);
    }

    static Stream<Arguments> testAtField() {
        return Stream.of(
                arguments(asList("head", "vars"),
                          singletonList(Field.VARS), false),
                arguments(asList("head", "vars"),
                          asList(Field.HEAD, Field.VARS), true),
                arguments(asList("results", "bindings", "type"),
                          singletonList(Field.TYPE), false),
                arguments(asList("results", "bindings", "type"),
                          asList(Field.BINDINGS, Field.TYPE), false),
                arguments(asList("results", "bindings", "type"),
                          asList(Field.RESULTS, Field.TYPE), false),
                arguments(asList("results", "bindings", "type"),
                          asList(Field.RESULTS, Field.BINDINGS, Field.TYPE), true),
                arguments(asList("results", "whatever", "type"),
                          singletonList(Field.TYPE), false),
                arguments(asList("results", "whatever", "type"),
                          asList(Field.BINDINGS, Field.TYPE), false),
                arguments(asList("results", "whatever", "type"),
                          asList(Field.RESULTS, Field.BINDINGS, Field.TYPE), false),
                arguments(asList("results", "whatever", "type"),
                        asList(Field.RESULTS, Field.UNKNOWN, Field.TYPE), true)
        );
    }

    @ParameterizedTest @MethodSource
    void testAtField(List<String> state, List<Field> query, boolean expected) {
        JsonParser parser = new JsonParser(new TestConsumer());
        parser.setupForTest("", 0, null, null, null, state);
        assertEquals(expected, parser.atField(query.toArray(new Field[0])));
    }

    static Stream<Arguments> testAtKnownField() {
        return Stream.of(
                arguments(emptyList(), false),
                arguments(singletonList("x"), false),
                arguments(asList("head", "links"), false),
                arguments(asList("head", "vars"), true),
                arguments(singletonList("vars"), true),
                arguments(asList("head", "bindings"), true),
                arguments(asList("results", "bindings"), true)
        );
    }

    @ParameterizedTest @MethodSource
    void testAtKnownField(List<String> state, boolean expected) {
        JsonParser parser = new JsonParser(new TestConsumer());
        parser.setupForTest("", 0, null, null, null, state);
        assertEquals(expected, parser.atKnownField());
    }

    static Stream<Arguments> testToNT() {
        return Stream.of(
    /*  1 */    arguments("23", "http://www.w3.org/2001/XMLSchema#integer", null,
                          "\"23\"^^<http://www.w3.org/2001/XMLSchema#integer>"),
    /*  2 */    arguments("23", "http://www.w3.org/2001/XMLSchema#string", null,
                          "\"23\"^^<http://www.w3.org/2001/XMLSchema#string>"),
    /*  3 */    arguments("23", null, null, "\"23\""),
    /*  4 */    arguments("\\\"x", null, null, "\"\\\"x\""),
    /*  5 */    arguments("bob", null, "en", "\"bob\"@en"),
    /*  6 */    arguments("bob", null, "en-US", "\"bob\"@en-US"),
    /*  7 */    arguments("bob", null, "en_US", "\"bob\"@en-US"),

                // tolerate <> inside type
    /*  8 */    arguments("23", "<http://www.w3.org/2001/XMLSchema#string>", null,
                        "\"23\"^^<http://www.w3.org/2001/XMLSchema#string>"),
                // expand xsd: in type
    /*  9 */    arguments("23", "xsd:int", null, "\"23\"^^<http://www.w3.org/2001/XMLSchema#int>"),
                // expand rdf: in type
    /* 10 */    arguments("23", "rdf:XMLLiteral", null,
                          "\"23\"^^<http://www.w3.org/1999/02/22-rdf-syntax-ns#XMLLiteral>"),
                // replace _ xsd: in tag
    /* 11 */    arguments("bob", null, "en_US", "\"bob\"@en-US")
        );
    }

    @ParameterizedTest @MethodSource
    void testToNT(@Nullable String value, @Nullable String type, @Nullable String lang,
                  @Nullable String expected) {
        JsonParser parser = new JsonParser(new TestConsumer());
        parser.setupForTest("", 0, value, type, lang, emptyList());
        boolean thrown = false;
        try {
            assertEquals(expected, parser.takeNT());
        } catch (JsonParser.SyntaxException e) {
            thrown = true;
        }
        assertEquals(expected == null, thrown);
    }

    static Stream<Arguments> parseData() {
        List<List<String>> POSITIVE_ASK = singletonList(emptyList());
        List<Arguments> base = asList(
                // assume empty response
        /*  1 */arguments("[]", emptyList(), emptyList()),
        /*  2 */arguments("{}", emptyList(), emptyList()),
                // non-empty response with unexpected properties
        /*  3 */arguments("{\"x\": false}", null, null),
                // empty vars, no results
        /*  4 */arguments("{\"head\": {\"vars\": []}}", emptyList(), emptyList()),
                //empty vars, null results
        /*  5 */arguments("{\"head\": {\"vars\": []}, \"results\": null}",
                          emptyList(), emptyList()),
                //empty vars, null bindings
        /*  6 */arguments("{\"head\": {\"vars\": []}, \"results\": {\"bindings\": null}}",
                         emptyList(), emptyList()),
                //empty vars, empty bindings
        /*  7 */arguments("{\"head\": {\"vars\": []}, \"results\": {\"bindings\": []}}",
                         emptyList(), emptyList()),

                // single var, no bindings
        /*  8 */arguments("{\"head\": {\"vars\": [\"x\"]}, \"results\": {\"bindings\": []}}",
                         singletonList("x"), emptyList()),
                // two vars, no bindings
        /*  9 */arguments("{\"head\": {\"vars\": [\"x\", \"y\"]}, \"results\": {\"bindings\": []}}",
                          asList("x", "y"), emptyList()),

                // single var, single typed result
        /* 10 */arguments("{\"head\": {\"vars\": [\"x\"]}, \"results\": {\"bindings\": [{\"x\": {\"value\": \"23\", \"type\": \"http://www.w3.org/2001/XMLSchema#integer\"}}]}}",
                         singletonList("x"), singletonList(singletonList("\"23\"^^<http://www.w3.org/2001/XMLSchema#integer>"))),
                // single var, single lang-tagged result
        /* 11 */arguments("{\"head\": {\"vars\": [\"x\"]}, \"results\": {\"bindings\": [{\"x\": {\"value\": \"bob\", \"xml:lang\": \"en\"}}]}}",
                         singletonList("x"), singletonList(singletonList("\"bob\"@en"))),
                // single var, plain literal
        /* 12 */arguments("{\"head\": {\"vars\": [\"x\"]}, \"results\": {\"bindings\": [{\"x\": {\"value\": \"alice\"}}]}}",
                         singletonList("x"), singletonList(singletonList("\"alice\""))),
                // single var, three rows
        /* 13 */arguments("{\"head\": {\"vars\": [\"x\"]}, \"results\": {\"bindings\": [" +
                                "{\"x\": {\"value\": \"23\", \"type\": \"http://www.w3.org/2001/XMLSchema#int\"}}," +
                                "{\"x\": {\"value\": \"alice\", \"xml:lang\": \"en_US\"}}," +
                                "{\"x\": {\"value\": \"bob\"}}," +
                                "]}}",
                         singletonList("x"),
                         asList(singletonList("\"23\"^^<http://www.w3.org/2001/XMLSchema#int>"),
                                singletonList("\"alice\"@en-US"),
                                singletonList("\"bob\""))),
                //single var extra var on binding
        /* 14 */arguments("{\"head\": {\"vars\": [\"x\"]}, \"results\": {\"bindings\": [" +
                                "{\"x\": {\"value\": \"bob\"}, \"y\": {\"value\": \"wrong\"}}," +
                                "]}}",
                         singletonList("x"), null),
                //single var extra var on 2nd binding
        /* 15 */arguments("{\"head\": {\"vars\": [\"x\"]}, \"results\": {\"bindings\": [" +
                                "{\"x\": {\"value\": \"alice\"}}," +
                                "{\"x\": {\"value\": \"bob\"}, \"y\": {\"value\": \"wrong\"}}," +
                                "]}}",
                         singletonList("x"), asList(singletonList("\"alice\""), null)),
                //single var empty value object on 2nd
        /* 16 */arguments("{\"head\": {\"vars\": [\"x\"]}, \"results\": {\"bindings\": [" +
                                "{\"x\": {\"value\": \"alice\"}}," +
                                "{\"x\": {}}," +
                                "]}}",
                         singletonList("x"), asList(singletonList("\"alice\""), null)),
                //single var empty binding on 2nd
        /* 17 */arguments("{\"head\": {\"vars\": [\"x\"]}, \"results\": {\"bindings\": [" +
                                "{\"x\": {\"value\": \"bob\"}}," +
                                "{}" +
                                "]}}",
                         singletonList("x"), asList(singletonList("\"bob\""), singletonList(null))),
                //single var empty binding
        /* 18 */arguments("{\"head\": {\"vars\": [\"x\"]}, \"results\": {\"bindings\": [{}]}}",
                         singletonList("x"), singletonList(singletonList(null))),

                //two vars two rows with unbound on first
        /* 19 */arguments("{\"head\": {\"vars\": [\"x\", \"y\"]}, \"results\": {\"bindings\": [" +
                                "{\"x\": {\"value\": \"bob\", \"y\": null}}," +
                                "{\"x\": {\"value\": \"alice\", \"xml:lang\": \"en\"},"+
                                 "\"y\": {\"value\": \"charlie\"}}" +
                                "]}}",
                         asList("x", "y"),
                         asList(asList("\"bob\"", null),
                                asList("\"alice\"@en", "\"charlie\""))),

                //negative ask
        /* 20 */arguments("{\"head\": {}, \"boolean\": false}", emptyList(), emptyList()),
                //positive ask
        /* 21 */arguments("{\"head\": {}, \"boolean\": true}", emptyList(), POSITIVE_ASK),
                //no-head negative ask
        /* 22 */arguments("{\"boolean\": false}", emptyList(), emptyList()),
                //no-head positive ask
        /* 23 */arguments("{\"boolean\": true}", emptyList(), POSITIVE_ASK),
                //negative ask with links
        /* 24 */arguments("{\"head\": {\"link\": [\"http://example.com\"]}, \"boolean\": false}",
                          emptyList(), emptyList()),
                //positive ask with links
        /* 25 */arguments("{\"head\": {\"link\": [\"http://example.com\"]}, \"boolean\": true}",
                          emptyList(), POSITIVE_ASK),

                //negative ask typed as string
        /* 26 */arguments("{\"head\": {\"link\": [\"http://example.com\"]}, \"boolean\": \"false\"}",
                          emptyList(), emptyList()),
                //positive ask typed as string
        /* 27 */arguments("{\"head\": {\"link\": [\"http://example.com\"]}, \"boolean\": \"true\"}",
                          emptyList(), POSITIVE_ASK),
                //negative ask typed as camel string
        /* 28 */arguments("{\"head\": {\"link\": [\"http://example.com\"]}, \"boolean\": \"False\"}",
                          emptyList(), emptyList()),
                //positive ask typed as camel string
        /* 29 */arguments("{\"head\": {\"link\": [\"http://example.com\"]}, \"boolean\": \"True\"}",
                          emptyList(), POSITIVE_ASK),
                //negative ask typed as number
        /* 30 */arguments("{\"head\": {\"link\": [\"http://example.com\"]}, \"boolean\": 0}",
                          emptyList(), emptyList()),
                //positive ask typed as number
        /* 31 */arguments("{\"head\": {\"link\": [\"http://example.com\"]}, \"boolean\": 1}",
                          emptyList(), POSITIVE_ASK),
                //positive ask typed as unguessable string
        /* 32 */arguments("{\"head\": {\"link\": [\"http://example.com\"]}, \"boolean\": \"x\"}",
                          null, null),
                //positive ask typed as null
        /* 33 */arguments("{\"head\": {\"link\": [\"http://example.com\"]}, \"boolean\": null}",
                          emptyList(), emptyList())
        );
        List<Arguments> expanded = new ArrayList<>();
        for (Integer chunkSize : asList(Integer.MAX_VALUE, 8, 3, 1)) {
            for (Arguments a : base) {
                List<String[]> rows = null;
                //noinspection unchecked
                List<List<String>> list = (List<List<String>>) a.get()[2];
                if (list != null) {
                    rows = list.stream().map(l -> l == null ? null : l.toArray(new String[0]))
                               .collect(toList());
                }
                expanded.add(arguments(chunkSize, a.get()[0], a.get()[1], rows));
            }
        }
        return expanded.stream();
    }

    @ParameterizedTest @MethodSource("parseData")
    void testSanityJsonIsValid(int chunkSize, String json, List<String> vars, List<String[]> rows) {
        if (vars == null || rows == null || rows.contains(null))
            return;
        if (json.startsWith("["))
            return;
        assertDoesNotThrow(() -> new Gson().fromJson(json, Map.class));
    }

    private void doTestParse(Function<String, CharSequence> str2cs,
                             int chunkSize, String json, List<String> vars, List<String[]> rows) {
        TestConsumer consumer = new TestConsumer();
        JsonParser parser = new JsonParser(consumer);
        List<CharSequence> givenInputs = new ArrayList<>();
        List<String> savedInputs = new ArrayList<>();
        for (int i = 0, len = json.length(); i < len; i += chunkSize) {
            String substring = json.substring(i, Math.min(len, i + chunkSize));
            CharSequence cs = str2cs.apply(substring);
            givenInputs.add(cs);
            savedInputs.add(substring);
            parser.feed(cs);
        }
        parser.end();
        consumer.check(vars, rows);
        assertEquals(givenInputs.size(), savedInputs.size()); //sanity
        for (int i = 0; i < givenInputs.size(); i++)
            assertTrue(savedInputs.get(i).contentEquals(givenInputs.get(i)), "i=" + i);
    }

    @ParameterizedTest @MethodSource("parseData")
    void testParse(int chunkSize, String json, List<String> vars, List<String[]> rows) {
        doTestParse(s -> s, chunkSize, json, vars, rows);
    }

    @ParameterizedTest @MethodSource("parseData")
    void testParseStringBuilder(int chunkSize, String json, List<String> vars, List<String[]> rows) {
        doTestParse(StringBuilder::new, chunkSize, json, vars, rows);
    }
}