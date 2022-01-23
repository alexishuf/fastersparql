package com.github.alexishuf.fastersparql.client.parser.results;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.client.parser.results.CSVParser.findCloseQuote;
import static com.github.alexishuf.fastersparql.client.parser.results.CSVParser.findSep;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class CSVParserTest {

    @ParameterizedTest @ValueSource(strings = {
            "1  | 1 | \"\"",
            "-1 | 2 | \"\"",
            "-1 | 0 | asd",
            "-1 | 1 | asd",
            "-1 | 1 | \"asd",
            "5  | 1 | \"text\"",
            "5  | 1 | \"text\"extra",
            "14 | 1 | \" \"\"escaped\"\" \"",
            "14 | 1 | \" \\\"escaped\\\" \"",
            "12 | 1 | \"\"\"escaped\"\"\"",
            "12 | 1 | \"\\\"escaped\\\"\"",
            "5  | 1 | \"\"\"\"\"\"",
            "5  | 1 | \"\\\"\\\"\"",
            "5  | 3 | \"\"\"\"\"\"",
            "5  | 3 | \"\\\"\\\"\"",
            "5  | 5 | \"\"\"\"\"\"",
            "-1 | 6 | \"\"\"\"\"\"",
            "-1 | 4 | \"\"\"\"\"\"",
            "5  | 5 | \"\\\"\\\"\"",
            "-1 | 6 | \"\\\"\\\"\"",
            "-1 | 4 | \"\\\"\\\"\"",
    })
    void testFindCloseQuote(String dataString) {
        String[] data = dataString.split(" *\\| ");
        int expected = Integer.parseInt(data[0]);
        int from = Integer.parseInt(data[1]);
        for (String suffix : asList(",", "\r\n", " ")) {
            String input = data[2] + suffix;
            int expectedWithSuffix = expected == -1 ? input.length() : expected;
            assertEquals(expectedWithSuffix, findCloseQuote(input, from));
            assertEquals(expectedWithSuffix, findCloseQuote(new StringBuilder(input), from));
        }
    }

    @ParameterizedTest @ValueSource(strings = {
            "2  | 2 | \"\",",
            "0  | 0 | ",
            "-1 | 0 | .",
            "1  | 1 | .",
            "3  | 3 | \"\",",
            "3  | 2 | \"\" ,",
            "3  | 2 | \"\"\t,",
            "3  | 2 | \"\"\n,",
            "3  | 2 | \"\"\r,",
            "4  | 2 | \"\"\r ,",
            "4  | 2 | \"\"\r\r,",
            "2  | 2 | \"\"\r\n,",
            "-1 | 3 | \"x\".,",
            "4  | 3 | \"x\"\r",
            "-1 | 3 | \"x\"\r.",
            "-1 | 2 | \"x\",",
    })
    void testFindSep(String dataString) throws AbstractSVResultsParser.SyntaxException {
        String[] data = dataString.split(" *\\| ");
        int expected = Integer.parseInt(data[0]), from = Integer.parseInt(data[1]);
        String input = data.length < 3 ? "" : data[2];
        if (expected == -1) {
            Class<? extends Throwable> exClass = AbstractSVResultsParser.SyntaxException.class;
            assertThrows(exClass, () -> findSep(input, from));
            assertThrows(exClass, () -> findSep(new StringBuilder(input), from));
        } else {
            assertEquals(expected, findSep(input, from));
            assertEquals(expected, findSep(new StringBuilder(input), from));
        }
    }

    static Stream<Arguments> testUnquote() {
        return Stream.of(
                arguments("", 0, 0, ""),
                arguments("\"\"", 1, 1, ""),
                arguments("\"\"\"", 1, 2, null),
                arguments("\"\"", 1, 2, null),
                arguments("\"a\"", 1, 2, "a"),
                arguments("\"http://example.org/search?x=23&y=[]\"", 1, 1+35, "http://example.org/search?x=23&y=[]"),
                arguments("\",\"", 1, 2, ","),
                arguments("\"\t\"", 1, 2, "\\t"),
                arguments("\"\r\"", 1, 2, "\\r"),
                arguments("\"\r\n\"", 1, 3, "\\r\\n"),
                arguments("\"\\\"\"", 1, 3, "\\\""),
                arguments("\"\\\"\"", 1, 3, "\\\""),
                arguments("\"\"\"\"", 1, 3, "\\\""),
                arguments("\"some \"\"thing\\\", \r\nand\t.\"", 1, 24, "some \\\"thing\\\", \\r\\nand\\t.")
        );
    }

    @ParameterizedTest @MethodSource
    void testUnquote(String input, int begin, int end,
                     @Nullable String expected) throws AbstractSVResultsParser.SyntaxException {
        CSVParser parser = new CSVParser(new TestConsumer());
        try {
            assertEquals(expected, parser.unquote(input, begin, end).toString());
            StringBuilder builder = new StringBuilder(input);
            assertEquals(expected, parser.unquote(builder, begin, end).toString());
            assertEquals(input, builder.toString(), "builder was mutated");
            if (expected == null)
                fail("Expected AbstractSVResultsParser.SyntaxException to be thrown");
        } catch (AbstractSVResultsParser.SyntaxException e) {
            if (expected != null)
                throw e;
        }
    }

    static Stream<Arguments> testToNt() {
        return Stream.of(
                arguments("http://example.org/Alice",   "<http://example.org/Alice>"),
                arguments("https://example.org/Secure", "<https://example.org/Secure>"),
                arguments("",     "\"\""),
                arguments("bob",  "\"bob\""),
                arguments("23",   "\"23\""),
                arguments("\\\"", "\"\\\"\""),
                arguments("_:b0", "_:b0"),
                arguments("_:",   "_:")
        );
    }

    @ParameterizedTest @MethodSource
    void testToNt(String input, String expected) {
        CSVParser parser = new CSVParser(new TestConsumer()) {
            @Override protected boolean atHeaders() { return false; }
        };
        int len = input.length();
        assertEquals(expected, parser.toNt(input, 0, len).toString());
        assertEquals(expected, parser.toNt(new StringBuilder(input), 0, len).toString());
        assertEquals(expected, parser.toNt("\""+input, 1, len +1).toString());
        assertEquals(expected, parser.toNt(input+"\"", 0, len).toString());
        assertEquals(expected, parser.toNt("\""+input+"\"", 1, len +1).toString());
    }

    static Stream<Arguments> parseData() {
        List<Arguments> list = Stream.of(
                // ask queries
        /*  1 */arguments("", emptyList(), emptyList()),
        /*  2 */arguments("\r\n", emptyList(), emptyList()),
        /*  3 */arguments("\r\n", emptyList(), emptyList()),
        /*  4 */arguments("\r\n\r\n", emptyList(), singletonList(emptyList())),
                // parse vars (empty results)
        /*  5 */arguments("x\r\n", singletonList("x"), emptyList()),
        /*  6 */arguments("x,y\r\n", asList("x", "y"), emptyList()),
        /*  7 */arguments("\"x\",\"y\"\r\n", asList("x", "y"), emptyList()),
        /*  8 */arguments("\"x & b\",\"y\"\r\n", asList("x & b", "y"), emptyList()),
        /*  9 */arguments("\" x \",\"y\"\r\n", asList("x", "y"), emptyList()),
        /* 10 */arguments("\",\",\"y\"\r\n", asList(",", "y"), emptyList()),
                // single-column, single-row
        /* 11 */arguments("x\r\nbob\r\n", singletonList("x"), singletonList(singletonList("\"bob\""))),
        /* 12 */arguments("x\r\n23\r\n", singletonList("x"), singletonList(singletonList("\"23\""))),
        /* 13 */arguments("x\r\nhttp://example.org/Alice\r\n", singletonList("x"), singletonList(singletonList("<http://example.org/Alice>"))),
        /* 14 */arguments("x\r\n\"http://example.org/Alice\"\r\n", singletonList("x"), singletonList(singletonList("<http://example.org/Alice>"))),
        /* 15 */arguments("x\r\n_:b0\r\n", singletonList("x"), singletonList(singletonList("_:b0"))),
                // multi-column
        /* 16 */arguments("x,y\r\n23,bob\r\n", asList("x", "y"), singletonList(asList("\"23\"", "\"bob\""))),
        /* 17 */arguments("x,y\r\n23,bob\r\n",     asList("x", "y"), singletonList(asList("\"23\"", "\"bob\""))),
        /* 18 */arguments("x,y\r\n_:,http://example.org/Alice\r\n",     asList("x", "y"), singletonList(asList("_:", "<http://example.org/Alice>"))),
                // multi-row and multi-column
        /* 19 */arguments("x,y\r\n23,bob\r\n\"a,\"\"b\"\"\",http://example.org/Alice\r\n",
                          asList("x", "y"), asList(
                                  asList("\"23\"", "\"bob\""),
                                  asList("\"a,\\\"b\\\"\"", "<http://example.org/Alice>")
                          )),
        /* 20 */arguments("x,y\r\n\"Line1\r\nLine2\",23\r\n_:,a thing\r\n", asList("x", "y"), asList(
                        asList("\"Line1\\r\\nLine2\"", "\"23\""),
                        asList("_:", "\"a thing\"")
                )),
                // Unescaped "
        /* 21 */arguments("x\r\n\"an \"\"a\".\"\r\n", singletonList("x"), null),
                //unterminated "
        /* 22 */arguments("x\r\n\"asd\r\n", singletonList("x"), null),
        /* 23 */arguments("x\r\n\"\r\n", singletonList("x"), null),
        /* 24 */arguments("\"x\r\n", null, null),
                // extra column
        /* 25 */arguments("x\r\n1,2\r\n", singletonList("x"), null),
                // extra column, second line
        /* 26 */arguments("x\r\n11\r\n21,22\r\n", singletonList("x"),
                          asList(singletonList("\"11\""), null)),
                // positive ask with explicit boolean
        /* 27 */arguments("_askResult\r\ntrue\r\n", emptyList(), singletonList(emptyList())),
                // positive ask with explicit boolean, quoted
        /* 28 */arguments("_askResult\r\n\"true\"", emptyList(), singletonList(emptyList())),
                // negative ask with explicit boolean
        /* 29 */arguments("_askResult\r\nfalse\r\n", emptyList(), emptyList()),
                // negative ask with explicit boolean, quoted
        /* 30 */arguments("_askResult\r\n\"false\"", emptyList(), emptyList()),
                // negative ask with no rows
        /* 31 */arguments("_askResult\r\n", emptyList(), emptyList())
        ).map(a -> {
            //noinspection unchecked
            List<List<String>> lists = (List<List<String>>) a.get()[2];
            List<String[]> arrays = null;
            if (lists != null) {
                arrays = lists.stream().map(l ->
                        l == null ? null : l.toArray(new String[0])).collect(toList());
            }
            return arguments(a.get()[0], a.get()[1], arrays);
        }).collect(toList());
        return Stream.of(Integer.MAX_VALUE, 1, 2, 3, 4, 8, 16).flatMap(chunk ->
                list.stream().map(a -> arguments(chunk, a.get()[0], a.get()[1], a.get()[2])));
    }

    @ParameterizedTest @MethodSource("parseData")
    void testParseString(int chunk, String csv, @Nullable List<String> vars,
                   @Nullable List<String[]> rows) {
        doTestParse(chunk, csv, vars, rows, String::new);
    }

    @ParameterizedTest @MethodSource("parseData")
    void testParseStringBuilder(int chunk, String csv, @Nullable List<String> vars,
                   @Nullable List<String[]> rows) {
        doTestParse(chunk, csv, vars, rows, StringBuilder::new);
    }

    private void doTestParse(int chunk, String csv, @Nullable List<String> vars,
                             @Nullable List<String[]> rows,
                             Function<String, CharSequence> transformer) {
        TestConsumer consumer = new TestConsumer();
        CSVParser parser = new CSVParser(consumer);
        for (int i = 0; i < csv.length(); i += chunk) {
            String substring = csv.substring(i, Math.min(csv.length(), i + chunk));
            parser.feed(transformer.apply(substring));
        }
        parser.end();
        consumer.check(vars, rows);
    }
}