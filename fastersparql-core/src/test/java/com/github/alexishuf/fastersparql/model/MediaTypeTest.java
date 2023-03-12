package com.github.alexishuf.fastersparql.model;

import com.github.alexishuf.fastersparql.exceptions.InvalidMediaType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@SuppressWarnings("unused")
class MediaTypeTest {
    private static final String COMPACTED = "http://www.w3.org/ns/json-ld#compacted";

    static Stream<Arguments> testParse() {
        Map<String, String> cs = new HashMap<>(), csq = new HashMap<>();
        cs.put("charset", "utf-8");
        csq.put("charset", "utf-8");
        csq.put("q", "0.9");
        return Stream.of(
                arguments("text/csv", new MediaType("text", "csv", emptyMap())),
                arguments("text/*", new MediaType("text", "*", emptyMap())),
                arguments("*/*", new MediaType("*", "*", emptyMap())),
                arguments("text/tab-separated-values",
                          new MediaType("text", "tab-separated-values", emptyMap())),
                arguments("application/sparql-results+json",
                          new MediaType("application", "sparql-results+json",
                                         emptyMap())),
                arguments("text/csv; charset=utf-8",
                          new MediaType("text", "csv", singletonMap("charset", "utf-8"))),
                arguments("text/csv; q=0.9; charset=utf-8",
                          new MediaType("text", "csv", csq)),
                arguments("text/csv; charset=utf-8; q=0.9",
                          new MediaType("text", "csv", csq)),
                arguments("text/csv;", new MediaType("text", "csv", emptyMap())),
                arguments("text/csv ;", new MediaType("text", "csv", emptyMap())),
                arguments("text/csv; ", new MediaType("text", "csv", emptyMap())),
                arguments("text/csv\t;\r\n", new MediaType("text", "csv", emptyMap())),
                arguments("text/*;", new MediaType("text", "*", emptyMap())),
                arguments("text/* ;", new MediaType("text", "*", emptyMap())),
                arguments("text/*; ", new MediaType("text", "*", emptyMap())),
                arguments("text/*\t;\r\n", new MediaType("text", "*", emptyMap())),
                arguments("*/*;", new MediaType("*", "*", emptyMap())),
                arguments("*/* ;", new MediaType("*", "*", emptyMap())),
                arguments("*/*; ", new MediaType("*", "*", emptyMap())),
                arguments("*/*\t;\r\n", new MediaType("*", "*", emptyMap())),
                arguments("text/csv; charset=utf-8;",
                          new MediaType("text", "csv", cs)),
                arguments("text/csv; charset=utf-8; ",
                          new MediaType("text", "csv", cs)),
                arguments("text/csv; charset=utf-8 ; ",
                          new MediaType("text", "csv", cs)),
                arguments("application/ld+json; profile=\""+ COMPACTED +"\"",
                          new MediaType("application", "ld+json",
                                        singletonMap("profile", COMPACTED))),
                arguments("text/x-vnd-dummy; par=\"\\\"\"",
                          new MediaType("text", "x-vnd-dummy",
                                        singletonMap("par", "\"")))
                );
    }

    @ParameterizedTest @MethodSource
    void testParse(String input, MediaType expected) {
        assertEquals(expected, MediaType.parse(input));
    }

    @ParameterizedTest @ValueSource(strings = {
            "",
            " ",
            "\r\n",
            " \t \r\n",
            "; text/csv",
            ", text/csv",
            ", text/csv",
            "text=csv",
            "text-csv",
            "text/csv; charset",
            "text/csv; charset=",
            "text/csv; charset =",
            "text/csv; charset = ",
            "text/csv; charset = \t\r\n",
            "*/*; charset = \t\r\n",
            "text/csv, text/*",
            "text/csv, text/tab-separated-values"
    })
    void testInvalidMediaTypes(String input) {
        assertThrows(InvalidMediaType.class, () -> MediaType.parse(input));
        assertNull(MediaType.tryParse(input));
    }

    @Test
    void testParseNull() {
        assertThrows(InvalidMediaType.class, () -> MediaType.parse(null));
        assertNull(MediaType.tryParse(null));
    }

    @ParameterizedTest @ValueSource(strings = {
    /*  1 */"text/csv | text/csv",
    /*  2 */"text/csv | text / csv",
    /*  3 */"text/csv | text \r\n/ csv",
    /*  4 */"text/csv | text \r\n/\t csv",
    /*  5 */"text/csv | text \r\n/\t csv ",
    /*  6 */"text/csv | text \r\n/\t csv\r\n",
    /*  7 */"text/csv | text \r\n/\t csv, ",
    /*  8 */"text/csv | text \r\n/\t csv , ",
    /*  9 */"text/csv | \ttext \r\n/\t csv , ",
    /* 10 */"text/csv | \ttext/csv",
    /* 11 */"text/tab-separated-values | text/tab-separated-values",
    /* 12 */"text/tab-separated-values | text /tab-separated-values",
    /* 13 */"text/tab-separated-values | text/ tab-separated-values",
    /* 14 */"text/tab-separated-values | \ttext/ tab-separated-values",
    /* 15 */"text/tab-separated-values | text/tab-separated-values\r\n",
    /* 16 */"text/tab-separated-values | text/tab-separated-values,\r\n",
    /* 17 */"application/sparql-results+json | application/sparql-results+json",
    /* 18 */"application/sparql-results+json | application /sparql-results+json",
    /* 19 */"application/sparql-results+json | application/ sparql-results+json",
    /* 20 */"application/sparql-results+json | \tapplication/sparql-results+json",
    /* 21 */"application/sparql-results+json | application/sparql-results+json, ",
    /* 22 */"text/csv; q=0.9 | text/csv; q=0.9",
    /* 23 */"text/csv; q=0.9 | text/csv; q= 0.9",
    /* 24 */"text/csv; q=0.9 | text/csv; q = 0.9",
    /* 25 */"text/csv; q=0.9 | text/csv;  q = 0.9",
    /* 26 */"text/csv; q=0.9 | text/csv;\t  q = 0.9",
    /* 27 */"text/csv; q=0.9 | text/csv;\t\r\n  q = 0.9",
    /* 28 */"text/csv; q=0.9 | text/csv ;\t\r\n  q = 0.9",
    /* 29 */"text/csv; q=0.9 | text/csv\t ;\t\r\n  q = 0.9",
    /* 30 */"text/csv; q=0.9 | text/csv\t\r\n ;\t\r\n  q = 0.9",
    /* 31 */"text/csv; q=0.9; charset=utf-8 | text/csv; q = 0.9; charset = utf-8",
    /* 32 */"text/csv; q=0.9; charset=utf-8 | text/csv; q = 0.9; charset = UTF-8",
    /* 33 */"text/csv; q=0.9; charset=utf-8 | text/csv ; q = 0.9 ; charset\r\n =\t UTF-8",
    /* 34 */"text/csv; q=0.9; charset=utf-8 | text/csv; charset=utf-8; q=0.9",
    /* 35 */"text/csv; q=0.9; charset=utf-8 | text/csv ; charset= utf-8; q\r\n=\t0.9",
    /* 36 */"text/x-vnd-dummy; par=\"1\" | text/x-vnd-dummy; par=1",
    /* 37 */"text/x-vnd-dummy; par = \"x\" | text/x-vnd-dummy; par=x",
    /* 38 */"text/x-vnd-dummy; par=\"\\\"\" | text/x-vnd-dummy; par=\"\\\"\""
    })
    void testEquals(String testData) {
        String[] segments = testData.split(" *\\| *");
        assertEquals(MediaType.parse(segments[0]), MediaType.parse(segments[1]));
    }

    @ParameterizedTest @ValueSource(strings = {
        "text/csv | text/*",
        "text/csv | */*",
        "*/csv    | */*",
        "text/csv | text/csv; q=0.9",
        "text/csv; q=0.8                | text/csv; q=0.9",
        "text/csv; charset=utf-8        | text/csv; q=0.9",
        "text/csv; q=0.9; charset=utf-8 | text/csv; q=0.9",
        "text/csv; charset=utf-8; q=0.9 | text/csv; q=0.9",
        "text/csv; charset=utf-8        | text/tsv; charset=utf-8",
        "text/csv; charset=utf-8        | text/*; charset=utf-8",
        "text/*; charset=utf-8          | text/csv; charset=utf-8",
        "*/*; charset=utf-8             | text/csv; charset=utf-8",
    })
    void testNotEquals(String testData) {
        String[] segments = testData.split(" *\\| *");
        assertNotEquals(MediaType.parse(segments[0]), MediaType.parse(segments[1]));
    }

    @ParameterizedTest @ValueSource(strings = {
            "text/csv                | text/csv",
            "text/csv                | text/csv; charset=utf-8",
            "text/csv; charset=utf-8 | text/csv; charset=utf-8",
            "text/*                  | text/csv",
            "*/*                     | text/csv",
            "*/csv                   | text/csv",
            "text/*; charset=utf-8   | text/csv; charset=utf-8",
            "*/*; charset=utf-8      | text/csv; charset=utf-8",
            "*/csv; charset=utf-8    | text/csv; charset=utf-8"
    })
    void testAccepts(String testData) {
        String[] segments = testData.split(" *\\| *");
        assertTrue(MediaType.parse(segments[0]).accepts(MediaType.parse(segments[1])));
        assertTrue(MediaType.parse(segments[1]).acceptedBy(MediaType.parse(segments[0])));
    }

    @ParameterizedTest @ValueSource(strings = {
            "text/csv                | text/*",
            "text/csv                | */*",
            "text/csv                | */csv",
            "text/csv; charset=utf-8 | text/csv",
            "text/csv; charset=utf-8 | text/*",
            "text/csv; charset=utf-8 | text/*; charset=utf-8",
            "text/csv; charset=utf-8 | */csv; charset=utf-8"
    })
    void testNotAccepts(String testData) {
        String[] segments = testData.split(" *\\| *");
        assertFalse(MediaType.parse(segments[0]).accepts(MediaType.parse(segments[1])));
        assertFalse(MediaType.parse(segments[1]).acceptedBy(MediaType.parse(segments[0])));
    }

    @ParameterizedTest @ValueSource(strings = {
            "text/csv |           | text/csv",
            "text/csv | q         | text/csv",
            "text/csv | q,charset | text/csv",
            "text/*   | q,charset | text/*",
            "*/*      | q,charset | */*",

            "text/csv; charset=utf-8 | charset,q | text/csv",
            "text/csv; charset=utf-8 | q         | text/csv; charset=utf-8",

            "application/sparql-results+json; q=0.9 | charset,q | application/sparql-results+json",
            "application/sparql-results+json; q=0.9 | q         | application/sparql-results+json",
            "application/sparql-results+json; q=0.9 | q,q,      | application/sparql-results+json",
            "application/sparql-results+json; q=0.9 | charset   | application/sparql-results+json; q=0.9",
            "application/sparql-results+json; q=0.9 | charset,  | application/sparql-results+json; q=0.9",
            "application/sparql-results+json; q=0.9 | ,q        | application/sparql-results+json",
    })
    void testWithoutParams(String testData) {
        String[] segments = testData.split(" *\\| *");
        MediaType input = MediaType.parse(segments[0]);
        MediaType inputCopy = MediaType.parse(segments[0]);
        MediaType expected = MediaType.parse(segments[2]);
        List<String> list = Arrays.asList(segments[1].split(","));

        assertEquals(expected, input.withoutParams(list));
        assertEquals(inputCopy, input);

        if (expected.params().isEmpty())
            assertEquals(expected, input.withoutParams());
    }

    @ParameterizedTest @ValueSource(strings = {
            "text/csv             | text/csv",
            "text/csv,            | text/csv",
            "text/csv ,           | text/csv",
            "text/*               | text/*",
            "*/*                  | */*",
            "text/csv; q=0.9      | text/csv; q=0.9",
            "text/*; q=0.9        | text/*; q=0.9",
            "*/*; q=0.9           | */*; q=0.9",
            "text / csv           | text/csv",
            "text / csv; q=0.9    | text/csv; q=0.9",
            "text / csv; q=0.9,   | text/csv; q=0.9",
            "text / csv; q=0.9 ,  | text/csv; q=0.9",
            "text / csv; q = 0.9  | text/csv; q=0.9",
            "text /\t csv; q\t= 1 | text/csv; q=1",
            "Text /\t CSV; q\t= 1 | text/csv; q=1",
            "Text /\t tab-separated-values; q\t= 1   | text/tab-separated-values; q=1",
            "Text /\t tab-separated-values; q\t= 1,  | text/tab-separated-values; q=1",
            "Text /\t tab-separated-values; q\t= 1 , | text/tab-separated-values; q=1",
            "text/csv; charset=utf-8; q=0.9          | text/csv; charset=utf-8; q=0.9",
            "text/csv; charset=utf-8; q=0.9,         | text/csv; charset=utf-8; q=0.9",
            "text/csv; charset=utf-8; q=0.9 ,        | text/csv; charset=utf-8; q=0.9",
            "text/csv; charset=UTF-8; q=0.9          | text/csv; charset=utf-8; q=0.9",
            "text/csv; \nq=0.9\t; charset=UTF-8      | text/csv; q=0.9; charset=utf-8",
            "text/csv; \nq=0.9\t; charset=UTF-8,     | text/csv; q=0.9; charset=utf-8",
            "text/csv; \nq=0.9\t; charset=UTF-8 ,    | text/csv; q=0.9; charset=utf-8"
    })
    void testSanitizeMediaType(String testData) {
        String[] segments = testData.split(" *\\| *");
        MediaType expected = MediaType.parse(segments[1]);
        assertEquals(expected, MediaType.parse(segments[0]));
        assertEquals(expected, MediaType.parse(segments[0]+" "));
        assertEquals(expected, MediaType.parse(segments[0]+"\r\n"));
    }

    @ParameterizedTest @ValueSource(strings = {
            "text/csv     | text/csv",
            "text/*       | text/*",
            "*/*          | */*",
            "text/csv; charset=utf-8          | text/csv; charset=utf-8",
            "text/csv; charset= utf-8         | text/csv; charset=utf-8",
            "text/csv; charset = UTF-8        | text/csv; charset=utf-8",
            "text/csv ; charset\t\r\n = utf-8 | text/csv; charset=utf-8",
            "application/sparql-results+json; q=0.9; charset=utf-8           | application/sparql-results+json; q=0.9; charset=utf-8",
            "application/sparql-results+json; q=0.9; charset= UTF-8          | application/sparql-results+json; q=0.9; charset=utf-8",
            "application/sparql-results+json; q=0.9; charset = utf-8         | application/sparql-results+json; q=0.9; charset=utf-8",
            "application/sparql-results+json; q\t=0.9; charset = utf-8       | application/sparql-results+json; q=0.9; charset=utf-8",
            "application/sparql-results+json; q\t=\r\n0.9; charset = utf-8   | application/sparql-results+json; q=0.9; charset=utf-8",
            "application/sparql-results+json ; q\t=\r\n0.9 ; charset = utf-8 | application/sparql-results+json; q=0.9; charset=utf-8",
            "application/ld+json; profile=\""+COMPACTED+"\"                   | application/ld+json; profile=\""+COMPACTED+"\""
    })
    void testToString(String testData) {
        String[] segments = testData.split(" *\\| *");
        MediaType parsed = MediaType.parse(segments[0]);
        assertEquals(segments[1], parsed.toString());
        assertEquals(segments[1], parsed.normalized());
    }

    @ParameterizedTest @ValueSource(strings = {
            "asd|asd",
            "|\"\"",
            " |\" \"",
            "\t|\"\t\"",
            "\r\n|\"\r\n\"",
            " x\r\n|\" x\r\n\"",
            " x/;,\r\n|\" x/;,\r\n\"",
            "x\"y|\"x\\\"y\"",
            "\"|\"\\\"\"",
            "\"\"|\"\\\"\\\"\"",
            "1\"2\"3|\"1\\\"2\\\"3\"",
            "\\\"|\"\\\\\\\"\"",
            "\"asd|\"\\\"asd\""
    })
    void testQuote(String dataString) {
        String[] data = dataString.split("\\|");
        String input = data[0], expected = data[1];
        String actual = MediaType.quote(input);
        assertEquals(expected, actual);
        if (expected.equals(input))
            assertSame(actual, input);

    }

    @ParameterizedTest @ValueSource(strings = {
            "asd|asd",
            "\\\\|\\",
            "\\\"|\"",
            "1\\\"2\\\"3|1\"2\"3",
            "begin\\\"inner\\\"end|begin\"inner\"end",
            "\\\\\\\"|\\\"",
            " | ",
            "\t |\t ",
            "\t \r\n|\t \r\n",
            "1 + 2 / 3; x=1,|1 + 2 / 3; x=1,"
    })
    void testUnquote(String dataString) {
        String[] data = dataString.split("\\|");
        String actual = MediaType.unquote(data[0]);
        assertEquals(data[1], actual);
        if (data[1].equals(data[0]))
            assertSame(data[0], actual);
    }
}