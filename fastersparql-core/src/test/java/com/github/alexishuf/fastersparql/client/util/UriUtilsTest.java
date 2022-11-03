package com.github.alexishuf.fastersparql.client.util;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static com.github.alexishuf.fastersparql.client.util.UriUtils.escapeQueryParam;
import static org.junit.jupiter.api.Assertions.*;

class UriUtilsTest {
    @ParameterizedTest
    @ValueSource(strings = {
            "\t|-1",
            "\0|-1",
            " |-1",
            "&|-1",
            "/|-1",
            "0| 0",
            "1| 1",
            "2| 2",
            "3| 3",
            "4| 4",
            "5| 5",
            "6| 6",
            "7| 7",
            "8| 8",
            "9| 9",
            "a| 10",
            "b| 11",
            "c| 12",
            "d| 13",
            "e| 14",
            "f| 15",
            "g|-1",
            "z|-1",
            "{|-1",
            "\u007F|-1",
            "A| 10",
            "B| 11",
            "C| 12",
            "D| 13",
            "E| 14",
            "F| 15",
            "G|-1",
            "Z|-1",
            "~|-1"
    })
    void testHexValue(String dataString) {
        String[] data = dataString.split("\\|\\s*");
        char c = data[0].charAt(0);
        int expected = Integer.parseInt(data[1]);
        byte actual = UriUtils.hexValue(c);
        if (expected == -1) {
            assertTrue(actual < 0, "expected < 0, got "+actual);
        } else {
            assertEquals(expected, actual);
        }
    }

    @Test
    void testNullStringNeedsEscape() {
        assertFalse(UriUtils.needsEscape(null));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "false|asd",
            "false|0",
            "false|~user",
            "false|asd%20qwe",
            "false|%20",
            "false|%7C",
            "false|%7Ca",
            "false|a%7Ca",
            "true |asd qwe",
            "true |x=2",
            "true |1 2",
            "true |1\t2",
            "true |1%gg",
            "true |1%GE",
            "true |1%0G",
            "true |1%0z",
            "true |1%fz",
            "true |1%fx",
            "true |1%fX",
            "true |1%2",
            "true |%gg",
            "true |%GE",
            "true |%0G",
            "true |%0z",
            "true |%fz",
            "true |%fx",
            "true |%fX",
            "true |%2",
            "true |%",
    })
    void testStringNeedsEscape(@NonNull String dataString) {
        boolean expected = Boolean.parseBoolean(dataString.replaceAll("^(true|false).*$", "$1"));
        String input = dataString.replaceAll("^(true|false)\\s*\\|", "");
        assertEquals(expected, UriUtils.needsEscape(input));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "asd|asd",
            "1 2|1%202",
            " |%20",
            "%20|%20",
            "%|%25",
            "%25|%25",
            "%2|%252",
            "%252|%252",
            "%2x|%252x",
            "%25x|%25x",
            "1%202|1%202",
            "1:2|1%3A2",
            "1%3A2|1%3A2",
            "<\nhttps://host/path?query={}>|%3C%0Ahttps%3A%2F%2Fhost%2Fpath%3Fquery%3D%7B%7D%3E",
    })
    void testEscapeString(@NonNull String dataString) {
        String    input = dataString.replaceAll("\\|.*$", "");
        String expected = dataString.substring(dataString.indexOf('|')+1);
        assertEquals(expected, escapeQueryParam(input));

        StringBuilder builder = new StringBuilder(input);
        assertEquals(expected, escapeQueryParam(builder).toString());
        assertEquals(input, builder.toString(), "builder mutated by escapeQueryParam()");

        String prefixed = "%" + input;
        String prefixedEscaped = UriUtils.escapeQueryParam(prefixed, 1, prefixed.length()).toString();
        assertEquals(expected, prefixedEscaped);

        String suffixed = input+"%";
        String suffixedEscaped = UriUtils.escapeQueryParam(suffixed, 0, suffixed.length()-1).toString();
        assertEquals(expected, suffixedEscaped);

        StringBuilder output = new StringBuilder();
        assertEquals(expected, escapeQueryParam(output, builder).toString());
        assertEquals(input, builder.toString(), "builder mutated by escapeQueryParam");

        assertEquals(expected, escapeQueryParam(null, builder).toString());
        assertEquals(input, builder.toString(), "builder mutated by escapeQueryParam");

        assertEquals(expected, escapeQueryParam(null, prefixed, 1, prefixed.length()).toString());
        assertEquals(expected, escapeQueryParam(null, suffixed, 0, suffixed.length()-1).toString());
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "asd",
            "qwe",
            "~user-one",
            "~user%20one",
    })
    void testEscapeStringNoOp(@NonNull String input) {
        assertSame(input, escapeQueryParam(input));
        StringBuilder builder = new StringBuilder(input);
        assertSame(builder, escapeQueryParam(builder));
    }

    @ParameterizedTest @ValueSource(strings = {
            "asd|asd",
            "x%20x|x x",
            "x%20y|x y",
            "x%202|x 2",
            "%22%3C%3F%3E%22|\"<?>\"",
            "%22%3c%3f%3e%22|\"<?>\"",
    })
    void testUnescape(String dataString) {
        String[] data = dataString.split("\\|");
        assertEquals(data[1], UriUtils.unescape(data[0]));
    }

}