package com.github.alexishuf.fastersparql.client.parser.fragment;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.charset.Charset;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

class StringFragmentParserTest {
    private static final List<String> STRINGS = asList("test", "rook \uD83E\uDE02");

    @Test
    void testParseStrings() {
        List<String> actual = STRINGS.stream()
                .map(s -> StringFragmentParser.INSTANCE.parseString(s, UTF_8)).toList();
        assertEquals(STRINGS, actual);
        for (int i = 0; i < STRINGS.size(); i++)
            assertSame(STRINGS.get(i), actual.get(i));
    }

    @ParameterizedTest @ValueSource(strings = {"utf-8", "utf-16"})
    void testParseBytes(String csName) {
        Charset cs = Charset.forName(csName);
        List<String> actual = STRINGS.stream()
                .map(s -> s.getBytes(cs))
                .map(b -> StringFragmentParser.INSTANCE.parseBytes(b, cs)).toList();
        assertEquals(STRINGS, actual);
    }
}