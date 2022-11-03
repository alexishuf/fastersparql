package com.github.alexishuf.fastersparql.client.parser.fragment;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

class CharSequenceFragmentParserTest {
    private static final List<String> STRINGS = Arrays.asList("test", "rook \uD83E\uDE02");

    @Test
    void testParseStrings() {
        List<CharSequence> actual = STRINGS.stream()
                .map(s -> CharSequenceFragmentParser.INSTANCE.parseString(s, UTF_8)).toList();
        assertEquals(STRINGS, actual);
        for (int i = 0; i < STRINGS.size(); i++)
            assertSame(STRINGS.get(i), actual.get(i));
    }

    @ParameterizedTest @ValueSource(strings = {"utf-8", "utf-16"})
    void testParseBytes(String csName) {
        Charset cs = Charset.forName(csName);
        List<CharSequence> actual = STRINGS.stream().map(s -> s.getBytes(cs))
                .map(b -> CharSequenceFragmentParser.INSTANCE.parseBytes(b, cs))
                .toList();
        assertEquals(STRINGS, actual);
    }
}