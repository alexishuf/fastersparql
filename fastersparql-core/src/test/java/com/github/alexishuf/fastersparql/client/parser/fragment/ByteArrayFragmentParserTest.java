package com.github.alexishuf.fastersparql.client.parser.fragment;

import com.github.alexishuf.fastersparql.client.parser.fragment.ByteArrayFragmentParser.Encoder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.*;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class ByteArrayFragmentParserTest {
    @Test
    void testParseBytes() {
        byte[] data = "test".getBytes(UTF_8);
        assertSame(data, ByteArrayFragmentParser.INSTANCE.parseBytes(data, UTF_8));
    }

    static Stream<Arguments> encoderData() {
        List<Arguments> list = new ArrayList<>();
        for (Charset cs : asList(UTF_8, UTF_16LE, UTF_16BE, ISO_8859_1)) {
            for (String string : asList("test", "ação", "\uD83E\uDE02"))
                list.add(arguments(string, cs));
        }
        return list.stream();
    }

    @ParameterizedTest @MethodSource("encoderData")
    void testEncoder(String string, Charset cs) {
        byte[] expected = string.getBytes(cs);
        Encoder encoder = new Encoder(cs);
        assertArrayEquals(expected, encoder.apply(string));
    }

    @ParameterizedTest @MethodSource("encoderData")
    void testFragmentedEncoder(String string, Charset cs) {
        byte[] expected = string.getBytes(cs);
        Encoder encoder = new Encoder(cs);
        ByteBuffer bb = ByteBuffer.allocate(string.length()*4);
        for (char c : string.toCharArray())
            bb.put(encoder.apply(new StringBuilder().append(c)));
        assertArrayEquals(expected, Arrays.copyOf(bb.array(), bb.position()));
    }

    @ParameterizedTest @ValueSource(strings = {
            "ISO-8859-1",
            "UTF-8"
    })
    void testParseStrings(String csName) {
        Charset cs = Charset.forName(csName);
        List<String> strings = List.of("test", "ação");
        List<byte[]> actual = strings.stream()
                .map(s -> ByteArrayFragmentParser.INSTANCE.parseString(s, cs)).toList();
        List<byte[]> expected = strings.stream().map(s -> s.getBytes(cs)).toList();
        assertEquals(expected.size(), actual.size());
        for (int i = 0; i < expected.size(); i++)
            assertArrayEquals(expected.get(i), actual.get(i));
    }

}