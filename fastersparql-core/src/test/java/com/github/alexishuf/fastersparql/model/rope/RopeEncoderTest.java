package com.github.alexishuf.fastersparql.model.rope;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.model.rope.RopeEncoder.BYTE_SINK_APPENDER;
import static com.github.alexishuf.fastersparql.model.rope.RopeEncoder.charSequence2utf8;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.mismatch;
import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.Assertions.assertEquals;

class RopeEncoderTest {

    static Stream<Arguments> test() {
        List<String> chars = List.of(
                "a",
                "±", // U+00B1 --> utf8=C2B1
                "ࠀ", // U+0800 --> utf8=E0A080
                "ワ", // U+30EF --> utf8=E383AF
                "７", // U+FF17 --> utf8=EFBC97 (above UTF-16 surrogates)
                "\uD800\uDD12" // U+10112 --> utf8=F0908492
        );
        List<String> strings = new ArrayList<>();
        strings.add("");
        strings.addAll(chars);
        for (String c : chars) {
            strings.add("."+c);
            strings.add("("+c+")");
            strings.add(" ["+c+" ]");
        }
        for (String c : chars) {
            strings.add(c+'-'+c);
            strings.add(c+c);
        }
        for (int i = 0; i < chars.size(); i++) {
            for (int j = 0; j < chars.size(); j++) {
                if (j != i)
                    strings.add(chars.get(i)+chars.get(j));
            }
        }
        return strings.stream().map(Arguments::arguments);
    }

    @ParameterizedTest @MethodSource
    void test(String string) {
        byte[] expectedU8 = string.getBytes(UTF_8);

        int reqBytes = RopeFactory.requiredBytes(string);
        assertEquals(expectedU8.length, reqBytes);

        for (int i = 0; i < 128; i++) {
            var rope = RopeFactory.make(reqBytes).add(string).take();
            assertEquals(-1, mismatch(expectedU8, 0, expectedU8.length,
                                      requireNonNull(rope.utf8),
                                      (int)rope.offset, (int)rope.offset+rope.len));
        }
        for (int i = 0; i < 128; i++) {
            byte[] u8 = new byte[reqBytes];
            int u8Len = charSequence2utf8(string, 0, string.length(), u8, 0);
            assertEquals(reqBytes, u8Len);
            assertEquals(-1, mismatch(expectedU8, 0, expectedU8.length,
                                      u8, 0, u8Len));

            byte[] pU8 = new byte[reqBytes+1];
            pU8[0] = '!';
            String pString = "７@"+string;
            int pU8End = charSequence2utf8(pString, 2, pString.length(), pU8, 1);
            assertEquals(reqBytes+1, pU8End);
            assertEquals(-1, mismatch(expectedU8, 0, expectedU8.length,
                                      pU8, 1, pU8End));

            try (var tmp = PooledMutableRope.get()) {
                tmp.append("#$");
                charSequence2utf8(string, 0, string.length(), BYTE_SINK_APPENDER, tmp);
                assertEquals(reqBytes+2, tmp.len);
                assertEquals(-1, mismatch(expectedU8, 0, expectedU8.length,
                                          tmp.u8(), 2, tmp.len));

                tmp.clear().append("]>");
                charSequence2utf8(pString, 2, pString.length(), BYTE_SINK_APPENDER, tmp);
                assertEquals(reqBytes+2, tmp.len);
                assertEquals(-1, mismatch(expectedU8, 0, expectedU8.length,
                                          tmp.u8(), 2, tmp.len));
            }
        }
    }
}