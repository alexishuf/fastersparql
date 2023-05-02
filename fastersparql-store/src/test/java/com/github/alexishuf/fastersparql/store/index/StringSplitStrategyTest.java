package com.github.alexishuf.fastersparql.store.index;

import com.github.alexishuf.fastersparql.model.rope.ByteRope;
import com.github.alexishuf.fastersparql.model.rope.PlainRope;
import com.github.alexishuf.fastersparql.model.rope.TwoSegmentRope;
import com.github.alexishuf.fastersparql.store.index.StringSplitStrategy.SharedSide;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.lang.foreign.MemorySegment;
import java.util.List;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.store.index.StringSplitStrategy.SharedSide.*;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class StringSplitStrategyTest {
    public static Stream<Arguments> testBase64() {
        return Stream.of(
                arguments(PREFIX, "QUJD.", 0x414243),
                arguments(SUFFIX, "TWEA!", 0x4d6100),
                arguments(PREFIX, "TWFu.", 0x4d616e),
                // x00       | 0x7c      | 0x50
                // 000000 00 | 0111 1100 | 01 010000
                // A      H         x         Q
                arguments(SUFFIX, "AHxQ!", 0x007c50)
        );
    }

    @ParameterizedTest @MethodSource
    public void testBase64(SharedSide side, String base64, int id) {
        var split = new StringSplitStrategy();
        split.sharedSide = side;
        assertEquals(base64, split.b64(id).toString());
        var seg = MemorySegment.ofArray(("!." + base64).getBytes(UTF_8));
        assertEquals(id, StringSplitStrategy.decode(seg, 2));
    }

    static Stream<Arguments> testSplit() {
        return Stream.of(
                arguments("<rel>", "", "<rel>", NONE),
                arguments("_:b0", "", "_:b0", NONE),
                arguments("_:anon", "", "_:anon", NONE),
                arguments("_:anon123456", "", "_:anon123456", NONE),
                arguments("\"Bob\"", "", "\"Bob\"", NONE),
                arguments("\"Alice\"@en", "", "\"Alice\"@en", NONE),
                arguments("\"Alice\"@en-US", "\"@en-US", "\"Alice", SUFFIX),
                arguments("\"23\"^^<http://www.w3.org/2001/XMLSchema#integer>", "\"^^<http://www.w3.org/2001/XMLSchema#integer>", "\"23", SUFFIX),
                arguments("<http://example.org/Alice>", "<http://example.org/", "Alice>", PREFIX),
                arguments("<http://example.org/a/b/file#X>", "<http://example.org/a/b/file#", "X>", PREFIX)
        );
    }

    private final StringSplitStrategy split = new StringSplitStrategy();

    @ParameterizedTest @MethodSource
    public void testSplit(String nt, String shared, String local, SharedSide side) {
        ByteRope ntRope = new ByteRope(nt);
        ByteRope localRope = new ByteRope(local), sharedRope = new ByteRope(shared);
        TwoSegmentRope ts0 = new TwoSegmentRope(), ts1 = new TwoSegmentRope();
        TwoSegmentRope ts2 = new TwoSegmentRope();
        ts0.wrapFirst(side == SUFFIX ? localRope : sharedRope);
        ts0.wrapSecond(side == SUFFIX ? sharedRope : localRope);
        ts1.wrapFirst(ntRope);
        ts2.wrapSecond(ntRope);
        for (PlainRope in : List.of(ntRope, ts0, ts1, ts2)) {
            assertEquals(side, split.split(in));
            assertEquals(shared, split.shared().toString());
            assertEquals(local, split.local().toString());
            assertEquals(local, split.localOrWhole().toString());
        }
    }
}