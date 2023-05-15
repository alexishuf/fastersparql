package com.github.alexishuf.fastersparql.store.index.dict;

import com.github.alexishuf.fastersparql.model.rope.ByteRope;
import com.github.alexishuf.fastersparql.model.rope.PlainRope;
import com.github.alexishuf.fastersparql.model.rope.TwoSegmentRope;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.lang.foreign.MemorySegment;
import java.util.List;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.store.index.dict.Splitter.SharedSide.*;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class SplitterTest {

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
    public void testBase64(Splitter.SharedSide side, String base64, int id) {
        var split = new Splitter();
        split.sharedSide = side;
        assertEquals(base64, split.b64(id).toString());
        var seg = MemorySegment.ofArray(("!." + base64).getBytes(UTF_8));
        assertEquals(id, Splitter.decode(seg, 2));
    }

    static Stream<Arguments> testSplit() {
        return Stream.of(
                arguments(defSplit, "<rel>", "", "<rel>", NONE),
                arguments(defSplit, "_:b0", "", "_:b0", NONE),
                arguments(defSplit, "_:anon", "", "_:anon", NONE),
                arguments(defSplit, "_:anon123456", "", "_:anon123456", NONE),
                arguments(defSplit, "\"Bob\"", "", "\"Bob\"", NONE),
                arguments(defSplit, "\"Alice\"@en", "\"@en", "\"Alice", SUFFIX),
                arguments(defSplit, "\"Alice\"@en-US", "\"@en-US", "\"Alice", SUFFIX),
                arguments(defSplit, "\"23\"^^<http://www.w3.org/2001/XMLSchema#integer>", "\"^^<http://www.w3.org/2001/XMLSchema#integer>", "\"23", SUFFIX),
                arguments(defSplit, "<http://example.org/Alice>", "<http://example.org/", "Alice>", PREFIX),
                arguments(defSplit, "<http://example.org/a/b/file#X>", "<http://example.org/a/b/file#", "X>", PREFIX),
                arguments(defSplit, "<http://example.org/TCGA-34-26k-g156>",
                        "<http://example.org/", "TCGA-34-26k-g156>", PREFIX),
                arguments(defSplit, "<http://example.org/chebi:ex>",
                        "<http://example.org/", "chebi:ex>", PREFIX),

                arguments(prolongedSplit, "_:b0-bogus:2", "", "_:b0-bogus:2", NONE),
                arguments(prolongedSplit, "\"x\"^^<http://example.org/TCGA-23>",
                        "\"^^<http://example.org/TCGA-23>", "\"x", SUFFIX),
                arguments(prolongedSplit, "<http://example.org/Alice>",
                        "<http://example.org/", "Alice>", PREFIX),
                arguments(prolongedSplit, "<http://example.org/chebi:ex>",
                        "<http://example.org/chebi:", "ex>", PREFIX),
                arguments(prolongedSplit, "<http://example.org/TCGA-34-26n-g156>",
                        "<http://example.org/TCGA-34-26n-", "g156>", PREFIX),

                arguments(penultimateSplit, "_:b0", "", "_:b0", NONE),
                arguments(penultimateSplit, "_:b0/1", "", "_:b0/1", NONE),
                arguments(penultimateSplit, "\"x\"^^<http://example.org/some/thing#type>",
                        "\"^^<http://example.org/some/thing#type>", "\"x", SUFFIX),
                arguments(penultimateSplit, "<http://example.org/123/name>",
                        "<http://example.org/", "123/name>", PREFIX),
                arguments(penultimateSplit, "<http://example.org/123/456/name>",
                        "<http://example.org/123/", "456/name>", PREFIX),
                arguments(penultimateSplit, "<http://example.org/Alice>",
                        "<http://example.org/", "Alice>", PREFIX)
        );
    }

    private static final Splitter defSplit = new Splitter();
    private static final Splitter prolongedSplit = new Splitter(Splitter.Mode.PROLONG);
    private static final Splitter penultimateSplit = new Splitter(Splitter.Mode.PENULTIMATE);

    @ParameterizedTest @MethodSource
    public void testSplit(Splitter split, String nt, String shared, String local, Splitter.SharedSide side) {
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
        }
    }
}