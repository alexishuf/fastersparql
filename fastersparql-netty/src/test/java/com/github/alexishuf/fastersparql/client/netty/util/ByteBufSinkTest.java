package com.github.alexishuf.fastersparql.client.netty.util;

import com.github.alexishuf.fastersparql.model.rope.ByteRope;
import com.github.alexishuf.fastersparql.model.rope.Rope;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class ByteBufSinkTest {
    @Test void testTake() {
        var s = new ByteBufSink(UnpooledByteBufAllocator.DEFAULT).touch();

        assertSame(s, s.touch());
        ByteBuf bb1 = s.take();
        assertEquals(1, bb1.refCnt());
        assertThrows(IllegalStateException.class, s::take);

        assertThrows(IllegalStateException.class, s::take);
        ByteBuf bb2 = s.touch().take();
        assertEquals(1, bb1.refCnt());
        assertEquals(1, bb2.refCnt());

        bb1.release();
        assertEquals(0, bb1.refCnt());
        assertEquals(1, bb2.refCnt());

        s.release();
        assertEquals(0, bb1.refCnt());
        assertEquals(1, bb2.refCnt());

        s.touch().release();
        assertEquals(0, bb1.refCnt());
        assertEquals(1, bb2.refCnt());
        assertThrows(IllegalStateException.class, s::take);

        bb2.release();
    }

    @Test void testAppendBytesAndStrings() {
        var s = new ByteBufSink(UnpooledByteBufAllocator.DEFAULT).touch();
        s.append('0');
        s.append((byte) '1');
        s.append(new byte[]{'3', '4'});
        s.append(new byte[]{'4', '5', '6', '7'}, 1, 3);
        s.append("8");
        s.append(new StringBuilder().append('9'));
        assertEquals("013456789", s.take().toString(UTF_8));
    }

    @Test void testAppendRopes() {
        List<Rope> ropes = List.of(new ByteRope("0"),
                new SegmentRope(ByteBuffer.wrap("012".getBytes(UTF_8)).slice(1, 2)),
                Term.valueOf("\"3\""),
                Term.valueOf("\"4\"^^<http://www.w3.org/2001/XMLSchema#integer>"),
                Term.valueOf("<http://www.w3.org/2001/XMLSchema#anyURI>"));
        ByteBufSink s1 = new ByteBufSink(UnpooledByteBufAllocator.DEFAULT).touch();
        ByteBufSink s2 = new ByteBufSink(UnpooledByteBufAllocator.DEFAULT).touch();
        for (Rope r : ropes) {
            s1.append(r);
            s2.append(r, 0, r.len);
        }
        String expected = "012\"3\"\"4\"^^<http://www.w3.org/2001/XMLSchema#integer><http://www.w3.org/2001/XMLSchema#anyURI>";
        assertEquals(expected, s1.take().toString(UTF_8));
        assertEquals(expected, s2.take().toString(UTF_8));
    }

    static Stream<Arguments> testAppendSubRope() {
        List<Arguments> list = new ArrayList<>();
        List<Rope> simpleRopes = List.of(
                new ByteRope("\"1234\""),
                new SegmentRope(ByteBuffer.wrap("\"1234\"".getBytes(UTF_8))),
                Term.valueOf("\"1234\""));
        for (Rope r : simpleRopes) {
            list.add(arguments(r, 0, 6, "\"1234\""));
            list.add(arguments(r, 2, 6, "234\""));
            list.add(arguments(r, 0, 3, "\"12"));
            list.add(arguments(r, 1, 5, "1234"));
        }
        Term i7 = Term.valueOf("\"7\"^^<http://www.w3.org/2001/XMLSchema#integer>");
        //"7"^^<http://www.w3.org/2001/XMLSchema#integer>
        Term iri = Term.valueOf("<http://www.w3.org/2001/XMLSchema#anyURI>");

        list.add(arguments(i7, 0, i7.len, i7.toString()));
        list.add(arguments(iri, 0, iri.len, iri.toString()));

        //substring of first segment
        list.add(arguments(i7, 0, 2, "\"7"));
        list.add(arguments(i7, 0, 1, "\""));
        list.add(arguments(i7, 1, 2, "7"));
        list.add(arguments(iri, 0, 34, "<http://www.w3.org/2001/XMLSchema#"));
        list.add(arguments(iri, 1, 33, "http://www.w3.org/2001/XMLSchema"));

        //substrings of second segment
        list.add(arguments(i7, 2, i7.len, "\"^^<http://www.w3.org/2001/XMLSchema#integer>"));
        list.add(arguments(i7, 39, i7.len, "integer>"));
        list.add(arguments(iri, 34, 41, "anyURI>"));
        list.add(arguments(iri, 35, 40, "nyURI"));

        // substrings crossing segments
        list.add(arguments(i7, 1, 3, "7\""));
        list.add(arguments(i7, 1, 5, "7\"^^"));
        list.add(arguments(i7, 0, 5, "\"7\"^^"));
        list.add(arguments(iri, 33, 41, "#anyURI>"));
        list.add(arguments(iri, 33, 37, "#any"));

        return list.stream();
    }

    @ParameterizedTest @MethodSource
    void testAppendSubRope(Rope rope, int begin, int end, String expected) {
        var s = new ByteBufSink(UnpooledByteBufAllocator.DEFAULT).touch();
        assertEquals(expected, s.append(rope, begin, end).take().toString(UTF_8));
    }
}