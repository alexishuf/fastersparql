package com.github.alexishuf.fastersparql.model.rope;

import com.github.alexishuf.fastersparql.util.SafeCloseable;
import com.github.alexishuf.fastersparql.util.concurrent.Alloc;
import com.github.alexishuf.fastersparql.util.concurrent.Primer;

import java.lang.foreign.MemorySegment;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.function.Supplier;

public class RopeDecoder implements SafeCloseable {
    public static final int BYTES = 16 + 4*4 + 20+128;
    private static final boolean DEBUG = RopeDecoder.class.desiredAssertionStatus();
    private static final Supplier<RopeDecoder> FAC = new Supplier<>() {
        @Override public RopeDecoder get() {return new RopeDecoder();}
        @Override public String toString() {return "RopeDecoder.FAC";}
    };
    private static final Alloc<RopeDecoder> ALLOC = new Alloc<>(RopeDecoder.class,
            "RopeDecoder.ALLOC", Alloc.THREADS*32, FAC, BYTES);
    static { Primer.INSTANCE.sched(ALLOC::prime); }

    private final CharsetDecoder decoder;
    private final char[] chars;
    private final CharBuffer buffer;
    private boolean live;

    public static RopeDecoder create() {
        RopeDecoder d = ALLOC.create();
        d.live = true;
        return d;
    }

    private RopeDecoder() {
        this.decoder = StandardCharsets.UTF_8.newDecoder()
                .onMalformedInput(CodingErrorAction.REPLACE)
                .onUnmappableCharacter(CodingErrorAction.REPLACE);
        this.chars = new char[128];
        this.buffer = CharBuffer.wrap(chars);
    }

    public void write(StringBuilder dst, MemorySegment segment, long begin, int len) {
        if (len < 0 || begin < 0)
            throw new IndexOutOfBoundsException("Negative begin/len");
        if (begin+len > segment.byteSize())
            throw new IndexOutOfBoundsException(begin+len);
        ByteBuffer bb = segment.asSlice(begin, len).asByteBuffer();
        for (CoderResult result = CoderResult.OVERFLOW; result != CoderResult.UNDERFLOW; ) {
            result = decoder.decode(bb, buffer.clear(), true);
            dst.append(chars, 0, buffer.position());
        }
    }

    public void write(StringBuilder dst, byte[] utf8, int offset, int len) {
        if (len < 0)
            throw new IndexOutOfBoundsException("Negative begin/len");
        if (offset+len > utf8.length)
            throw new IndexOutOfBoundsException(offset+len);
        var bb = ByteBuffer.wrap(utf8).position(offset).limit(offset+len);
        for (CoderResult result = CoderResult.OVERFLOW; result != CoderResult.UNDERFLOW; ) {
            result = decoder.decode(bb, buffer.clear(), true);
            dst.append(chars, 0, buffer.position());
        }
    }

    @Override public void close() {
        boolean bad = !live;
        if (!bad) {
            live = false;
            decoder.reset();
            buffer.clear();
        }
        if (DEBUG)
            VarHandle.fullFence();
        if (bad || live)
            throw new IllegalStateException("duplicate/concurrent close()");
        ALLOC.offer(this);
    }
}
