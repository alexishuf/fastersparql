package com.github.alexishuf.fastersparql.model.rope;

import org.checkerframework.common.returnsreceiver.qual.This;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.foreign.MemorySegment;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.nio.charset.StandardCharsets.UTF_8;

public class OutputStreamSink implements ByteSink<OutputStreamSink, OutputStreamSink> {
    public OutputStream os;
    private int len = 0;

    public OutputStreamSink(OutputStream os) {
        this.os = os;
    }

    private @This OutputStreamSink write(byte b) {
        try {
            len++;
            os.write(b);
            return this;
        } catch (IOException e) { throw new RuntimeException(e); }
    }

    private @This OutputStreamSink write(byte[] bytes, int off, int len) {
        try {
            this.len += len;
            os.write(bytes, off, len);
            return this;
        } catch (IOException e) { throw new RuntimeException(e); }
    }

    @Override public OutputStreamSink take() { return this; }

    @Override public boolean isEmpty() { return len == 0; }
    @Override public int         len() { return len; }

    @Override public @This OutputStreamSink append(byte[] arr, int begin, int len) {
        return write(arr, begin, len);
    }

    @Override public @This OutputStreamSink append(byte c) { return write(c); }

    @Override public @This OutputStreamSink append(MemorySegment segment, byte[] base,
                                                   long offset, int len) {
        if (base == null && !segment.isNative())
            base = (byte[]) segment.array().orElse(null);
        int u8Off;
        if (base == null) {
            base = new byte[len];
            MemorySegment.copy(segment, JAVA_BYTE, offset, base, u8Off = 0, len);
        } else {
            u8Off = (int) (segment.address()+offset);
        }
        return write(base, u8Off, len);
    }

    @Override public @This OutputStreamSink append(Rope rope, int begin, int end) {
        if (rope instanceof ByteRope b)
            return write(b.u8(), (int)b.offset+begin, end-begin);
        byte[] a = rope.toArray(begin, end);
        return write(a, 0, a.length);
    }

    @Override public @This OutputStreamSink append(CharSequence cs, int begin, int end) {
        if (cs instanceof Rope r)
            return append(r, begin, end);
        return append(cs.subSequence(begin, end).toString().getBytes(UTF_8));
    }

    @Override public @This OutputStreamSink ensureFreeCapacity(int increment) {
        return this;
    }
}
