package com.github.alexishuf.fastersparql.model.rope;

import com.github.alexishuf.fastersparql.util.concurrent.Bytes;
import org.checkerframework.common.returnsreceiver.qual.This;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.foreign.MemorySegment;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;

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

    @Override public OutputStreamSink takeUntil(int len) {
        throw new UnsupportedOperationException();
    }

    @Override public boolean  isEmpty() { return len == 0; }
    @Override public int          len() { return len; }
    @Override public int freeCapacity() { return Integer.MAX_VALUE; }

    @Override public @This OutputStreamSink append(byte[] arr, int begin, int len) {
        return write(arr, begin, len);
    }

    @Override public @This OutputStreamSink append(byte c) { return write(c); }

    @Override public @This OutputStreamSink append(MemorySegment segment, byte[] base,
                                                   long offset, int len) {
        if (base == null && !segment.isNative())
            base = (byte[]) segment.heapBase().orElse(null);
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
        int len = end - begin;
        byte[] arr = rope.backingArray();
        if (arr != null)
            write(arr, rope.backingArrayOffset()+begin, len);
        else
            appendChunked(rope, begin, end);
        return this;
    }

    private void appendChunked(Rope rope, int begin, int end) {
        Bytes b = Bytes.atLeastElse(Math.min(end-begin, 2048), 128).takeOwnership(this);
        try {
            byte[] arr = b.arr;
            for (int i = begin; i < end; i += arr.length) {
                int chunkEnd = Math.min(end, i + arr.length);
                rope.copy(i, chunkEnd, arr, 0);
                write(arr, 0, chunkEnd-i);
            }
        } finally {
            b.recycle(this);
        }
    }

    @Override public @This OutputStreamSink append(SegmentRope rope, int begin, int end) {
        return append((Rope)rope, begin, end);
    }

    @Override public @This OutputStreamSink ensureFreeCapacity(int increment) {
        return this;
    }
}
