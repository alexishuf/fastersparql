package com.github.alexishuf.fastersparql.model.rope;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;

public class BufferRope extends Rope {
    public ByteBuffer buffer;
    protected MemorySegment segment;

    public BufferRope(ByteBuffer buffer) {
        super(buffer.remaining());
        this.buffer = buffer;
    }

    @Override public @Nullable MemorySegment segment() {
        MemorySegment s = segment;
        if (s == null)
            segment = s = MemorySegment.ofBuffer(buffer);
        return s;
    }
}
