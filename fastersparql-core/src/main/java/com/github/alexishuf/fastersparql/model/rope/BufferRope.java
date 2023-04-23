package com.github.alexishuf.fastersparql.model.rope;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;

import static java.lang.foreign.MemorySegment.ofBuffer;

public class BufferRope extends Rope {
    ByteBuffer buffer;
    int offset;
    protected @Nullable MemorySegment segment;

    public BufferRope(ByteBuffer buffer, int offset, int len) {
        super(len);
        this.buffer = buffer;
        this.offset = offset;
    }
    public BufferRope(ByteBuffer buffer) {
        super(buffer.remaining());
        this.buffer = buffer;
        this.offset = buffer.position();
    }

    public ByteBuffer buffer() { return buffer; }

    public int offset() { return offset; }

    public void buffer(ByteBuffer buffer) {
        this.buffer = buffer;
        this.offset = buffer.position();
        this.len = buffer.remaining();
        this.segment = null;
    }

    @Override public @Nullable MemorySegment segment() {
        MemorySegment s = segment;
        if (s == null) {
            int limit = offset+len;
            if (buffer.position() == offset && buffer.limit() == limit)
                segment = s = ofBuffer(buffer);
            else
                segment = s = ofBuffer(buffer.duplicate().position(offset).limit(limit));
        }
        return s;
    }
}
