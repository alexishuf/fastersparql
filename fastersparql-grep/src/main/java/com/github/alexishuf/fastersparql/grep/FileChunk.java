package com.github.alexishuf.fastersparql.grep;

import com.github.alexishuf.fastersparql.model.rope.SegmentRope;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;

public class FileChunk extends SegmentRope {
    private final ByteBuffer buffer;
    public long firstBytePos;
    public int chunkNumber;

    public FileChunk(MemorySegment segment) {
        super(segment, 0, (int)Math.min(Integer.MAX_VALUE, segment.byteSize()));
        buffer = segment.asByteBuffer();
        len = 0;
    }

    public int          capacity() { return buffer.capacity(); }
    public ByteBuffer recvBuffer() { return buffer.limit(buffer.capacity()).position(len); }

    public ByteBuffer sendBuffer(int begin, int end) {
        return buffer.position(0).limit(end).position(begin);
    }

}
