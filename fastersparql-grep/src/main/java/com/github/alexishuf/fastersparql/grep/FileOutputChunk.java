package com.github.alexishuf.fastersparql.grep;

import com.github.alexishuf.fastersparql.model.rope.ByteRope;

import java.nio.ByteBuffer;

public class FileOutputChunk {
    public final ByteRope rope;
    private final ByteBuffer buffer;

    public FileOutputChunk(int capacity) {
        buffer = (rope = new ByteRope(capacity)).asBuffer();
    }

    public ByteBuffer buffer() { return buffer.position(0).limit(rope.len); }
}
