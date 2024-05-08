package com.github.alexishuf.fastersparql.grep;

import com.github.alexishuf.fastersparql.model.rope.MutableRope;
import com.github.alexishuf.fastersparql.util.concurrent.Bytes;

import java.nio.ByteBuffer;

public class FileOutputChunk {
    public final MutableRope rope;
    private final ByteBuffer buffer;

    public FileOutputChunk(int capacity) {
        rope = new MutableRope(Bytes.createUnpooled(new byte[capacity]), 0);
        buffer = rope.asBuffer();
    }

    public ByteBuffer buffer() { return buffer.position(0).limit(rope.len); }
}
