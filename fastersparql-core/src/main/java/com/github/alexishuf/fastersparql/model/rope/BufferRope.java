package com.github.alexishuf.fastersparql.model.rope;

import java.nio.ByteBuffer;

public class BufferRope extends Rope {
    public ByteBuffer buffer;
    public BufferRope(ByteBuffer buffer) {
        this.buffer = buffer;
    }
}
