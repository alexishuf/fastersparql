package com.github.alexishuf.fastersparql.lrb;

import com.github.alexishuf.fastersparql.client.netty.util.ByteBufSink;
import com.github.alexishuf.fastersparql.model.rope.ByteRope;
import com.github.alexishuf.fastersparql.model.rope.ByteSink;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;

import java.util.ArrayList;
import java.util.List;

public class RopeTypeHolder implements AutoCloseable {
    private final RopeType ropeType;
    private final ByteBufSink nettySink = new ByteBufSink(PooledByteBufAllocator.DEFAULT);
    private final List<ByteBuf> byteBufs = new ArrayList<>();

    public RopeTypeHolder(RopeType ropeType) {
        this.ropeType = ropeType;
    }

    @Override public void close() {
        nettySink.release();
        byteBufs.forEach(ByteBuf::release);
        byteBufs.clear();
    }

    @SuppressWarnings("unchecked")
    public <B extends ByteSink<B>> B byteSink() {
        return switch (ropeType) {
            case BYTE  -> (B)new ByteRope();
            case NETTY -> (B)nettySink.touch();
        };
    }
    public <B extends ByteSink<B>> SegmentRope takeRope(B sink) {
        return switch (ropeType) {
            case BYTE  -> (ByteRope)sink;
            case NETTY -> {
                ByteBuf bb = ((ByteBufSink) sink).take();
                byteBufs.add(bb);
                yield new SegmentRope(bb.nioBuffer());
            }
        };
    }

}
