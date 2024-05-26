package com.github.alexishuf.fastersparql.lrb;

import com.github.alexishuf.fastersparql.client.netty.util.ByteBufRopeView;
import com.github.alexishuf.fastersparql.client.netty.util.ByteBufSink;
import com.github.alexishuf.fastersparql.model.rope.ByteSink;
import com.github.alexishuf.fastersparql.model.rope.MutableRope;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import com.github.alexishuf.fastersparql.model.rope.SegmentRopeView;
import com.github.alexishuf.fastersparql.util.SafeCloseable;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;

import java.util.ArrayList;
import java.util.List;

public class RopeTypeHolder implements SafeCloseable {
    private final RopeType ropeType;
    private final List<ByteBuf> byteBufs = new ArrayList<>();
    private final ByteBufRopeView bbRopeView = new ByteBufRopeView(new SegmentRopeView());

    public RopeTypeHolder(RopeType ropeType) {
        this.ropeType = ropeType;
    }

    @Override public void close() {
        byteBufs.forEach(ByteBuf::release);
        byteBufs.clear();
    }

    @SuppressWarnings("unchecked")
    public <B extends ByteSink<B, T>, T> B byteSink() {
        return switch (ropeType) {
            case BYTE  -> (B)new MutableRope(64);
            case NETTY -> (B)new ByteBufSink(PooledByteBufAllocator.DEFAULT);
        };
    }
    public SegmentRope takeRope(ByteSink<?,?> sink) {
        return switch (ropeType) {
            case BYTE  -> (MutableRope)sink;
            case NETTY -> {
                var bb = ((ByteBufSink) sink).take();
                byteBufs.add(bb);
                yield new SegmentRopeView().wrap(bbRopeView.wrapAsSingle(bb));
            }
        };
    }



}
