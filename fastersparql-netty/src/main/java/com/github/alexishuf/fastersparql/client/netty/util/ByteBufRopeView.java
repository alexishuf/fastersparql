package com.github.alexishuf.fastersparql.client.netty.util;

import com.github.alexishuf.fastersparql.model.rope.PlainRope;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import com.github.alexishuf.fastersparql.model.rope.TwoSegmentRope;
import com.github.alexishuf.fastersparql.util.concurrent.ThreadAffinityPool;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;

import java.lang.foreign.MemorySegment;

public final class ByteBufRopeView {
    private static final ThreadAffinityPool<ByteBufRopeView> POOL = new ThreadAffinityPool<>(ByteBufRopeView.class, 8);

    private final SegmentRope sr = new SegmentRope();
    private @MonotonicNonNull TwoSegmentRope tsr;
    private byte @MonotonicNonNull[] copy;

    public static ByteBufRopeView create() {
        ByteBufRopeView v = POOL.get();
        return v == null ? new ByteBufRopeView() : v;
    }

    private ByteBufRopeView() {}

    public void recycle() {
        POOL.offer(this);
    }

    public SegmentRope wrapAsSingle(ByteBuf bb) {
        if (bb instanceof CompositeByteBuf c) {
            switch (c.numComponents()) {
                case 0  ->   bb = Unpooled.EMPTY_BUFFER;
                case 1  -> { return wrapAsSingle(c.component(0)); }
                default -> { return copy(c); }
            }
        }
        return wrapSingle(bb);
    }

    /**
     * Get a {@link PlainRope} wrapping the underlying {@link ByteBuf#readableBytes()} bytes
     * of {@code bb}. The returned {@link PlainRope} will be invalidated if this method or
     * {@link #wrapAsSingle(ByteBuf)} is invoked or if {@code bb} lifetime ends (zero references).
     * This method does not call {@link ByteBuf#retain()}.
     *
     * <p>This method is implemented to avoid copies and usually returns a {@link SegmentRope}.
     * If {@code bb} is a {@link CompositeByteBuf} with 2 non-composite components, this method
     * returns a {@link TwoSegmentRope}. For more than 2 components (or for composite components)
     * Data will be copied into temporary storage that will be invalidated by future  calls to
     * this method or to {@link #wrapAsSingle(ByteBuf)}.</p>
     *
     * @param bb A {@link ByteBuf} to view as a {@link PlainRope}
     * @return A {@link PlainRope} exposing the {@link ByteBuf#readableBytes()} in {@code bb}.
     */
    public PlainRope wrap(ByteBuf bb) {
        return bb instanceof CompositeByteBuf c ? wrapComposite(c) : wrapSingle(bb);
    }

    private SegmentRope wrapSingle(ByteBuf bb) {
        MemorySegment segment;
        byte[] u8;
        int offset = bb.readerIndex();
        int len = bb.readableBytes();
        if (bb.hasArray()) {
            segment = MemorySegment.ofArray(u8 = bb.array());
            offset += bb.arrayOffset();
        } else {
            u8 = null;
            segment = MemorySegment.ofAddress(bb.memoryAddress(), offset+len);
        }
        sr.wrapSegment(segment, u8, offset, len);
        return sr;
    }

    private PlainRope wrapComposite(CompositeByteBuf c) {
        return switch (c.numComponents()) {
            case 0 -> {
                sr.wrapEmptyBuffer();
                yield sr;
            }
            case 1 -> wrap(c.component(0));
            case 2 -> {
                TwoSegmentRope tsr = this.tsr;
                if (tsr == null) this.tsr = tsr = new TwoSegmentRope();
                tsr.wrapFirst(wrapAsSingle(c.component(0)));
                tsr.wrapSecond(wrapAsSingle(c.component(1)));
                yield tsr;
            }
            default -> copy(c);
        };
    }

    private SegmentRope copy(CompositeByteBuf c) {
        int size = c.readableBytes();
        byte[] copy = this.copy;
        if (copy == null || copy.length < size)
            this.copy = copy = new byte[size];
        c.readBytes(this.copy);
        sr.wrapSegment(MemorySegment.ofArray(copy), copy, 0, size);
        return sr;
    }
}
