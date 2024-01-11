package com.github.alexishuf.fastersparql.client.netty.util;

import com.github.alexishuf.fastersparql.model.rope.ByteRope;
import com.github.alexishuf.fastersparql.model.rope.PlainRope;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import com.github.alexishuf.fastersparql.model.rope.TwoSegmentRope;
import com.github.alexishuf.fastersparql.util.concurrent.GlobalAffinityShallowPool;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.foreign.MemorySegment;

import static java.util.Objects.requireNonNull;

public final class ByteBufRopeView {
    private static final int POOL_COLUMN = GlobalAffinityShallowPool.reserveColumn();
    private static final MemorySegment   EMPTY_SEGMENT = ByteRope.EMPTY.segment;
    private static final byte @NonNull[] EMPTY_U8      = requireNonNull(ByteRope.EMPTY.utf8);

    private final SegmentRope sr = new SegmentRope();
    private @MonotonicNonNull TwoSegmentRope tsr;
    private byte @MonotonicNonNull[] copy;

    public static ByteBufRopeView create() {
        ByteBufRopeView v = GlobalAffinityShallowPool.get(POOL_COLUMN);
        return v == null ? new ByteBufRopeView() : v;
    }

    private ByteBufRopeView() {}

    public void recycle() {
        GlobalAffinityShallowPool.offer(POOL_COLUMN, this);
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
        byte[] u8 = null;
        int offset = bb.readerIndex();
        int len = bb.readableBytes();
        if (len == 0) {
            segment = EMPTY_SEGMENT;
            u8      = EMPTY_U8;
            offset  = 0;
        } else if (bb.hasArray()) {
            segment = MemorySegment.ofArray(u8 = bb.array());
            offset += bb.arrayOffset();
        } else if (bb.hasMemoryAddress()) {
            segment = MemorySegment.ofAddress(bb.memoryAddress()).reinterpret(offset+len);
        } else if (bb.isDirect() && bb.nioBufferCount() == 1) {
            segment = MemorySegment.ofBuffer(bb.nioBuffer());
            offset = 0;
        } else {
            return copy(bb);
        }
        sr.wrapSegment(segment, u8, offset, len);
        return sr;
    }

    private @Nullable SegmentRope wrapComponent(ByteBuf bb) {
        MemorySegment seg;
        byte[] u8 = null;
        int off = bb.readerIndex(), len = bb.readableBytes();
        if (bb.hasArray()) {
            seg = MemorySegment.ofArray(u8 = bb.array());
            off += bb.arrayOffset();
        } else if (bb.hasMemoryAddress()) {
            seg = MemorySegment.ofAddress(bb.memoryAddress()).reinterpret(off+len);
        } else if (bb.nioBufferCount() == 1 && bb.isDirect()) {
            seg = MemorySegment.ofBuffer(bb.nioBuffer());
            off = 0;
        } else {
            return null;
        }
        sr.wrapSegment(seg, u8, off, len);
        return sr;
    }

    private PlainRope wrapComposite(CompositeByteBuf c) {
        int n = c.numComponents();
        if (n == 0) {
            sr.wrapEmptyBuffer();
            return sr;
        } else if (n == 1) {
            return wrap(c.component(0));
        } else if (n == 2) {
            var tsr = this.tsr;
            if (tsr == null)
                this.tsr = tsr = new TwoSegmentRope();
            var sr = wrapComponent(c.component(0));
            if (sr != null) {
                tsr.wrapFirst(sr);
                sr = wrapComponent(c.component(1));
                if (sr != null) {
                    tsr.wrapSecond(sr);
                    return tsr;
                }
            }
        }
        return copy(c);
    }

    private SegmentRope copy(ByteBuf bb) {
        int size = bb.readableBytes();
        byte[] copy = this.copy;
        if (copy == null || copy.length < size)
            this.copy = copy = new byte[size];
        bb.readBytes(this.copy);
        sr.wrapSegment(MemorySegment.ofArray(copy), copy, 0, size);
        return sr;
    }
}
