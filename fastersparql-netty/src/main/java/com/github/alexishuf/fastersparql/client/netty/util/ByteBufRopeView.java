package com.github.alexishuf.fastersparql.client.netty.util;

import com.github.alexishuf.fastersparql.model.rope.*;
import com.github.alexishuf.fastersparql.util.concurrent.Bytes;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.foreign.MemorySegment;

import static java.util.Objects.requireNonNull;

public final class ByteBufRopeView implements AutoCloseable {
    private static final MemorySegment   EMPTY_SEGMENT = FinalSegmentRope.EMPTY.segment;
    private static final byte @NonNull[] EMPTY_U8      = requireNonNull(FinalSegmentRope.EMPTY.utf8);

    private SegmentRopeView sr;
    private @MonotonicNonNull TwoSegmentRope tsr;
    private @Nullable Bytes copy;

    public ByteBufRopeView(SegmentRopeView view) { sr = view; }

    @Override public void close() {
        if (copy != null)
            copy.recycle(this);
        if (sr instanceof PooledSegmentRopeView p) {
            p.close();
            sr = null;
        } else {
            sr.wrapEmpty();
        }
        if (tsr != null) {
            tsr.wrapFirst (EMPTY_SEGMENT, EMPTY_U8, 0, 0);
            tsr.wrapSecond(EMPTY_SEGMENT, EMPTY_U8, 0, 0);
        }
    }

    /**
     * Get a {@link SegmentRope} whose bytes are the same as those in {@code bb} between
     * {@link ByteBuf#readerIndex()} and {@link ByteBuf#writerIndex()} (not inclusive). If
     * possible, the segment rope will directly wrap the underlying heap or native memory that the
     * {@link ByteBuf} is itself wrapping.
     *
     *
     * <p><strong>Important</strong>: The returned {@link SegmentRope} will become empty or wrap
     * invalid data if <strong>ANY</strong> of these methods are called:</p>
     * <ul>
     *     <li>{@link #close()} is called on {@code this}</li>
     *     <li>{@link #wrap(ByteBuf)} is called on {@code this}</li>
     *     <li>{@code wrapAsSingle(ByteBuf)} is called on {@code this}</li>
     *     <li>{@link ByteBuf#release()} is called on {@code bb}</li>
     * </ul>
     *
     * @param bb a {@link ByteBuf}
     * @return A {@link SegmentRope} wrapping the readable bytes in {@code bb} or a copy of them.
     *         The {@link SegmentRope} is invalidated when {@code bb} is released, {@code this}
     *         is closed or another {@code wrap*()} calls is made on {@code this}.
     */
    public SegmentRope wrapAsSingle(ByteBuf bb) {
        if (!bb.isReadable())
            return sr.wrapEmpty();
        while (bb instanceof CompositeByteBuf c) {
            ByteBuf c0 = c.component(0), c1;
            int n = c.numComponents();
            if (n == 1) {
                bb = c0;
            } else {
                if (n == 2) {
                    if (!(c1=c.component(1)).isReadable()) {
                        bb = c0;
                        continue;
                    } else if (!c0.isReadable()) {
                        bb = c1;
                        continue;
                    }
                }
                return copy(bb);
            }
        }
        return wrapSingle(bb);
    }

    /**
     * Get a {@link PlainRope} whose bytes are the same as those in {@code bb} between
     * {@link ByteBuf#readerIndex()} and {@link ByteBuf#writerIndex()} (not inclusive). If
     * possible, the {@link PlainRope} rope will directly wrap the underlying heap or native
     * memory segment(s) that the {@link ByteBuf} is itself wrapping.
     *
     *
     * <p><strong>Important</strong>: The returned {@link PlainRope} will become empty or wrap
     * invalid data if <strong>ANY</strong> of these methods are called:</p>
     * <ul>
     *     <li>{@link #close()} is called on {@code this}</li>
     *     <li>{@link #wrapAsSingle(ByteBuf)} is called on {@code this}</li>
     *     <li>{@code wrap(ByteBuf)} is called on {@code this}</li>
     *     <li>{@link ByteBuf#release()} is called on {@code bb}</li>
     * </ul>
     *
     * @param bb a {@link ByteBuf}
     * @return A {@link SegmentRope} wrapping the readable bytes in {@code bb} or a copy of them.
     *         The {@link SegmentRope} is invalidated when {@code bb} is released, {@code this}
     *         is closed or another {@code wrap*()} calls is made on {@code this}.
     */
    public PlainRope wrap(ByteBuf bb) {
        if (!bb.isReadable())
            return sr.wrapEmpty();
        while (bb instanceof CompositeByteBuf c) {
            ByteBuf c0 = c.component(0), c1;
            int n = c.numComponents();
            if (n == 1) {
                bb = c0;
            } else {
                if (n == 2) {
                    c1 = c.component(1);
                    if (!c0.isReadable()) {
                        bb = c1;
                        continue;
                    } else if (!c1.isReadable()) {
                        bb = c0;
                        continue;
                    } else if (tryWrapTwo(c0, c1)) {
                        return tsr;
                    }
                }
                return copy(bb);
            }
        }
        return wrapSingle(bb);
    }

    private @Nullable SegmentRope wrapSingleNoCopy(ByteBuf bb) {
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
            segment = MemorySegment.ofBuffer(bb.internalNioBuffer(offset, len));
            offset = 0;
        } else {
            return null;
        }
        sr.wrap(segment, u8, offset, len);
        return sr;
    }

    private SegmentRope wrapSingle(ByteBuf bb) {
        var sr = wrapSingleNoCopy(bb);
        return sr != null ? sr : copy(bb);
    }

    private boolean tryWrapTwo(ByteBuf c0, ByteBuf c1) {
        var tsr = this.tsr;
        if (tsr == null)
            this.tsr = tsr = new TwoSegmentRope();
        var sr = wrapSingleNoCopy(c0);
        if (sr != null) {
            tsr.wrapFirst(sr);
            sr  = wrapSingleNoCopy(c1);
            if (sr != null) {
                tsr.wrapSecond(sr);
                return true;
            }
        }
        return false;
    }

    private SegmentRope copy(ByteBuf bb) {
        int size = bb.readableBytes();
        Bytes copy = this.copy;
        if (copy == null || copy.arr.length < size) {
            if (copy != null) copy.recycle(this);
            this.copy = copy = Bytes.atLeast(size).takeOwnership(this);
        }
        bb.readBytes(copy.arr, 0, size);
        sr.wrap(copy.segment, copy.arr, 0, size);
        return sr;
    }
}
