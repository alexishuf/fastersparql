package com.github.alexishuf.fastersparql.model.rope;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;

/**
 * A {@link SegmentRope} which can be re-mapped to other {@link MemorySegment}s.
 */
public class SegmentRopeView extends SegmentRope  {
    public static final int BYTES = 16 + 2*4 + 8+2*4;

    public SegmentRopeView() { }

    /**
     * Wrap the empty string, represented by a singleton {@code byte[0]}.
     *
     * @return {@code this}
     */
    public @This SegmentRopeView wrapEmpty() {
        this.segment = EMPTY_SEGMENT;
        this.utf8    = EMPTY_UTF8;
        this.offset  = 0;
        this.len     = 0;
        return this;
    }

    /**
     * Wrap the whole {@link MemorySegment} given
     *
     * @return {@code this}
     */
    public @This SegmentRopeView wrap(MemorySegment segment) {
        this.segment = segment;
        this.utf8    = arrayOf(segment);
        this.offset  = 0;
        this.len     = asIntLen(segment.byteSize());
        return this;
    }

    /**
     * Wrap the whole {@link MemorySegment} given
     *
     * @return {@code this}
     */
    public @This SegmentRopeView wrap(byte[] u8) {
        this.segment = MemorySegment.ofArray(u8);
        this.utf8    = u8;
        this.offset  = 0;
        this.len     = asIntLen(u8.length);
        return this;
    }

    /**
     * Wrap the whole {@link MemorySegment} given
     *
     * @return {@code this}
     */
    public @This SegmentRopeView wrap(byte[] u8, int offset, int len) {
        checkRange(offset, len, u8.length);
        this.segment = MemorySegment.ofArray(u8);
        this.utf8    = u8;
        this.offset  = offset;
        this.len     = len;
        return this;
    }

    /**
     * Wrap the whole {@link MemorySegment} given.
     *
     * @return {@code this}
     */
    public @This SegmentRopeView wrap(MemorySegment segment, byte @Nullable[] utf8) {
        if (DEBUG)
            checkSameArray(segment, utf8);
        this.segment = segment;
        this.utf8    = utf8;
        this.offset  = 0;
        this.len     = asIntLen(segment.byteSize());
        return this;
    }

    /**
     * Wrap the range {@code [offset, offset+begin)} of the given {@link MemorySegment}
     *
     * @return {@code this}
     */
    public @This SegmentRopeView wrap(MemorySegment segment, long offset, int len) {
        checkRange(offset, len, segment.byteSize());
        this.segment = segment;
        this.utf8    = arrayOf(segment);
        this.offset  = offset;
        this.len     = len;
        return this;
    }

    /**
     * Wrap the range {@code [offset, offset+begin)} of the given {@link MemorySegment}
     *
     * @return {@code this}
     */
    public @This SegmentRopeView wrap(MemorySegment segment, byte @Nullable[] utf8, long offset, int len) {
        checkRange(offset, len, segment.byteSize());
        if (DEBUG)
            checkSameArray(segment, utf8);
        this.segment = segment;
        this.utf8    = utf8;
        this.offset  = offset;
        this.len     = len;
        return this;
    }

    /**
     * Wraps the {@link ByteBuffer#remaining()} bytes in {@code bb} starting at
     * {@link ByteBuffer#position()}. changing the position and limit of {@code bb} will not
     * affect this view.
     *
     * @return {@code this}
     */
    public @This SegmentRopeView wrap(ByteBuffer bb) { return wrap(MemorySegment.ofBuffer(bb)); }

    /**
     * Wraps the same bytes owned or wrapped by {@code other}.
     *
     * @return {@code this}
     */
    public @This SegmentRopeView wrap(SegmentRope other) {
        this.segment = other.segment;
        this.utf8    = other.utf8;
        this.offset  = other.offset;
        this.len     = other.len;
        return this;
    }

    /**
     * Wraps {@code len} bytes visible through {@code other} starting at {@code offset}
     * (relative to the {@link SegmentRope}, not to its {@link MemorySegment}).
     *
     * @return {@code this}
     */
    public @This SegmentRopeView wrap(SegmentRope other, int offset, int len) {
        checkRange(offset, len, other.len);
        this.segment = other.segment;
        this.utf8    = other.utf8;
        this.offset  = other.offset+offset;
        this.len     = len;
        return this;
    }

    public @This SegmentRopeView slice(long offset, int len) {
        this.offset = offset;
        this.len = len;
        return this;
    }
}
