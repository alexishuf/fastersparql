package com.github.alexishuf.fastersparql.model.rope;

import org.checkerframework.common.returnsreceiver.qual.This;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import static java.lang.System.arraycopy;
import static java.nio.charset.StandardCharsets.UTF_8;

@SuppressWarnings("unchecked")
sealed abstract class BaseRopeFactory<F extends BaseRopeFactory<F>>
        permits PrivateRopeFactory, RopeFactory {
    protected static final int CHUNK_SIZE = 128;
    protected static final int FULL_CHUNK = CHUNK_SIZE-32;
    private MemorySegment chunkSegment;
    private byte[] chunk, dst;
    private int chunkPos, dstPos;

    protected BaseRopeFactory(int initialChunkSize) {
        chunkSegment = MemorySegment.ofArray(dst = chunk = new byte[initialChunkSize]);
    }

    protected final void reserve(int bytes) {
        byte[] dst = this.chunk;
        if (this.chunkPos + bytes < dst.length) {
            this.dstPos = this.chunkPos;
        } else {
            if (bytes < CHUNK_SIZE && this.chunk.length > FULL_CHUNK) {
                this.chunk        = dst = new byte[CHUNK_SIZE];
                this.chunkSegment = MemorySegment.ofArray(dst);
                this.chunkPos     = 0;
            } else {
                dst = new byte[bytes];
            }
            this.dstPos = 0;
        }
        this.dst = dst;
    }

    protected final FinalSegmentRope take0() {
        MemorySegment seg = chunkSegment;
        int begin = 0, len = dstPos;
        byte[] dst = this.dst;
        if (dst == chunk) {
            begin    = chunkPos;
            chunkPos = len;
            len     -= begin;
        } else {
            this.dst    = chunk;
            this.dstPos = chunkPos;
            seg         = MemorySegment.ofArray(dst);
        }
        if (len > 1)
            return new FinalSegmentRope(seg, dst, begin, len);
        else if (len == 0)
            return FinalSegmentRope.EMPTY;
        else
            return SINGLE_CHAR_ROPES[dst[begin]];
    }

    private static final FinalSegmentRope[] SINGLE_CHAR_ROPES;
    static {
        byte[] chars = new byte[128];
        MemorySegment charsSegment = MemorySegment.ofArray(chars);
        for (int i = 0; i < chars.length; i++)
            chars[i] = (byte)i;
        FinalSegmentRope[] ropes = new FinalSegmentRope[128];
        for (int i = 0; i < ropes.length; i++)
            ropes[i] = new FinalSegmentRope(charsSegment, chars, i, 1);
        SINGLE_CHAR_ROPES = ropes;
    }

    public @This F add(CharSequence cs) {
        if (cs instanceof Rope r)
            return add(r);
        dstPos = RopeEncoder.charSequence2utf8(cs, 0, cs.length(), dst, dstPos);
        return (F)this;
    }

    public @This F add(CharSequence cs, int begin, int end) {
        if (cs instanceof Rope r)
            return add(r, begin, end);
        dstPos = RopeEncoder.charSequence2utf8(cs, begin, end, dst, dstPos);
        return (F)this;
    }

    public @This F add(byte b) {
        dst[dstPos++] = b;
        return (F)this;
    }

    public @This F add(char c) {
        dstPos = RopeEncoder.char2utf8(c, dst, dstPos);
        return (F)this;
    }

    public @This F add(long n) {
        if (n >= 0 && n < 10) {
            dst[dstPos++] = (byte)('0'+n);
        } else if (n == Long.MIN_VALUE) {
            add(MIN_LONG_U8);
        } else {
            int sign = (int)(n>>>63), i = dstPos+sign+(int)Math.floor(Math.log10(n)+1);
            dstPos = i;
            n = Math.abs(n);
            for (long rem = n%10; n  > 0; n = n/10, rem = n%10)
                dst[--i] = (byte)('0' + rem);
            if (sign == 1)
                dst[--i] = '-';
        }
        return (F)this;
    }
    private static final byte[] MIN_LONG_U8 = Long.toString(Long.MIN_VALUE).getBytes(UTF_8);

    public @This F add(Rope r) {
        int len = r.len;
        r.copy(0, len, dst, dstPos);
        dstPos += len;
        return (F)this;
    }

    public @This F add(MutableRope r) {
        int len = r.len;
        arraycopy(r.u8(), (int)r.offset, dst, dstPos, len);
        dstPos += len;
        return (F)this;
    }

    public @This F add(Rope r, int begin, int end) {
        r.copy(begin, end, dst, dstPos);
        dstPos += end-begin;
        return (F)this;
    }

    public @This F add(MutableRope r, int begin, int end) {
        int len = end - begin;
        arraycopy(r.u8(), (int)r.offset+begin, dst, dstPos, len);
        dstPos += len;
        return (F)this;
    }

    public @This F add(byte[] u8) {
        arraycopy(u8, 0, dst, dstPos, u8.length);
        dstPos += u8.length;
        return (F)this;
    }

    public @This F add(byte[] u8, int begin, int end) {
        arraycopy(u8, begin, dst, dstPos, end-begin);
        dstPos += end-begin;
        return (F)this;
    }

    public @This F add(MemorySegment segment, long offset, int len) {
        MemorySegment.copy(segment, ValueLayout.JAVA_BYTE, offset, dst, dstPos, len);
        dstPos += len;
        return (F)this;
    }
}
