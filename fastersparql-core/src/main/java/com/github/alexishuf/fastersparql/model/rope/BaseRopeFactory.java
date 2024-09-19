package com.github.alexishuf.fastersparql.model.rope;

import com.github.alexishuf.fastersparql.FSProperties;
import org.checkerframework.common.returnsreceiver.qual.This;
import org.slf4j.LoggerFactory;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import static java.lang.System.arraycopy;
import static java.nio.charset.StandardCharsets.UTF_8;

@SuppressWarnings("unchecked")
sealed abstract class BaseRopeFactory<F extends BaseRopeFactory<F>>
        permits PrivateRopeFactory, RopeFactory {
    protected static final int CHUNK_SIZE = 128-24;
    private   static final int FULL_CHUNK = CHUNK_SIZE;
    private   static final byte[] EMPTY_CHUNK = new byte[0];
    private static final boolean WASTE = FSProperties.batchNoInternIri();
    static {
        if (WASTE) {
            var logger = LoggerFactory.getLogger(BaseRopeFactory.class);
            logger.warn("{}=true, RopeFactory will not reuse chunks",
                        FSProperties.BATCH_NO_INTERN_IRI);
        }
    }

    private MemorySegment chunkSegment;
    private byte[] chunk;
    private int begin, dstPos;

    protected BaseRopeFactory(int initialChunkSize) {
        chunkSegment = MemorySegment.ofArray(chunk = new byte[initialChunkSize]);
    }

    protected final void reserve(int bytes) {
        if (WASTE) {
            reserveWaste(bytes);
        } else {
            if (this.begin + bytes < chunk.length) {
                this.dstPos = this.begin;
            } else {
                this.chunk        = new byte[Math.max(bytes, CHUNK_SIZE)];
                this.chunkSegment = MemorySegment.ofArray(chunk);
                this.begin = 0;
                this.dstPos = 0;
            }
        }
    }
    private void detachChunk() {
        this.chunk  = EMPTY_CHUNK;
        this.begin  = 0;
        this.dstPos = 0;
    }

    private void reserveWaste(int bytes) {
        chunkSegment = MemorySegment.ofArray(chunk = new byte[bytes]);
        begin        = 0;
        dstPos       = 0;
    }

    protected final FinalSegmentRope take0() {
        final int begin = this.begin, end = dstPos, len = end-begin;
        if (len > 1) {
            var rope = new FinalSegmentRope(chunkSegment, chunk, begin, len);
            if (end < FULL_CHUNK)
                this.begin = end;
            else
                detachChunk();
            return rope;
        } else {
            return len == 0 ? FinalSegmentRope.EMPTY : SINGLE_CHAR_ROPES[chunk[begin]];
        }
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
        dstPos = RopeEncoder.charSequence2utf8(cs, 0, cs.length(), chunk, dstPos);
        return (F)this;
    }

    public @This F add(CharSequence cs, int begin, int end) {
        if (cs instanceof Rope r)
            return add(r, begin, end);
        dstPos = RopeEncoder.charSequence2utf8(cs, begin, end, chunk, dstPos);
        return (F)this;
    }

    public @This F add(byte b) {
        chunk[dstPos++] = b;
        return (F)this;
    }

    public @This F add(char c) {
        dstPos = RopeEncoder.char2utf8(c, chunk, dstPos);
        return (F)this;
    }

    public @This F add(long n) {
        if (n >= 0 && n < 10) {
            chunk[dstPos++] = (byte)('0'+n);
        } else if (n == Long.MIN_VALUE) {
            add(MIN_LONG_U8);
        } else {
            int sign = (int)(n>>>63), i = dstPos+sign+(int)Math.floor(Math.log10(n)+1);
            dstPos = i;
            n = Math.abs(n);
            for (long rem = n%10; n  > 0; n = n/10, rem = n%10)
                chunk[--i] = (byte)('0' + rem);
            if (sign == 1)
                chunk[--i] = '-';
        }
        return (F)this;
    }
    private static final byte[] MIN_LONG_U8 = Long.toString(Long.MIN_VALUE).getBytes(UTF_8);

    public @This F add(Rope r) {
        int len = r.len;
        r.copy(0, len, chunk, dstPos);
        dstPos += len;
        return (F)this;
    }

    public @This F add(MutableRope r) {
        int len = r.len;
        arraycopy(r.u8(), (int)r.offset, chunk, dstPos, len);
        dstPos += len;
        return (F)this;
    }

    public @This F add(Rope r, int begin, int end) {
        r.copy(begin, end, chunk, dstPos);
        dstPos += end-begin;
        return (F)this;
    }

    public @This F add(MutableRope r, int begin, int end) {
        int len = end - begin;
        arraycopy(r.u8(), (int)r.offset+begin, chunk, dstPos, len);
        dstPos += len;
        return (F)this;
    }

    public @This F add(byte[] u8) {
        arraycopy(u8, 0, chunk, dstPos, u8.length);
        dstPos += u8.length;
        return (F)this;
    }

    public @This F add(byte[] u8, int begin, int end) {
        arraycopy(u8, begin, chunk, dstPos, end-begin);
        dstPos += end-begin;
        return (F)this;
    }

    public @This F add(MemorySegment segment, long offset, int len) {
        MemorySegment.copy(segment, ValueLayout.JAVA_BYTE, offset, chunk, dstPos, len);
        dstPos += len;
        return (F)this;
    }
}
