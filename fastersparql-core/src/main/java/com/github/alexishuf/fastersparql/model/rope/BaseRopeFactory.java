package com.github.alexishuf.fastersparql.model.rope;

import com.github.alexishuf.fastersparql.FSProperties;
import org.checkerframework.common.returnsreceiver.qual.This;
import org.slf4j.LoggerFactory;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import static com.github.alexishuf.fastersparql.util.LowLevelHelper.U;
import static com.github.alexishuf.fastersparql.util.LowLevelHelper.U8_BASE;
import static java.lang.System.arraycopy;
import static java.nio.charset.StandardCharsets.UTF_8;

@SuppressWarnings("unchecked")
sealed abstract class BaseRopeFactory<F extends BaseRopeFactory<F>>
        permits PrivateRopeFactory, RopeFactory {
    protected static final int CHUNK_SIZE = 128;
    protected static final int FULL_CHUNK = CHUNK_SIZE-24;
    private static final byte[]        EMPTY_CHUNK = new byte[0];
    private static final MemorySegment EMPTY_SEG   = MemorySegment.ofArray(EMPTY_CHUNK);
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
    private int dstPos;
    private short begin, oldChunkBegin;
    private MemorySegment oldChunkSegment;
    private byte[] oldChunk;

    protected BaseRopeFactory(boolean alloc) {
        if (alloc) {
            chunkSegment = MemorySegment.ofArray(chunk = new byte[CHUNK_SIZE]);
        } else {
            chunk        = EMPTY_CHUNK;
            chunkSegment = EMPTY_SEG;
        }
        oldChunkBegin   = 0;
        oldChunk        = EMPTY_CHUNK;
        oldChunkSegment = EMPTY_SEG;
    }


    protected final void reserve(int bytes) {
        if (WASTE) {
            reserveWaste(bytes);
        } else {
            if (this.begin + bytes > chunk.length) {
                if (chunk.length < FULL_CHUNK) {
                    oldChunkSegment = chunkSegment;
                    oldChunk        = chunk;
                    oldChunkBegin   = begin;
                }
                chunk        = new byte[Math.max(bytes, CHUNK_SIZE)];
                chunkSegment = MemorySegment.ofArray(chunk);
                begin        = 0;
                dstPos       = 0;
            }
        }
    }
    private void detachChunk() {
        this.chunk         = oldChunk;
        this.chunkSegment  = oldChunkSegment;
        this.begin         = oldChunkBegin;
        this.dstPos        = oldChunkBegin;
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
                this.begin = (short)end;
            else
                detachChunk();
            return rope;
        } else {
            dstPos = begin;
            return len == 0 ? FinalSegmentRope.EMPTY : SINGLE_CHAR_ROPES[chunk[begin]];
        }
    }

    protected final PooledSegmentRopeView pooledView0(int begin, int len) {
        return PooledSegmentRopeView.of(chunkSegment, chunk, this.begin+begin, len);
    }

    protected MemorySegment  segment0() { return chunkSegment; }
    protected byte[]            utf80() { return        chunk; }
    protected int              begin0() { return        begin; }
    protected int                len0() { return dstPos-begin; }

    protected void done0() {
        final int end = dstPos;
        if (end < FULL_CHUNK)
            begin = (short)end;
        else
            detachChunk();
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

    public byte[] bytes() { return chunk; }

    public int beginBytesAdd() { return dstPos; }

    public @This F endBytesAdd(int dstPos) {
        this.dstPos = dstPos;
        return (F)this;
    }

    public void erase(int begin, int len) {
        int physBegin = this.begin+begin, physEnd = physBegin+len;
        if (begin < 0 || physBegin > dstPos) {
            var msg = begin < 0 ? "begin < 0" : "begin+len > bytes in  string";
            throw new IndexOutOfBoundsException(msg);
        }
        arraycopy(chunk, physEnd, chunk, physBegin, dstPos-physEnd);
        dstPos -= len;
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

    public @This F add(MemorySegment segment, byte[] u8, long offset, int len) {
        if (U == null)
            return add(segment, offset, len);
        if (offset < 0 || offset+len > segment.byteSize())
            throw new IllegalArgumentException("[offset, offset+len) is out of bounds");
        long phys = segment.address() + (u8 == null ? 0 : U8_BASE) + offset;
        U.copyMemory(u8, phys, chunk, dstPos+U8_BASE, len);
        dstPos += len;
        return (F)this;
    }
}
