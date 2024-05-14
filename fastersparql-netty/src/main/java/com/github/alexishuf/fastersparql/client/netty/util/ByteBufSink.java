package com.github.alexishuf.fastersparql.client.netty.util;

import com.github.alexishuf.fastersparql.model.rope.ByteSink;
import com.github.alexishuf.fastersparql.model.rope.Rope;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import com.github.alexishuf.fastersparql.model.rope.TwoSegmentRope;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.PooledByteBufAllocator;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;

import java.lang.foreign.MemorySegment;

import static java.nio.charset.StandardCharsets.UTF_8;

public class ByteBufSink implements ByteSink<ByteBufSink, ByteBuf> {
    public static final int NORMAL_HINT = PooledByteBufAllocator.defaultPageSize();
    public static final int    MIN_HINT = 1024;
    public static final int    MAX_HINT = 32768;

    private ByteBufAllocator alloc;
    private ByteBuf bb;
    private int sizeHint = MIN_HINT;

    public ByteBufSink(ByteBufAllocator      alloc) { this.alloc = alloc; }

    public void alloc(ByteBufAllocator alloc) { this.alloc = alloc; }

    public ByteBufOutputStream asOutputStream() { return new ByteBufOutputStream(touch().bb); }

    @Override public int sizeHint() { return sizeHint;  }

    @Override public void sizeHint(int hint) {
        int safeHint = Math.max(MIN_HINT, Math.min(MAX_HINT, hint));
        sizeHint = 1 << (32 - Integer.numberOfLeadingZeros(safeHint -1));
    }

    @Override public ByteBuf take() {
        ByteBuf bb = this.bb;
        this.bb = null;
        if (bb == null)
            throw new IllegalStateException("no ByteBuf to take()");
        return bb;
    }

    @Override public ByteBuf takeUntil(int len) {
        ByteBuf taken = take();
        int takenLen = taken.readableBytes();
        if (len > takenLen) {
            taken.release();
            throw new IndexOutOfBoundsException("len > taken.readableBytes()");
        } else if (len < takenLen) {
            int tailLen = takenLen - len;
            bb = alloc.buffer(Math.max(sizeHint, tailLen));
            bb.writeBytes(taken, len, tailLen); // copy un-taken bytes back
            taken.writerIndex(len);             // remove un-taken bytes
        }
        return taken;
    }

    @Override public boolean needsTouch() { return bb == null; }

    @Override public ByteBufSink touch() {
        if (bb == null)
            bb = alloc.buffer(sizeHint);
        return this;
    }

    @Override public void close() {
        ByteBuf bb = this.bb;
        this.bb = null;
        if (bb != null)
            bb.release();
    }

    @Override public boolean isEmpty() { return bb == null || bb.readableBytes() == 0; }

    @Override public int len() { return bb == null ? 0 : bb.readableBytes(); }

    @Override public int freeCapacity() { return bb == null ? 0 : bb.writableBytes(); }

    @Override public @This ByteBufSink append(byte[] arr, int begin, int len) {
        bb.writeBytes(arr, begin, len);
        return this;
    }

    @Override public @This ByteBufSink append(char c) {
        if (c > 127) throw new IllegalArgumentException();
        bb.writeByte(c);
        return this;
    }

    @Override public @This ByteBufSink append(byte c) {
        bb.writeByte(c);
        return this;
    }

    @Override public @This ByteBufSink append(Rope rope) {
        NettyRopeUtils.write(bb, rope);
        return this;
    }

    @Override public @This ByteBufSink append(SegmentRope rope) {
        NettyRopeUtils.write(bb, rope);
        return this;
    }

    @Override public @This ByteBufSink append(Rope rope, int begin, int end) {
        int len = end - begin;
        if (rope == null || len <= 0) {
            return this;
        } else if (rope instanceof SegmentRope sr) {
            append(sr.segment, sr.utf8, sr.offset+begin, len);
        } else {
            MemorySegment fst, snd;
            byte[] fstU8, sndU8;
            long fstOff, sndOff;
            int fstLen;
            if (rope instanceof TwoSegmentRope t) {
                fst = t.fst; fstU8 = t.fstU8; fstOff = t.fstOff; fstLen = t.fstLen;
                snd = t.snd; sndU8 = t.sndU8; sndOff = t.sndOff;
            } else if (rope instanceof Term t) {
                SegmentRope fr = t.first(), sr = t.second();
                fst = fr.segment; fstU8 = fr.utf8; fstOff = fr.offset; fstLen = fr.len;
                snd = sr.segment; sndU8 = sr.utf8; sndOff = sr.offset;
            } else {
                throw new UnsupportedOperationException("Unsupported Rope type: "+rope.getClass());
            }
            if (begin < fstLen) {
                append(fst, fstU8, fstOff+begin, fstLen-=begin);
            } else {
                sndOff += begin-fstLen;
                fstLen = 0;
            }
            append(snd, sndU8, sndOff, len-fstLen);
        }
        return this;
    }

    @Override public @This ByteBufSink append(SegmentRope rope, int begin, int end) {
        int len = end - begin;
        if (rope != null && len > 0)
            append(rope.segment, rope.utf8, rope.offset+begin, len);
        return this;
    }

    @Override public @This ByteBufSink append(MemorySegment segment, byte @Nullable [] array,
                                              long offset, int len) {
        NettyRopeUtils.write(bb, segment, array, offset, len);
        return this;
    }

    @Override public @This ByteBufSink append(CharSequence cs) {
        if (cs instanceof Rope r) return append(r);
        bb.writeCharSequence(cs, UTF_8);
        return this;
    }

    @Override public @This ByteBufSink ensureFreeCapacity(int increment) {
        bb.ensureWritable(increment);
        return this;
    }

    @Override public String toString() {
        return bb == null ? "null" : bb.toString(UTF_8);
    }
}
