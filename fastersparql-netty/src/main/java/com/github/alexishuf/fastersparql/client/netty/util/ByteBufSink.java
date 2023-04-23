package com.github.alexishuf.fastersparql.client.netty.util;

import com.github.alexishuf.fastersparql.model.rope.*;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.checkerframework.common.returnsreceiver.qual.This;

import static java.lang.Math.min;
import static java.nio.charset.StandardCharsets.UTF_8;

public class ByteBufSink implements ByteSink<ByteBufSink> {
    private ByteBufAllocator alloc;
    private ByteBuf bb;
    private int sizeHint = 256;

    public ByteBufSink(ByteBufAllocator alloc) { this.alloc = alloc; }

    public void alloc(ByteBufAllocator alloc) {
        this.alloc = alloc;
    }

    public ByteBuf take() {
        ByteBuf bb = this.bb;
        this.bb = null;
        if (bb == null)
            throw new IllegalStateException("no ByteBuf to take()");
        // increase/decrease size rounding UP to a multiple of 64
        sizeHint = 64 + ((sizeHint + bb.readableBytes()) >> 1 & ~63);
        return bb;
    }

    public ByteBufSink touch() {
        if (bb == null)
            bb = alloc.buffer(sizeHint);
        return this;
    }

    public void release() {
        ByteBuf bb = this.bb;
        this.bb = null;
        if (bb != null)
            bb.release();
    }

    @Override public boolean isEmpty() {
        return bb == null || bb.readableBytes() == 0;
    }

    @Override public int len() {
        return bb == null ? 0 : bb.readableBytes();
    }

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
        switch (rope) {
            case ByteRope b -> bb.writeBytes(b.utf8, b.offset, b.len);
            case BufferRope b -> bb.writeBytes(b.buffer().slice(b.offset(), b.len));
            case Term t -> {
                var shared = RopeDict.getTolerant(t.flaggedDictId).utf8;
                if (t.flaggedDictId < 0) {
                    bb.writeBytes(t.local);
                    bb.writeBytes(shared);
                } else {
                    bb.writeBytes(shared);
                    bb.writeBytes(t.local);
                }
            }
            default -> throw new UnsupportedOperationException();
        }
        return this;
    }

    @Override public @This ByteBufSink append(Rope rope, int begin, int end) {
        switch (rope) {
            case ByteRope b -> bb.writeBytes(b.utf8, begin, end-begin);
            case BufferRope b -> bb.writeBytes(b.buffer().slice(begin, end-begin));
            case Term t -> {
                var shared = RopeDict.getTolerant(t.flaggedDictId).utf8;
                var fst = t.flaggedDictId < 0 ? t.local :  shared;
                var snd = t.flaggedDictId < 0 ?  shared : t.local;
                if (begin < fst.length) {
                    bb.writeBytes(fst, begin, min(fst.length, end)-begin);
                    begin = 0;
                } else {
                    begin -= fst.length;
                }
                end -= fst.length;
                if (begin < end)
                    bb.writeBytes(snd, begin, end-begin);
            }
            default -> throw new UnsupportedOperationException();
        }
        return this;
    }

    @Override public @This ByteBufSink append(CharSequence cs) {
        if (cs instanceof Rope r) return append(r);
        bb.writeCharSequence(cs, UTF_8);
        return this;
    }

    @Override public @This ByteBufSink append(CharSequence cs, int begin, int end) {
        if (cs instanceof Rope r) return append(r);
        bb.writeCharSequence(cs.subSequence(begin, end), UTF_8);
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
