package com.github.alexishuf.fastersparql.client.netty.util;

import com.github.alexishuf.fastersparql.model.rope.ByteSink;
import com.github.alexishuf.fastersparql.model.rope.Rope;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import com.github.alexishuf.fastersparql.model.rope.TwoSegmentRope;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.concurrent.EventExecutor;
import org.checkerframework.common.returnsreceiver.qual.This;

import java.lang.foreign.MemorySegment;
import java.util.concurrent.locks.LockSupport;

import static java.nio.charset.StandardCharsets.UTF_8;

public class ByteBufSink implements ByteSink<ByteBufSink, ByteBuf> {
    private ByteBufAllocator alloc;
    private ByteBuf bb;
    private volatile ByteBuf asyncBB;
    private volatile Thread consumer;
    private EventExecutor executor;
    private Runnable allocTask;
    private boolean spawnedAllocTask = false;
    private int sizeHint = 256;

    public ByteBufSink(ChannelHandlerContext   ctx) { alloc(ctx); }
    public ByteBufSink(ByteBufAllocator      alloc) { this.alloc = alloc; }

    public void alloc(ChannelHandlerContext ctx) {
        this.alloc = ctx.alloc();
        this.executor = ctx.executor();
        this.allocTask = this::doAlloc;
        consumer = Thread.currentThread();
        spawnAllocTask();
    }

    public void alloc(ByteBufAllocator alloc) {
        this.alloc = alloc;
    }

    private void doAlloc() {
        asyncBB = this.alloc.buffer(sizeHint);
        LockSupport.unpark(consumer);
    }

    private void spawnAllocTask() {
        if (!spawnedAllocTask && executor != null) {
            spawnedAllocTask = true;
            executor.execute(allocTask);
        }
    }

    @Override public ByteBuf take() {
        ByteBuf bb = this.bb;
        this.bb = null;
        if (bb == null)
            throw new IllegalStateException("no ByteBuf to take()");
        // average sizeHint and bytes written, then generously align to 64 bytes
        sizeHint = 64 + (((sizeHint + bb.readableBytes()) >> 1) & ~63);
        return bb;
    }

    @Override public ByteBufSink touch() {
        if (bb == null) {
            Thread consumer;
            if (executor == null || executor.inEventLoop(consumer = Thread.currentThread())) {
                bb = alloc.buffer(sizeHint);
            } else {
                if (this.consumer != consumer)
                    this.consumer = consumer;
                spawnAllocTask(); // spawn task in case of previous release()
                while ((bb = asyncBB) == null)
                    LockSupport.park(executor);
                asyncBB = null;
                spawnedAllocTask = false;
                spawnAllocTask(); // spawn task to satisfy next touch() call
            }
        }
        return this;
    }

    @Override public void release() {
        ByteBuf bb = this.bb;
        this.bb = null;
        if (bb != null)
            bb.release();
        if (spawnedAllocTask) {
            if (executor.inEventLoop()) {
                bb = asyncBB;
            } else {
                while ((bb = asyncBB) == null && spawnedAllocTask && !executor.isTerminated())
                    LockSupport.park(executor);
            }
            if (bb != null) {
                bb.release();
                asyncBB = null;
                spawnedAllocTask = false;
            }
        }
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
        NettyRopeUtils.write(bb, rope);
        return this;
    }

    @Override public @This ByteBufSink append(Rope rope, int begin, int end) {
        int len = end - begin;
        if (rope == null || len <= 0) {
            return this;
        } else if (rope instanceof SegmentRope sr) {
            append(sr.segment, sr.offset+begin, len);
        } else {
            MemorySegment fst, snd;
            long fstOff, sndOff;
            int fstLen;
            if (rope instanceof TwoSegmentRope t) {
                fst = t.fst; fstOff = t.fstOff; fstLen = t.fstLen;
                snd = t.snd;
            } else if (rope instanceof Term t) {
                SegmentRope fr = t.first();
                fst = fr.segment; fstOff = fr.offset; fstLen = fr.len;
                snd = t.second().segment;
            } else {
                throw new UnsupportedOperationException("Unsupported Rope type: "+rope.getClass());
            }
            fstOff += begin;
            sndOff = Math.max(0, begin-fstLen);
            fstLen = Math.max(0, Math.min(fstLen, end)-begin);
            append(fst, fstOff, fstLen);
            append(snd, sndOff, len-fstLen);
        }
        return this;
    }

    @Override public @This ByteBufSink append(MemorySegment segment, long offset, int len) {
        NettyRopeUtils.write(bb, segment, offset, len);
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
