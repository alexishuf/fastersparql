package com.github.alexishuf.fastersparql.store.index;

import com.github.alexishuf.fastersparql.model.rope.SegmentRope;

import java.io.IOException;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;

import static java.lang.invoke.MethodHandles.lookup;

public class DictWriter implements AutoCloseable  {
    private static final VarHandle WRITING;
    static {
        try {
            WRITING = lookup().findVarHandle(DictWriter.class, "plainWriting", int.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private final FileChannel channel;
    private final ByteBuffer tmp;
    private final long totalBytes, lastOffOff;
    private final int offsetWidth;
    private long nextTableOff, nextUtf8Off;
    @SuppressWarnings("unused") private int plainWriting;

    public DictWriter(FileChannel dest, long nStrings, long utf8Bytes) throws IOException {
        if (nStrings > 0x00ffffffffffffffL)
            throw new IllegalArgumentException("Too many strings");
        this.channel = dest;
        long total = 8 + (nStrings +1)*4 + utf8Bytes;
        if (total < 0xffffffffL) {
            this.offsetWidth = 4;
        } else {
            this.offsetWidth = 8;
            total = 8 + (nStrings +1)*8 + utf8Bytes;
        }
        this.nextTableOff = 8;
        this.lastOffOff = nextTableOff + offsetWidth*nStrings;
        this.nextUtf8Off = lastOffOff + offsetWidth;
        this.totalBytes = total;

        //write N_STRINGS and flags byte
        tmp = ByteBuffer.allocateDirect(8).order(ByteOrder.LITTLE_ENDIAN);
        tmp.putLong(nStrings).put(7, offsetWidth == 4 ? (byte)1 : 0);
        write(0, tmp.flip());

        // write end offset for last rope
        tmp.clear().putLong(total).position(0).limit(offsetWidth);
        write(nextTableOff+ nStrings *offsetWidth, tmp);
    }

    @Override public void close() throws IOException {
        channel.force(true);
        channel.close();
    }

    public void writeSorted(SegmentRope rope) throws IOException {
        checkConcurrency();
        try {
            int len = rope.len;
            checkExpected(1, len);
            writeSorted0(rope, len);
        } finally {
            WRITING.setRelease(this, 0);
        }
    }

    private void writeSorted0(SegmentRope rope, int len) throws IOException {
        tmp.clear().putLong(nextUtf8Off).position(0).limit(offsetWidth);
        write(nextTableOff, tmp);
        nextTableOff += offsetWidth;

        write(nextUtf8Off, rope.asBuffer());
        nextUtf8Off += len;
    }

    public void writeSorted(SegmentRope[] ropes, int nRopes) throws IOException {
        checkConcurrency();
        try {
            long nBytes = 0;
            for (int i = 0; i < nRopes; i++) nBytes += ropes[i].len;
            checkExpected(nRopes, nBytes);
            for (int i = 0; i < nRopes; i++) {
                var r = ropes[i];
                writeSorted0(r, r.len);
            }
        } finally {
            WRITING.setRelease(this, 0);
        }
    }

    /* --- --- --- helper methods --- --- --- */

    private void write(long position, ByteBuffer buf) throws IOException {
        channel.position(position);
        int expected = buf.remaining();
        if (channel.write(buf) != expected)
            throw new IOException("channel refused to write all bytes");
    }

    private void checkExpected(int addStrings, long addBytes) {
        if (nextUtf8Off+addBytes > totalBytes)
            throw new IllegalArgumentException("More UTF-8 bytes than expected for this DictWriter");
        if (nextTableOff+(long)addStrings*offsetWidth > lastOffOff)
            throw new IllegalArgumentException("More strings than expected for this DictWriter");
    }

    private void checkConcurrency() {
        if ((int)WRITING.compareAndExchangeAcquire(this, 0, 1) != 0)
            throw new IllegalStateException("Concurrent writeSorted");
    }
}
