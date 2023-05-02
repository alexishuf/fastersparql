package com.github.alexishuf.fastersparql.store.index;

import com.github.alexishuf.fastersparql.model.rope.SegmentRope;

import java.io.IOException;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static com.github.alexishuf.fastersparql.store.index.Dict.*;
import static java.lang.invoke.MethodHandles.lookup;
import static java.nio.ByteOrder.LITTLE_ENDIAN;

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
    private final long tableEndOff, utf8EndOff;
    private final int offsetWidth;
    private final byte flags;
    private long nextTableOff, nextUtf8Off;
    @SuppressWarnings("unused") private int plainWriting;

    public DictWriter(FileChannel dest, long nStrings, long utf8Bytes,
                      boolean usesShared, boolean sharedIdOverflow) {
        if (nStrings > Dict.STRINGS_MASK)
            throw new IllegalArgumentException("Too many strings");
        this.channel = dest;
        long est = 8 + (nStrings +1)*4 + utf8Bytes;
        if (est < 0xffffffffL) {
            this.offsetWidth = 4;
        } else {
            this.offsetWidth = 8;
            est = 8 + (nStrings+1)*8 + utf8Bytes;
        }
        this.nextTableOff = 8;
        this.utf8EndOff = est;
        this.tableEndOff = nextTableOff + offsetWidth*nStrings;
        this.nextUtf8Off = tableEndOff + offsetWidth;
        this.flags = (byte)(
                     (offsetWidth == 4 ? (byte)(OFF_W_MASK>>FLAGS_BIT)      : (byte)0)
                   | (usesShared       ? (byte)(SHARED_MASK>>FLAGS_BIT)     : (byte)0)
                   | (sharedIdOverflow ? (byte)(SHARED_OVF_MASK>>FLAGS_BIT) : (byte)0));
        this.tmp = SmallBBPool.smallDirectBB().order(LITTLE_ENDIAN);
    }

    @Override public void close() throws IOException {
        //write N_STRINGS
        tmp.clear().putLong((nextTableOff-8)/offsetWidth | (long)flags<< FLAGS_BIT);
        write(0, tmp.flip());

        //write end offset of last string
        tmp.clear().putLong(nextUtf8Off).limit(offsetWidth);
        write(nextTableOff, tmp.flip());

        //flush & close
        SmallBBPool.releaseSmallDirectBB(tmp);
        channel.force(true);
        channel.close();
    }

    public void writeSorted(SegmentRope rope) throws IOException {
        checkConcurrency();
        try {
            writeSorted0(rope);
        } finally {
            WRITING.setRelease(this, 0);
        }
    }

    private void writeSorted0(SegmentRope rope) throws IOException {
        if (nextTableOff >= tableEndOff)
            throw new IllegalStateException("No more entries available in offsets table. Bad nStrings param to constructor?");
        tmp.clear().putLong(nextUtf8Off).position(0).limit(offsetWidth);
        write(nextTableOff, tmp);
        nextTableOff += offsetWidth;

        write(nextUtf8Off, rope.asBuffer());
        nextUtf8Off += rope.len;
        if (offsetWidth == 4 && utf8EndOff > Integer.MAX_VALUE)
            throw new IllegalStateException("Total UTF-8 bytes written overflow the 4-bytes in the offsets table. Review utf8BYtes constructor param");
    }

    /**
     * Bulk write all non-null strings in the {@code [begin, end)} range of {@code ropes}.
     *
     * @param ropes array of {@link SegmentRope}s to be written, may contain nulls in any
     *              position, which will be skipped over until {@code nRopes} ropes are written.
     * @param begin first index of {@code rope} to add if non-null.
     * @param end {@code ropes.length} or first index to not add.
     * @throws IOException If an I/O error occurs during writing
     * @throws IllegalStateException if adding more ropes or bytes than expected at the constructor
     */
    public void writeSorted(SegmentRope[] ropes, int begin, int end) throws IOException {
        checkConcurrency();
        try {
            for (; begin < end; ++begin) {
                var r = ropes[begin];
                if (r != null) writeSorted0(r);
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

    private void checkConcurrency() {
        if ((int)WRITING.compareAndExchangeAcquire(this, 0, 1) != 0)
            throw new IllegalStateException("Concurrent writeSorted");
    }
}
