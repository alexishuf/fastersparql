package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import com.github.alexishuf.fastersparql.util.concurrent.ArrayPool;
import com.github.alexishuf.fastersparql.util.concurrent.LevelPool;

import java.lang.foreign.MemorySegment;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

import static com.github.alexishuf.fastersparql.batch.type.Batch.COMPRESSED;
import static com.github.alexishuf.fastersparql.batch.type.CompressedBatch.LEN_MASK;
import static com.github.alexishuf.fastersparql.batch.type.CompressedBatch.SH_SUFF_MASK;
import static com.github.alexishuf.fastersparql.util.concurrent.ArrayPool.*;
import static java.lang.System.arraycopy;

public class CompressedRowBucket implements RowBucket<CompressedBatch> {
    private static final int SL_OFF = 0;
    private static final int SL_LEN = 1;
    private static final byte[][] EMPTY_ROWS_DATA = new byte[0][];
    private static final LevelPool<byte[][]> DATA_POOL = new LevelPool<>(byte[][].class, 1024, 16, 1, 1);

    private int cols;
    private byte[][] rowsData;
    private SegmentRope[] shared;

    public CompressedRowBucket(int rowsCapacity, int cols) {
        if ((this.rowsData = DATA_POOL.getAtLeast(rowsCapacity)) == null)
            this.rowsData = new byte[rowsCapacity][];
        this.shared = segmentRopesAtLeast(rowsCapacity*cols);
        this.cols = cols;
    }

    @Override public BatchType<CompressedBatch> batchType()  { return COMPRESSED; }
    @Override public int                        capacity()   { return rowsData.length; }
    @Override public int                        cols()       { return cols; }
    @Override public boolean                    has(int row) { return rowsData[row] != null; }

    private static int read(byte[] d, int i) {
        int i4 = i<<2;
        return    (d[i4  ] & 0xff)
                | (d[i4+1] & 0xff) << 8
                | (d[i4+2] & 0xff) << 16
                | (d[i4+3] & 0xff) << 24;
    }

    private static void write(byte[] d, int i, int val) {
        int i4 = i<<2;
        d[i4  ] = (byte)(val       );
        d[i4+1] = (byte)(val >>>  8);
        d[i4+2] = (byte)(val >>> 16);
        d[i4+3] = (byte)(val >>> 24);
    }

    private static void clearRowsData(byte[][] d) {
        int i = 0;
        for(; i < d.length; ++i) {
            byte[] row = d[i];
            d[i] = null;
            if (row != null && BYTE.offer(row, row.length) != null) break;
        }
        Arrays.fill(d, i, d.length, null);
    }

    @Override public void grow(int additionalRows) {
        if (additionalRows <= 0) return;
        byte[][] oldData = rowsData;
        int required = oldData.length + additionalRows;
        byte[][] newData = DATA_POOL.getAtLeast(required);
        if (newData == null)
            newData = new byte[required][];
        arraycopy(oldData, 0, newData, 0, oldData.length);
        rowsData = newData;
        shared = ArrayPool.grow(shared, required*cols);
        clearRowsData(oldData);
        DATA_POOL.offer(oldData, oldData.length);
    }

    @Override public void clear(int rowsCapacity, int cols) {
        clearRowsData(rowsData);
        if (rowsData.length < rowsCapacity) {
            DATA_POOL.offer(rowsData, rowsData.length);
            rowsData = DATA_POOL.getAtLeast(rowsCapacity);
        }
        int required = rowsCapacity * cols;
        if (shared.length < required)
            shared = segmentRopesAtLeast(required, shared);
        Arrays.fill(shared, 0, required, null);
        this.cols = cols;
    }

    @Override public void recycleInternals() {
        clearRowsData(rowsData);
        rowsData = EMPTY_ROWS_DATA;
        SEG_ROPE.offer(shared, shared.length);
        shared = EMPTY_SEG_ROPE;
        cols = 0;
    }

    @Override public Iterator<CompressedBatch> iterator() {
        return new Iterator<>() {
            final CompressedBatch batch = new CompressedBatch(64, cols, 512);
            boolean filled = false;
            int row = 0;

            @Override public boolean hasNext() {
                if (!filled) {
                    filled = true;
                    row = fill(batch, row);
                    if (batch.rows == 0)
                        batch.recycleInternals();
                }
                return batch.rows != 0;
            }

            @Override public CompressedBatch next() {
                if (!hasNext()) throw new NoSuchElementException();
                filled = false;
                return batch;
            }
        };
    }

    private int fill(CompressedBatch b, int row) {
        int cols = this.cols;
        b.clear(cols);
        while (row < rowsData.length && !has(row)) ++row;
        while (b.bytesCapacity() >= 32 && row < rowsData.length) {
            // add row
            byte[] d = rowsData[row];
            b.beginPut();
            for (int c = 0, c2, shIdx = row*cols; c < cols; c++) {
                int len = read(d, (c2=c<<1)+SL_LEN);
                boolean suffix = (len&SH_SUFF_MASK) != 0;
                len &= LEN_MASK;
                SegmentRope sh = shared[shIdx++];
                if (sh != null || len != 0)
                    b.putTerm(c, sh, d, read(d, c2+SL_OFF), len, suffix);
            }
            b.commitPut();
            // find next non-empty row
            do { ++row; } while (row < rowsData.length && !has(row));
        }
        return row; // return possible next non-empty row
    }

    @Override public void set(int dst, CompressedBatch batch, int row) {
        byte[] d = rowsData[dst];
        int out = cols * 8, shOut = dst * cols;
        d = bytesAtLeast(batch.bytesUsed(row) + out, d);
        for (int c = 0, c2, cols = this.cols; c < cols; c++) {
            shared[shOut++] = batch.sharedUnchecked(row, c);
            write(d, (c2=c<<1)+SL_OFF, out);
            write(d, c2       +SL_LEN, batch.flaggedLen(row, c));
            out += batch.copyLocal(row, c, d, out);
        }
        rowsData[dst] = d;
    }

    @Override public void set(int dst, int src) {
        byte[] s = rowsData[src], d = rowsData[dst];
        if (s == null) {
            if (d != null)
                BYTE.offer(d, d.length);
            rowsData[dst] = null;
        } else {
            d = bytesAtLeast(s.length, d);
            arraycopy(s, 0, d, 0, s.length);
            arraycopy(shared, src * cols, shared, dst * cols, cols);
            rowsData[dst] = d;
        }
    }

    @Override public void set(int dst, RowBucket<CompressedBatch> other, int src) {
        var bucket = (CompressedRowBucket) other;
        int cols = this.cols;
        if (bucket.cols < cols) throw new IllegalArgumentException("cols mismatch");
        byte[] s = bucket.rowsData[src], d = rowsData[dst];
        if (s == null) {
            if (d != null)
                BYTE.offer(d, d.length);
            rowsData[dst] = null;
        } else {
            d = bytesAtLeast(s.length, d);
            arraycopy(s, 0, d, 0, s.length);
            arraycopy(bucket.shared, src*bucket.cols, shared, dst* cols, cols);
        }
    }

    @Override public boolean equals(int row, CompressedBatch other, int otherRow) {
        int cols = this.cols, shIdx = row*cols;
        if (cols != other.cols) throw new IllegalArgumentException("cols mismatch");
        byte[] d = rowsData[row];
        if (d == null) return false;
        boolean eq = true, suffixed;
        Term tmp = Term.pooledMutable();
        SegmentRope local = SegmentRope.pooledWrap(MemorySegment.ofArray(d), d, 0, 0);
        for (int c = 0, c2; c < cols; c++) {
            SegmentRope sh = shared[shIdx++];
            int len = read(d, (c2=c<<1)+SL_LEN);
            suffixed = (len & SH_SUFF_MASK) != 0;
            len &= LEN_MASK;
            if (sh.len == 0 && len == 0)
                continue;
            local.slice(read(d, c2+SL_OFF), len);
            tmp.set(sh, local, suffixed);
            if (!other.equals(otherRow, c, tmp)) {
                eq = false;
                break;
            }
        }
        tmp.recycle();
        local.recycle();
        return eq;
    }


}
