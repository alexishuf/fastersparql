package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.model.rope.ByteRope;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import com.github.alexishuf.fastersparql.util.LowLevelHelper;
import com.github.alexishuf.fastersparql.util.concurrent.ArrayPool;
import com.github.alexishuf.fastersparql.util.concurrent.LevelPool;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.lang.foreign.MemorySegment;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

import static com.github.alexishuf.fastersparql.batch.type.Batch.COMPRESSED;
import static com.github.alexishuf.fastersparql.batch.type.CompressedBatch.LEN_MASK;
import static com.github.alexishuf.fastersparql.batch.type.CompressedBatch.SH_SUFF_MASK;
import static com.github.alexishuf.fastersparql.model.rope.Rope.FNV_BASIS;
import static com.github.alexishuf.fastersparql.util.concurrent.ArrayPool.*;
import static java.lang.System.arraycopy;
import static java.nio.charset.StandardCharsets.UTF_8;

public class CompressedRowBucket implements RowBucket<CompressedBatch> {
    private static final int SL_OFF = 0;
    private static final int SL_LEN = 1;
    private static final byte[][] EMPTY_ROWS_DATA = new byte[0][];
    private static final LevelPool<byte[][]> DATA_POOL = new LevelPool<>(byte[][].class,
            16, 1024, 16, 1, 1);

    private int cols;
    private byte[][] rowsData;
    private SegmentRope[] shared;

    public CompressedRowBucket(int rowsCapacity, int cols) {
        if ((this.rowsData = DATA_POOL.getAtLeast(rowsCapacity)) == null)
            this.rowsData = new byte[rowsCapacity][];
        else
            Arrays.fill(this.rowsData, null);
        this.shared = segmentRopesAtLeast(rowsData.length*cols);
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
        else
            Arrays.fill(newData, oldData.length, newData.length, null);
        arraycopy(oldData, 0, newData, 0, oldData.length);
        rowsData = newData;
        DATA_POOL.offer(oldData, oldData.length);
        required = rowsData.length * cols;
        if (shared.length < required)
            shared = ArrayPool.grow(shared, required);
    }

    @Override public void clear(int rowsCapacity, int cols) {
        clearRowsData(rowsData);
        if (rowsData.length < rowsCapacity) {
            DATA_POOL.offer(rowsData, rowsData.length);
            rowsData = DATA_POOL.getAtLeast(rowsCapacity);
            if (rowsData == null)
                rowsData = new byte[rowsCapacity][];
            else
                Arrays.fill(rowsData, null);
        }
        int required = rowsData.length * cols;
        if (shared.length < required)
            shared = segmentRopesAtLeast(required, shared);
        Arrays.fill(shared, 0, required, null);
        this.cols = cols;
    }

    @Override public void recycleInternals() {
        clearRowsData(rowsData);
        DATA_POOL.offer(rowsData, rowsData.length);
        rowsData = EMPTY_ROWS_DATA;
        SEG_ROPE.offer(shared, shared.length);
        shared = EMPTY_SEG_ROPE;
        cols = 0;
    }

    private class It implements Iterator<CompressedBatch> {
        private CompressedBatch batch;
        private boolean filled = false;
        private int row = 0;

        public It() {
            int rows = Math.min(64, rowsData.length);
            batch = CompressedBatchType.INSTANCE.create(rows, cols, rows*32);
        }

        @Override public boolean hasNext() {
            boolean has = batch != null;
            if (!filled && has) {
                filled = true;
                row = fill(batch, row);
                has = batch.rows != 0;
                if (!has)
                    batch = CompressedBatchType.INSTANCE.recycle(batch);
            }
            return has;
        }

        @Override public CompressedBatch next() {
            if (!hasNext()) throw new NoSuchElementException();
            filled = false;
            return batch;
        }
    }

    @Override public @NonNull Iterator<CompressedBatch> iterator() {
        return new It();
    }

    private int fill(CompressedBatch b, int row) {
        int cols = this.cols;
        b.clear(cols);
        while (row < rowsData.length && !has(row)) ++row;
        while (b.localsFreeCapacity() >= 32 && row < rowsData.length) {
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
        rowsData[dst] = batch.copyToBucket(rowsData[dst], shared, dst, row);
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
        byte[] data = rowsData[row];
        if (data == null) return false;
        boolean eq = true;
        var tmp     = Term.pooledMutable();
        var dataSeg = MemorySegment.ofArray(data);
        for (int c = 0, c2; c < cols; c++) {
            SegmentRope sh = shared[shIdx++];
            if (sh == null) sh = ByteRope.EMPTY;
            int len = read(data, (c2=c<<1)+SL_LEN);
            boolean suffixed = (len & SH_SUFF_MASK) != 0;
            len &= LEN_MASK;
            if (sh.len+len == 0) {
                if (other.termType(otherRow, c) != null) {
                    eq = false;
                    break;
                }
            } else {
                tmp.set(sh, dataSeg, data, read(data, c2+SL_OFF), len, suffixed);
                if (!other.equals(otherRow, c, tmp)) {
                    eq = false;
                    break;
                }
            }
        }
        tmp.recycle();
        return eq;
    }

    @Override public int hashCode(int row) {
        if (!LowLevelHelper.HAS_UNSAFE)
            return safeHashCode(row);
        int h = 0;
        var shared = this.shared;
        byte[] d = rowsData[row];
        MemorySegment dSeg = null;
        Term tmp = null;
        for (int c = 0, c2 = 0, cols = this.cols, si = cols*row; c < cols; c++, c2=c<<1, si++) {
            SegmentRope sh = shared[si];
            int flLen = read(d, c2+SL_LEN), fstLen, sndLen = flLen&LEN_MASK;
            long fstOff, sndOff = read(d, c2+SL_OFF);
            if (sh == null) {
                sh = ByteRope.EMPTY;
            } else if (Term.isNumericDatatype(sh)) {
                if (tmp == null) {
                    tmp = Term.pooledMutable();
                    dSeg = MemorySegment.ofArray(d);
                }
                tmp.set(sh, dSeg, d, sndOff, sndLen, true);
                h ^= tmp.hashCode();
                continue;
            }
            fstLen = sh.len;
            fstOff = sh.offset;
            byte[] fst = sh.utf8, snd = d;
            if ((flLen & SH_SUFF_MASK) != 0) {
                fst = d;       fstOff = sndOff;    fstLen = sndLen;
                snd = sh.utf8; sndOff = sh.offset; sndLen = sh.len;
            }
            int termHash = SegmentRope.hashCode(FNV_BASIS, fst, fstOff, fstLen);
            h ^=           SegmentRope.hashCode(termHash, snd, sndOff, sndLen);
        }
        return h;
    }

    private int safeHashCode(int row) {
        int h = 0;
        var shared = this.shared;
        byte[] d = rowsData[row];
        var dSeg = MemorySegment.ofArray(d);
        Term tmp = null;
        for (int c = 0, c2 = 0, cols = this.cols, si = cols*row; c < cols; c++, c2=c<<1, si++) {
            SegmentRope sh = shared[si];
            int flLen = read(d, c2+SL_LEN), fstLen, sndLen = flLen&LEN_MASK;
            long fstOff, sndOff = read(d, c2+SL_OFF);
            if (sh == null) {
                sh = ByteRope.EMPTY;
            } else if (Term.isNumericDatatype(sh)) {
                if (tmp == null)
                    tmp = Term.pooledMutable();
                tmp.set(sh, dSeg, d, sndOff, sndLen, true);
                h ^= tmp.hashCode();
                continue;
            }
            fstLen = sh.len;
            fstOff = sh.offset;
            MemorySegment fst = sh.segment, snd = dSeg;
            if ((flLen & SH_SUFF_MASK) != 0) {
                fst = dSeg;       fstOff = sndOff;    fstLen = sndLen;
                snd = sh.segment; sndOff = sh.offset; sndLen = sh.len;
            }
            int termHash = SegmentRope.hashCode(FNV_BASIS, fst, fstOff, fstLen);
            h ^=           SegmentRope.hashCode(termHash,  snd, sndOff, sndLen);
        }
        return h;
    }

    private static final byte[] DUMP_NULL = "null".getBytes(UTF_8);
    @Override public void dump(ByteRope dest, int row) {
        byte[] d = rowsData[row];
        if (d == null) {
            dest.append(DUMP_NULL);
            return;
        }
        dest.append('[');
        for (int c = 0, shBase=row*cols, c2; c < cols; c++) {
            int flagLen = read(d, (c2=c<<1)+SL_LEN);
            int off = read(d, c2+SL_OFF);
            SegmentRope sh = shared[shBase + c];
            if ((flagLen&SH_SUFF_MASK) == 0) {
                if (sh == null) {
                    dest.append(DUMP_NULL);
                } else {
                    dest.append(sh);
                    dest.append(d, off, flagLen & LEN_MASK);
                }
            } else {
                dest.append(d, off, flagLen & LEN_MASK);
                dest.append(sh);
            }
            dest.append(',').append(' ');
        }
        if (cols > 0) dest.len -=2 ;
        dest.append(']');
    }
}
