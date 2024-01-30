package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.model.rope.ByteRope;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import com.github.alexishuf.fastersparql.util.BS;
import com.github.alexishuf.fastersparql.util.LowLevelHelper;
import com.github.alexishuf.fastersparql.util.concurrent.ArrayPool;
import com.github.alexishuf.fastersparql.util.concurrent.LevelPool;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.foreign.MemorySegment;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

import static com.github.alexishuf.fastersparql.batch.type.CompressedBatch.LEN_MASK;
import static com.github.alexishuf.fastersparql.batch.type.CompressedBatch.SH_SUFF_MASK;
import static com.github.alexishuf.fastersparql.batch.type.CompressedBatchType.COMPRESSED;
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

    private byte[][] rowsData;
    private SegmentRope[] shared;
    private int cols, rows;
    private long[] has;

    public CompressedRowBucket(int rowsCapacity, int cols) {
        int level = rowsCapacity == 0 ? 0 : 33 - Integer.numberOfLeadingZeros(rowsCapacity-1);
        if ((this.rowsData = DATA_POOL.getFromLevel(level)) == null)
            this.rowsData = new byte[1<<level][];
        else
            Arrays.fill(this.rowsData, null);
        this.shared = segmentRopesAtLeastUpcycle(rowsData.length*cols);
        this.has = longsAtLeastUpcycle(BS.longsFor(rowsData.length));
        Arrays.fill(has, 0, BS.longsFor(rowsCapacity), 0L);
        this.cols = cols;
        this.rows = rowsCapacity;
    }

    @Override public BatchType<CompressedBatch> batchType()  { return COMPRESSED; }
    @Override public int                        capacity()   { return rows; }
    @Override public int                        cols()       { return cols; }

    @Override public boolean has(int row) {
        if (row >= rows) throw new IndexOutOfBoundsException(row);
        return BS.get(has, row);
    }

    static { assert Integer.bitCount(SL_OFF) == 0; }
    private static short readOff(byte[] d, int c) {
        int i = c<<2;
        return (short)( (d[i]&0xff) | ((d[i+1]&0xff) << 8) );
    }
    static { assert Integer.bitCount(SL_LEN-1) == 0; }
    private static short readLen(byte[] d, int c) {
        int i = (c<<2) + 2;
        return (short)( (d[i]&0xff) | ((d[i+1]&0xff) << 8) );
    }

    @Override public void maximizeCapacity() {
        int max = rowsData.length;
        if (rows < max) {
            BS.clear(has, rows, max);
            rows = max;
        }
    }

    @Override public void grow(int additionalRows) {
        if (additionalRows <= 0)
            return;
        int required = rows+additionalRows;
        if (required > rowsData.length) {
            byte[][] newData = DATA_POOL.getAtLeast(required);
            if (newData == null)
                newData = new byte[required][];
            byte[][] oldData = rowsData;
            arraycopy(oldData, 0, newData, 0, rows);
            rowsData = newData;
            recycleRowsData(oldData);
            has = ArrayPool.grow(has, BS.longsFor(newData.length));
            int terms = required*cols;
            if (terms > shared.length)
                shared = ArrayPool.grow(shared, terms);
        }
        BS.clear(has, rows, required);
        rows = required;
    }

    @Override public void clear(int rowsCapacity, int cols) {
        if (rowsCapacity > rowsData.length) {
            recycleRowsData(rowsData);
            var newData = DATA_POOL.getAtLeast(rowsCapacity);
            if (newData == null)
                newData = new byte[rowsCapacity][];
            rowsData = newData;
            has = longsAtLeast(BS.longsFor(newData.length), has);
        }
        int terms = rowsData.length*cols;
        if (shared.length < terms)
            shared = segmentRopesAtLeastUpcycle(terms);
        this.rows = rowsCapacity;
        this.cols = cols;
        Arrays.fill(has, 0, BS.longsFor(rowsCapacity), 0L);
    }

    @Override public @Nullable CompressedRowBucket recycleInternals() {
        recycleRowsData(rowsData);
        rowsData = EMPTY_ROWS_DATA;
        rows = 0;
        SEG_ROPE.offerToNearest(shared, shared.length);
        shared = EMPTY_SEG_ROPE;
        LONG.offerToNearest(has, has.length);
        has = EMPTY_LONG;
        cols = 0;
        return null;
    }

    private static void recycleRowsData(byte[][] rowsData) {
        if (DATA_POOL.offer(rowsData, rowsData.length) != null) {
            for(int i = 0; i < rowsData.length; ++i) {
                byte[] row = rowsData[i];
                rowsData[i] = null;
                if (row != null && BYTE.offer(row, row.length) != null)
                    break; // stop recycling once pool rejects
            }
        }
    }

    private class It implements Iterator<CompressedBatch> {
        private @Nullable CompressedBatch batch;
        private boolean filled = false;
        private int row = 0;

        public It() {
            batch = COMPRESSED.create(cols);
        }

        @Override public boolean hasNext() {
            boolean has = batch != null;
            if (!filled && has) {
                filled = true;
                row = fill(batch, row);
                has = batch.rows != 0;
                if (!has)
                    batch = COMPRESSED.recycle(batch);
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

    static { //noinspection ConstantValue
        assert SL_OFF == 0 && SL_LEN == 1: "update \"check if row fits in b\"";
    }
    private int fill(CompressedBatch b, int row) {
        b.clear();
        // first pass counts required locals capacity
        short localsBase = (short)(cols<<2), lastBase = (short)(localsBase-4);
        short nr = 0, rCap = (short)(b.termsCapacity()/cols);
        short localsRequired = 0;
        int startRow = row;
        for (boolean missing = true; nr <= rCap; ++nr) {
            // find next non-empty row
            while (row < rows && (missing = BS.get(has, row))) ++row;
            byte[] rd = missing ? null : rowsData[row];
            if (rd == null) break;

            // check if row fits in b
            int nReq =  ((rd[lastBase  ]&0xff) | ((rd[lastBase+1]&0xff) << 8))
                     + (((rd[lastBase+2]&0xff) | ((rd[lastBase+3]&0xff) << 8))&LEN_MASK)
                     - localsBase + localsRequired;
            if (nReq > Short.MAX_VALUE) break;
            localsRequired = (short)nReq;
            ++row;
        }
        b.reserveAddLocals(localsRequired);

        // second pass copies rows
        SegmentRope[] shared = this.shared;
        row = startRow;
        rCap = nr;
        nr = 0;
        for (boolean missing = true; nr < rCap; ++nr, ++row) {
            // find next non-empty row
            while (row < rows && (missing = BS.get(has, row))) ++row;
            byte[] rd = missing ? null : rowsData[row];
            if (rd == null)
                break; // can happen if bucket was concurrently modified
            // copy row
            b.copyFromBucket(rd, shared, row);
        }
        return row; // return value to be given as row in the next call to fill()
    }

    @Override public void set(int dst, CompressedBatch batch, int row) {
        rowsData[dst] = batch.copyToBucket(rowsData[dst], shared, dst, row);
        BS.set(has, dst);
    }

    @Override public void set(int dst, int src) {
        if (dst == src)
            return;
        byte[] s, d = rowsData[dst];
        if (BS.get(has, src)) {
            BS.set(has, dst);
            s = rowsData[src];
            rowsData[dst] = d = bytesAtLeast(s.length, d);
            arraycopy(s, 0, d, 0, s.length);
            arraycopy(shared, src * cols, shared, dst * cols, cols);
        } else {
            BS.clear(has, dst);
            rowsData[dst] = BYTE.offerToNearest(d, d.length);
        }
    }

    @Override public void set(int dst, RowBucket<CompressedBatch> other, int src) {
        var bucket = (CompressedRowBucket) other;
        int cols = this.cols;
        if (bucket.cols < cols)
            throw new IllegalArgumentException("cols mismatch");
        byte[] s = bucket.rowsData[src], d = rowsData[dst];
        if (BS.get(bucket.has, src)) {
            BS.set(has, dst);
            rowsData[dst] = d = bytesAtLeast(s.length, d);
            arraycopy(s, 0, d, 0, s.length);
            arraycopy(bucket.shared, src*bucket.cols, shared, dst*cols, cols);
        } else if (d != null) {
            BS.clear(has, dst);
            rowsData[dst] = BYTE.offer(d, d.length);
        }
    }

    @Override public void putRow(CompressedBatch dst, int srcRow) {
        if (srcRow >= rows)
            throw new IndexOutOfBoundsException(srcRow);
        else if (BS.get(has, srcRow))
            dst.copyFromBucket(rowsData[srcRow], shared, srcRow);
    }

    @Override public boolean equals(int row, CompressedBatch other, int otherRow) {
        int cols = this.cols, shIdx = row*cols;
        if (cols != other.cols) throw new IllegalArgumentException("cols mismatch");
        byte[] data = rowsData[row];
        if (data == null || !BS.get(has, row)) return false;
        boolean eq = true;
        var tmp     = Term.pooledMutable();
        var dataSeg = MemorySegment.ofArray(data);
        for (int c = 0; c < cols; c++) {
            SegmentRope sh = shared[shIdx++];
            if (sh == null) sh = ByteRope.EMPTY;
            int len = readLen(data, c);
            boolean suffixed = (len & SH_SUFF_MASK) != 0;
            len &= LEN_MASK;
            if (sh.len+len == 0) {
                if (other.termType(otherRow, c) != null) {
                    eq = false;
                    break;
                }
            } else {
                tmp.set(sh, dataSeg, data, readOff(data, c), len, suffixed);
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
        if (!BS.get(has, row))
            return 0;
        if (!LowLevelHelper.HAS_UNSAFE)
            return safeHashCode(row);
        int h = 0;
        var shared = this.shared;
        byte[] d = rowsData[row];
        MemorySegment dSeg = null;
        Term tmp = null;
        for (int c = 0, cols = this.cols, shIdx = cols*row; c < cols; c++, shIdx++) {
            SegmentRope sh = shared[shIdx];
            int flLen = readLen(d, c), fstLen, sndLen = flLen&LEN_MASK;
            long fstOff, sndOff = readOff(d, c);
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
            fstOff = sh.segment.address()+sh.offset;
            byte[] fst = sh.utf8, snd = d;
            if ((flLen & SH_SUFF_MASK) != 0) {
                fst = d;       fstOff = sndOff;    fstLen = sndLen;
                snd = sh.utf8; sndOff = sh.offset; sndLen = sh.len;
            }
            int termHash = SegmentRope.hashCode(FNV_BASIS, fst, fstOff, fstLen);
            h ^=           SegmentRope.hashCode(termHash,  snd, sndOff, sndLen);
        }
        return h;
    }

    private int safeHashCode(int row) {
        int h = 0;
        var shared = this.shared;
        byte[] d = rowsData[row];
        var dSeg = MemorySegment.ofArray(d);
        Term tmp = null;
        for (int c = 0, cols = this.cols, shIdx = cols*row; c < cols; c++, shIdx++) {
            SegmentRope sh = shared[shIdx];
            int flLen = readLen(d, c), fstLen, sndLen = flLen&LEN_MASK;
            long fstOff, sndOff = readOff(d, c);
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
        if (d == null || !BS.get(has, row)) {
            dest.append(DUMP_NULL);
            return;
        }
        dest.append('[');
        for (int c = 0, shBase=row*cols; c < cols; c++) {
            int flagLen = readLen(d, c);
            int off = readOff(d, c);
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
