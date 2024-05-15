package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.FS;
import com.github.alexishuf.fastersparql.model.rope.FinalSegmentRope;
import com.github.alexishuf.fastersparql.model.rope.MutableRope;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import com.github.alexishuf.fastersparql.sparql.expr.PooledTermView;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import com.github.alexishuf.fastersparql.util.BS;
import com.github.alexishuf.fastersparql.util.LowLevelHelper;
import com.github.alexishuf.fastersparql.util.concurrent.*;
import com.github.alexishuf.fastersparql.util.owned.AbstractOwned;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import com.github.alexishuf.fastersparql.util.owned.SpecialOwner;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;

import java.lang.foreign.MemorySegment;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.IntFunction;

import static com.github.alexishuf.fastersparql.batch.type.BatchType.PREFERRED_BATCH_TERMS;
import static com.github.alexishuf.fastersparql.batch.type.CompressedBatch.LEN_MASK;
import static com.github.alexishuf.fastersparql.batch.type.CompressedBatch.SH_SUFF_MASK;
import static com.github.alexishuf.fastersparql.batch.type.CompressedBatchType.COMPRESSED;
import static com.github.alexishuf.fastersparql.model.rope.Rope.FNV_BASIS;
import static com.github.alexishuf.fastersparql.util.concurrent.ArrayAlloc.*;
import static java.lang.System.arraycopy;
import static java.nio.charset.StandardCharsets.UTF_8;

public abstract sealed class CompressedRowBucket
        extends AbstractOwned<CompressedRowBucket>
        implements RowBucket<CompressedBatch, CompressedRowBucket> {
    private static final int SL_OFF = 0;
    private static final int SL_LEN = 1;
    private static final Bytes[] EMPTY_DATA = new Bytes[0];
    private static final RowsDataFac DATA_FAC = new RowsDataFac();
    private static final RowsDataRecycler DATA_RECYCLER = new RowsDataRecycler();
    private static final LevelAlloc<Bytes[]> DATA_POOL;

    static {
        int batchLevel = len2level(PREFERRED_BATCH_TERMS);
        int maxLevel = len2level(Short.MAX_VALUE);
        DATA_POOL = new LevelAlloc<>(
                Bytes[].class, "CompressedRowBucket.DATA_POOL",
                20, 4, DATA_RECYCLER.clearElseMake,
                new Capacities()
                        .set(0, maxLevel, Alloc.THREADS*8)
                        .set(0, 3, Alloc.THREADS*32 )
                        .set(4, 4, Alloc.THREADS*128)
                        .set(5, 6, Alloc.THREADS*64 )
                        .set(batchLevel-3, batchLevel-3, Alloc.THREADS*64)
                        .set(batchLevel-2, batchLevel-1, Alloc.THREADS*128)
                        .set(batchLevel, batchLevel, Alloc.THREADS*64)
                        .set(0, len2level(PREFERRED_BATCH_TERMS), Alloc.THREADS*32)
        );
        DATA_POOL.setZero(DATA_FAC.apply(0));
        DATA_RECYCLER.pool = DATA_POOL;
//        Primer.INSTANCE.sched(() -> {
//            // StrongDedup usage:
//            for (int level = 4; level <= 5; level++)
//                DATA_POOL.primeLevelLocalAndShared(DATA_FAC, level);
//            DATA_POOL.primeLevelLocal(DATA_FAC, 6);
//            // WeakDedup, DistinctType.WEAK
//            for (int level = batchLevel-3; level < batchLevel; level++)
//                DATA_POOL.primeLevelLocalAndShared(DATA_FAC, level);
//            // WeakDedup, DistinctType > WEAK
//            for (int level = maxLevel-3; level <= maxLevel; level++)
//                DATA_POOL.primeLevelLocalAndShared(DATA_FAC, level);
//        });
    }

    private static final class RowsDataFac implements IntFunction<Bytes[]> {
        @Override public Bytes[] apply(int len) {return len == 0 ? EMPTY_DATA : new Bytes[len];}
        @Override public String toString() {return "RowsDataFac";}
    }

    private static final class RowsDataRecycler extends LevelCleanerBackgroundTask<Bytes[]> {
        private RowsDataRecycler() {
            super("CompressedRowsDataRecycler", DATA_FAC);
            FS.addShutdownHook(this::dumpStats);
        }

        public void dumpStats() {
            sync();
            double nArrays = Math.max(1, objsPooled+fullPoolGarbage);
            System.err.printf("""
                    CompressedRowsData Bytes[]s   pooled: %,9d (%5.3f%%)
                    CompressedRowsData Bytes[]s no space: %,9d (%5.3f%%)
                    """,
                    objsPooled,      100.0 *      objsPooled/nArrays,
                    fullPoolGarbage, 100.0 * fullPoolGarbage/nArrays);
        }


        @Override protected void clear(Bytes[] rowsData) {
            for (int i = 0; i < rowsData.length; i++) {
                var rd = rowsData[i];
                if (rd != null)
                    rowsData[i] = rd.recycle(rowsData);
            }
        }
    }

    private Bytes[] rowsData;
    private FinalSegmentRope[] shared;
    private int cols, rows;
    private long[] has;
    private LIFOPool<RowBucket<CompressedBatch, ?>> pool;

    static int estimateBytes(int rows, int cols) {
        return 16+6*4
                + 20*BS.longsFor(rows)*8 /*long[] has */
                + 20+rows*cols*4         /*shared[]*/
                + 20+rows*4              /*byte[][] rowsData*/
                + rows*(20+32);          /*byte[]s in byte[][]*/
    }

    public CompressedRowBucket(int rowsCapacity, int cols) {
        int level = LevelAlloc.len2level(rowsCapacity);
        this.rowsData = DATA_POOL.createFromLevel(level);
        this.shared = finalSegmentRopesAtLeast(rowsData.length*cols);
        int hasWords = BS.longsFor(rowsData.length);
        long[] has = longsAtLeast(hasWords);
        if (has == null)
            has = new long[hasWords];
        else
            Arrays.fill(has, 0, hasWords, 0L);
        this.has  = has;
        this.cols = cols;
        this.rows = rowsCapacity;
    }

    @Override public @Nullable CompressedRowBucket recycle(Object currentOwner) {
        if (pool != null) {
            internalMarkRecycled(currentOwner);
            if (pool.offer(this) == null)
                return null;
            currentOwner = SpecialOwner.RECYCLED;
        }
        internalMarkGarbage(currentOwner);
        DATA_RECYCLER.sched(rowsData, rowsData.length);
        rowsData = EMPTY_DATA;
        shared   = recycleSegmentRopesAndGetEmpty(shared);
        has      = recycleLongsAndGetEmpty(has);
        rows     = 0;
        cols     = 0;
        return null;
    }

    final static class Concrete extends CompressedRowBucket implements Orphan<CompressedRowBucket> {
        public Concrete(int rowsCapacity, int cols) {super(rowsCapacity, cols);}
        @Override public CompressedRowBucket takeOwnership(Object o) {return takeOwnership0(o);}
    }

    @Override public @This CompressedRowBucket
    setPool(LIFOPool<RowBucket<CompressedBatch, ?>> pool) {
        this.pool = pool;
        return this;
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
            int requiredLevel = len2level(rows + additionalRows);
            Bytes[] newData = DATA_POOL.createFromLevel(requiredLevel);
            Bytes[] oldData = rowsData;
            // DATA_RECYCLER recycles anything in oldData, move all non-garbage rows to newData
            for (int i = 0; (i=BS.nextSetOrLen(has, i)) < oldData.length; ++i) {
                newData[i] = oldData[i].transferOwnership(oldData, newData);
                oldData[i] = null;
            }
            rowsData = newData;
            DATA_RECYCLER.sched(oldData, oldData.length);
            has = ArrayAlloc.grow(has, BS.longsFor(newData.length));
            int terms = newData.length*cols;
            if (terms > shared.length)
                shared = ArrayAlloc.grow(shared, terms);
        }
        BS.clear(has, rows, required);
        rows = required;
    }

    @Override public void clear(int rowsCapacity, int cols) {
        if (rowsCapacity > rowsData.length) {
            int level = len2level(rowsCapacity);
            var newData = DATA_POOL.createFromLevel(level);
            DATA_RECYCLER.sched(rowsData, rowsData.length);
            rowsData = newData;
            has = longsAtLeast(BS.longsFor(newData.length), has);
        }
        int terms = rowsData.length*cols;
        if (shared.length < terms)
            shared = finalSegmentRopesAtLeast(terms, shared);
        this.rows = rowsCapacity;
        this.cols = cols;
        Arrays.fill(has, 0, BS.longsFor(rowsCapacity), 0L);
    }

    public boolean isEmpty() {
        for (int i = 0, n = BS.longsFor(rows); i < n; i++) {
            if (has[i] != 0)
                return false;
        }
        return true;
    }


    private class It implements Iterator<CompressedBatch>, AutoCloseable {
        private @Nullable CompressedBatch batch = COMPRESSED.create(cols).takeOwnership(this);
        private boolean filled = false;
        private int row = 0;

        @Override public void close() {
            if (batch != null) batch = batch.recycle(this);
        }

        @Override public boolean hasNext() {
            boolean has = batch != null;
            if (!filled && has) {
                filled = true;
                row = fill(batch, row);
                has = batch.rows != 0;
                if (!has)
                    close();
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
        requireAlive();
        b.clear();
        // first pass counts required locals capacity
        long [] has      = this.has;
        Bytes[] rowsData = this.rowsData;
        for (int rows = this.rows; (row=BS.nextSetOrLen(has, row)) < rows; ++row) {
            if (!b.copyFromBucketIfFits(rowsData[row].requireOwner(rowsData).arr, shared, row))
                break; // b has no more space
        }
        // if first row already blew the budget, enlarge b to fit
        if (b.rows == 0 && row < rows) {
            b.copyFromBucket(rowsData[row].requireOwner(rowsData).arr, shared, row);
            ++row;
        }
        return row; // return value to be given as row in the next call to fill()
    }

    @Override public void set(int dst, CompressedBatch batch, int row) {
        rowsData[dst] = batch.copyToBucket(rowsData[dst], rowsData, shared, dst, row);
        BS.set(has, dst);
    }

    @Override public void set(int dst, int src) {
        if (dst == src)
            return;
        Bytes s, d = rowsData[dst];
        if (BS.get(has, src)) {
            BS.set(has, dst);
            s = rowsData[src];
            rowsData[dst] = Bytes.copy(s.arr, 0, d, rowsData, s.arr.length);
            arraycopy(shared, src * cols, shared, dst * cols, cols);
        } else {
            BS.clear(has, dst);
            rowsData[dst] = d.recycle(rowsData);
        }
    }

    @Override public void set(int dst, RowBucket<CompressedBatch, ?> other, int src) {
        var bucket = (CompressedRowBucket)other;
        int cols = this.cols;
        if (bucket.cols < cols)
            throw new IllegalArgumentException("cols mismatch");
        Bytes s = bucket.rowsData[src], d = rowsData[dst];
        if (BS.get(bucket.has, src)) {
            BS.set(has, dst);
            rowsData[dst] = Bytes.copy(s.arr, 0, d, rowsData, s.arr.length);
            arraycopy(bucket.shared, src*bucket.cols, shared, dst*cols, cols);
        } else if (d != null) {
            BS.clear(has, dst);
            rowsData[dst] = d.recycle(rowsData);
        }
    }

    @Override public void putRow(CompressedBatch dst, int srcRow) {
        if (srcRow >= rows)
            throw new IndexOutOfBoundsException(srcRow);
        else if (BS.get(has, srcRow))
            dst.copyFromBucket(rowsData[srcRow].requireOwner(rowsData).arr, shared, srcRow);
    }

    @Override public boolean equals(int row, CompressedBatch other, int otherRow) {
        int cols = this.cols, shIdx = row*cols;
        if (cols != other.cols) throw new IllegalArgumentException("cols mismatch");
        var data = rowsData[row];
        if (data == null || !BS.get(has, row)) return false;
        boolean eq = true;
        try (var tmp = PooledTermView.ofEmptyString()) {
            for (int c = 0; c < cols; c++) {
                SegmentRope sh = shared[shIdx++];
                if (sh == null) sh = FinalSegmentRope.EMPTY;
                int len = readLen(data.arr, c);
                boolean suffixed = (len & SH_SUFF_MASK) != 0;
                len &= LEN_MASK;
                if (sh.len + len == 0) {
                    if (other.termType(otherRow, c) != null) {
                        eq = false;
                        break;
                    }
                } else {
                    tmp.wrap(sh, data.segment, data.arr, readOff(data.arr, c), len, suffixed);
                    if (!other.equals(otherRow, c, tmp)) {
                        eq = false;
                        break;
                    }
                }
            }
        }
        return eq;
    }

    @Override public int hashCode(int row) {
        if (!BS.get(has, row))
            return 0;
        if (LowLevelHelper.U == null)
            return safeHashCode(row);
        int h = 0;
        var shared = this.shared;
        var d = rowsData[row];
        PooledTermView tmp = null;
        try {
            for (int c = 0, cols = this.cols, shIdx = cols*row; c < cols; c++, shIdx++) {
                SegmentRope sh = shared[shIdx];
                int flLen = readLen(d.arr, c), fstLen, sndLen = flLen&LEN_MASK;
                long fstOff, sndOff = readOff(d.arr, c);
                if (sh == null) {
                    sh = FinalSegmentRope.EMPTY;
                } else if (Term.isNumericDatatype(sh)) {
                    if (tmp == null)
                        tmp = PooledTermView.ofEmptyString();
                    tmp.wrap(sh, d.segment, d.arr, sndOff, sndLen, true);
                    h ^= tmp.hashCode();
                    continue;
                }
                fstLen = sh.len;
                fstOff = sh.segment.address()+sh.offset;
                byte[] fst = sh.utf8, snd = d.arr;
                if ((flLen & SH_SUFF_MASK) != 0) {
                    fst = d.arr;   fstOff = sndOff;    fstLen = sndLen;
                    snd = sh.utf8; sndOff = sh.offset; sndLen = sh.len;
                }
                int termHash = SegmentRope.hashUnsafe(FNV_BASIS, fst, fstOff, fstLen);
                h ^=           SegmentRope.hashUnsafe(termHash,  snd, sndOff, sndLen);
            }
        } finally {
            if (tmp != null) tmp.close();
        }
        return h;
    }

    private int safeHashCode(int row) {
        int h = 0;
        var shared = this.shared;
        var d = rowsData[row];
        PooledTermView tmp = null;
        try {
            for (int c = 0, cols = this.cols, shIdx = cols*row; c < cols; c++, shIdx++) {
                SegmentRope sh = shared[shIdx];
                int flLen = readLen(d.arr, c), fstLen, sndLen = flLen&LEN_MASK;
                long fstOff, sndOff = readOff(d.arr, c);
                if (sh == null) {
                    sh = FinalSegmentRope.EMPTY;
                } else if (Term.isNumericDatatype(sh)) {
                    if (tmp == null)
                        tmp = PooledTermView.ofEmptyString();
                    tmp.wrap(sh, d.segment, d.arr, sndOff, sndLen, true);
                    h ^= tmp.hashCode();
                    continue;
                }
                fstLen = sh.len;
                fstOff = sh.offset;
                MemorySegment fst = sh.segment, snd = d.segment;
                if ((flLen & SH_SUFF_MASK) != 0) {
                    fst = snd ;       fstOff = sndOff;    fstLen = sndLen;
                    snd = sh.segment; sndOff = sh.offset; sndLen = sh.len;
                }
                int termHash = SegmentRope.hashSafe(FNV_BASIS, fst, fstOff, fstLen);
                h ^=           SegmentRope.hashSafe(termHash,  snd, sndOff, sndLen);
            }
        } finally {
            if (tmp != null) tmp.close();
        }
        return h;
    }

    private static final byte[] DUMP_NULL = "null".getBytes(UTF_8);
    @Override public void dump(MutableRope dest, int row) {
        var d = rowsData[row];
        if (d == null || !BS.get(has, row)) {
            dest.append(DUMP_NULL);
            return;
        }
        dest.append('[');
        for (int c = 0, shBase=row*cols; c < cols; c++) {
            int flagLen = readLen(d.arr, c);
            int off = readOff(d.arr, c);
            SegmentRope sh = shared[shBase + c];
            if ((flagLen&SH_SUFF_MASK) == 0) {
                if (sh == null) {
                    dest.append(DUMP_NULL);
                } else {
                    dest.append(sh);
                    dest.append(d.arr, off, flagLen & LEN_MASK);
                }
            } else {
                dest.append(d.arr, off, flagLen & LEN_MASK);
                if (sh != null)
                    dest.append(sh);
            }
            dest.append(',').append(' ');
        }
        if (cols > 0) dest.len -=2 ;
        dest.append(']');
    }
}
