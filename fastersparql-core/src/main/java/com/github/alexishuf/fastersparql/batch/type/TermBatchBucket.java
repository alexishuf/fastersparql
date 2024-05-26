package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.model.rope.MutableRope;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import com.github.alexishuf.fastersparql.util.BS;
import com.github.alexishuf.fastersparql.util.SafeCloseable;
import com.github.alexishuf.fastersparql.util.concurrent.Alloc;
import com.github.alexishuf.fastersparql.util.concurrent.ArrayAlloc;
import com.github.alexishuf.fastersparql.util.concurrent.LIFOPool;
import com.github.alexishuf.fastersparql.util.concurrent.LevelAlloc;
import com.github.alexishuf.fastersparql.util.owned.AbstractOwned;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import com.github.alexishuf.fastersparql.util.owned.SpecialOwner;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;

import static com.github.alexishuf.fastersparql.batch.type.TermBatchType.TERM;
import static com.github.alexishuf.fastersparql.util.concurrent.ArrayAlloc.cleanLongsAtLeast;
import static com.github.alexishuf.fastersparql.util.concurrent.ArrayAlloc.recycleLongs;
import static java.lang.System.arraycopy;

public abstract sealed class TermBatchBucket
        extends AbstractOwned<TermBatchBucket>
        implements RowBucket<TermBatch, TermBatchBucket> {
    private static final ArrayAlloc<Term[]> TERMS = new ArrayAlloc<>(Term[].class,
            "TermBatchBucket.TERMS", 4,
            new LevelAlloc.Capacities()
                    .set(0, 3, Alloc.THREADS*32)
                    .set(4, 9, Alloc.THREADS*64)
                    .set(10, 15, Alloc.THREADS*32)
    );

//    static {
//        Primer.INSTANCE.sched(() -> {
//            TERMS.primeLevelLocalAndShared(len2level(TermBatchType.PREFERRED_BATCH_TERMS));
//            TERMS.primeLevelLocalAndShared(len2level(Short.MAX_VALUE));
//        });
//    }

    private Term[] terms;
    private long[] has;
    private int rows, cols, rowsCapacity;
    private LIFOPool<RowBucket<TermBatch, ?>> pool;

    static int estimateBytes(int rows, int cols) {
        return 16+6*4
                + 20*rows*cols*4
                + 20+BS.longsFor(rows)*8;
    }


    public TermBatchBucket(int rows, int cols) {
        this.rows         = rows;
        this.cols         = cols;
        this.terms        = TERMS.createAtLeast(rows*cols);
        this.rowsCapacity = terms.length/Math.max(1, cols);
        this.has          = cleanLongsAtLeast(rowsCapacity);
    }

    @Override public @Nullable TermBatchBucket recycle(Object currentOwner) {
        if (pool != null) {
            internalMarkRecycled(currentOwner);
            if (pool.offer(this) == null)
                return null;
            currentOwner = SpecialOwner.RECYCLED;
        }
        internalMarkGarbage(currentOwner);
        terms = TERMS.offer(terms, terms.length);
        has   = recycleLongs(has);
        return null;
    }

    static final class Concrete extends TermBatchBucket implements Orphan<TermBatchBucket> {
        public Concrete(int rows, int cols) {super(rows, cols);}
        @Override public TermBatchBucket takeOwnership(Object o) {return takeOwnership0(o);}
    }

    @Override public @This TermBatchBucket setPool(LIFOPool<RowBucket<TermBatch, ?>> pool) {
        this.pool = pool;
        return this;
    }

    @Override public void maximizeCapacity() {
        rows = rowsCapacity;
    }

    @Override public void grow(int addRows) {
        int nRows = rows+addRows;
        if (nRows > rowsCapacity) {
            int safeCols = Math.max(1, cols);
            terms = TERMS.grow(terms, terms.length, nRows*safeCols);
            rowsCapacity = terms.length/safeCols;
            int oldWords = has.length, requiredWords = BS.longsFor(rowsCapacity);
            if (requiredWords > oldWords) {
                has = ArrayAlloc.grow(has, requiredWords);
                Arrays.fill(has, oldWords, requiredWords, 0L);
            }
        }
        rows = nRows;
    }

    @Override public void clear(int rows, int cols) {
        boolean clean = false;
        int safeCols = Math.max(1, cols);
        if (cols != this.cols) {
            rowsCapacity = terms.length/safeCols;
            this.cols = cols;
        }
        if (rows > rowsCapacity) {
            TERMS.offer(terms, terms.length);
            terms        = TERMS.createAtLeast(rows*cols);
            rowsCapacity = terms.length/safeCols;
            int reqWords = BS.longsFor(rowsCapacity);
            if (reqWords > has.length) {
                recycleLongs(has);
                has = cleanLongsAtLeast(reqWords);
                clean = true;
            }
        }
        this.rows = rows;
        if (!clean)
            Arrays.fill(has, 0L);
    }

    @Override public boolean has(int row) { return row < rows && BS.get(has, row); }

    @Override public BatchType<TermBatch> batchType()        { return TERM; }
    @Override public int                       cols()        { return cols; }
    @Override public int                   capacity()        { return rows; }

    @Override public int hashCode(int row) {
        int acc = 0, cols = this.cols;
        if (row < 0 || row >= rows)
            throw new IndexOutOfBoundsException(row);
        if (BS.get(has, row)) {
            for (int i = row*cols, end = (row+1)*cols; i < end; i++)
                acc ^= Term.hashCode(terms[i]);
        }
        return acc;
    }

    @Override public void set(int dst, TermBatch batch, int row) {
        int cols = this.cols;
        if (cols != batch.cols)
            throw new IllegalArgumentException("cols mismatch");
        if (dst < 0 || dst >= rows)
            throw new IndexOutOfBoundsException("dst < 0 || dst >= capacity()");
        BS.set(has, dst);
        arraycopy(batch.arr, row*cols, terms, dst*cols, cols);
    }

    @Override public void set(int dst, RowBucket<TermBatch, ?> other, int src) {
        TermBatchBucket bucket = (TermBatchBucket)other;
        int cols = this.cols;
        if (bucket.cols != cols)
            throw new IllegalArgumentException("cols mismatch");
        if (dst < 0 || dst > rows || src < 0 || src > bucket.rows)
            throw new IndexOutOfBoundsException("dst or src are out of bounds");
        if (BS.get(bucket.has, src)) {
            BS.set(has, dst);
            arraycopy(bucket.terms, src*cols, terms, dst*cols, cols);
        } else {
            BS.clear(has, dst);
        }
    }

    @Override public void set(int dst, int src) {
        if (src == dst) return;
        int cols = this.cols;
        if (BS.get(has, src)) {
            BS.set(has, dst);
            arraycopy(terms, src*cols, terms, dst*cols, cols);
        } else {
            BS.clear(has, dst);
        }
    }

    @Override public void putRow(TermBatch dst, int srcRow) {
        if (BS.get(has, srcRow)) {
            if (dst.cols != cols)
                throw new IllegalArgumentException("cols mismatch");
            dst.putRow(terms, srcRow*cols);
        }
    }

    @Override public boolean equals(int row, TermBatch other, int otherRow) {
        int cols = this.cols;
        if (cols != other.cols)
            throw new IllegalArgumentException("cols mismatch");
        if (!BS.get(has, row))
            return false;
        Term[] la = terms, ra = other.arr;
        for (int l = row*cols, r = otherRow*cols, e = l+cols; l < e; l++, r++)
            if (!Objects.equals(la[l], ra[r])) return false;
        return true;
    }

    private static final byte[] DUMP_NULL = "null".getBytes(StandardCharsets.UTF_8);

    @Override public void dump(MutableRope dest, int row) {
        if (!has(row)) {
            dest.append(DUMP_NULL);
        } else {
            dest.append('[');
            int cols = cols();
            for (int i = row*cols, end = i+cols; i < end; i++) {
                Term term = terms[i];
                if (term == null) dest.append(DUMP_NULL);
                else              dest.append(term);
                dest.append(',').append(' ');
            }
            if (cols > 0) dest.len -= 2;
            dest.append('[');
        }
    }

    @Override public String toString() { return "TermBatchBucket{capacity="+capacity()+"}"; }

    @Override public @NonNull Iterator<TermBatch> iterator() {
        return new It();
    }

    private class It implements Iterator<TermBatch>, SafeCloseable {
        private TermBatch tmp = TERM.create(cols).takeOwnership(this);
        private int row = 0;
        private boolean filled = false;

        @Override public void close() { tmp = Batch.safeRecycle(tmp, this); }

        @Override public boolean hasNext() {
            boolean hasNext = tmp != null;
            if (hasNext && !filled) {
                filled = true;
                tmp.clear();
                TermBatchBucket bucket = TermBatchBucket.this;
                Term[] terms = bucket.terms;
                long[] has = bucket.has;
                int rows = bucket.rows, cols = bucket.cols;
                for (int used = 0, free = tmp.termsCapacity() ; used < free && row < rows; ++row) {
                    if (BS.get(has, row)) {
                        used += cols;
                        tmp.putRow(terms, row*cols);
                    }
                }
                hasNext = tmp.rows > 0;
                if (!hasNext)
                    close();
            }
            return hasNext;
        }

        @Override public TermBatch next() {
            if (!hasNext()) throw new NoSuchElementException();
            filled = false;
            return tmp;
        }
    }
}
