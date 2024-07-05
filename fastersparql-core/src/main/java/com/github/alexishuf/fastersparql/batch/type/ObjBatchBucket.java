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

import java.lang.reflect.Array;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantLock;

import static com.github.alexishuf.fastersparql.util.concurrent.ArrayAlloc.cleanLongsAtLeast;
import static com.github.alexishuf.fastersparql.util.concurrent.ArrayAlloc.recycleLongs;
import static java.lang.System.arraycopy;

public abstract class ObjBatchBucket<T, B extends ObjBatch<B, T>>
        extends AbstractOwned<ObjBatchBucket<T, B>>
        implements RowBucket<B, ObjBatchBucket<T, B>> {
    private static final ReentrantLock TYPE_2_ARRAY_ALLOC_LOCK = new ReentrantLock();
    private static ArrayAlloc<?>[] TYPE_2_ARRAY_ALLOC = new ArrayAlloc[64];

    private static <T, B extends ObjBatch<B, T>> ArrayAlloc<T[]>
    arrayAlloc(ObjBatchType<T, B> type) {
        int id = type.id;
        ArrayAlloc<?> alloc;
        if (id >= TYPE_2_ARRAY_ALLOC.length || (alloc= TYPE_2_ARRAY_ALLOC[id]) == null) {
            return arrayAllocMake(type);
        }
        //noinspection unchecked
        return (ArrayAlloc<T[]>)alloc;
    }

    @SuppressWarnings("unchecked") private static <T, B extends ObjBatch<B, T>>
    ArrayAlloc<T[]> arrayAllocMake(ObjBatchType<T, B> type) {
        TYPE_2_ARRAY_ALLOC_LOCK.lock();
        try {
            var arrCls = (Class<T[]>)Array.newInstance(type.termClass(), 0).getClass();
            if (TYPE_2_ARRAY_ALLOC.length < type.id) {
                int newLen = Math.max(type.id + 1, 2 * TYPE_2_ARRAY_ALLOC.length);
                TYPE_2_ARRAY_ALLOC = Arrays.copyOf(TYPE_2_ARRAY_ALLOC, newLen);
            }
            var alloc = (ArrayAlloc<T[]>)TYPE_2_ARRAY_ALLOC[type.id];
            if (alloc != null)
                return alloc;
            alloc = new ArrayAlloc<>(arrCls,
                    "ObjBatchBucket(" + type + ").arrayAlloc", 4,
                    new LevelAlloc.Capacities()
                            .set(0, 3, Alloc.THREADS * 32)
                            .set(4, 9, Alloc.THREADS * 64)
                            .set(10, 15, Alloc.THREADS * 32)
            );
            TYPE_2_ARRAY_ALLOC[type.id] = alloc;
            return alloc;
        } finally { TYPE_2_ARRAY_ALLOC_LOCK.unlock(); }
    }

    private T[] terms;
    private long[] has;
    private int rows, cols, rowsCapacity;
    private final ArrayAlloc<T[]> arrayAlloc;
    private final BatchType<B> batchType;
    private LIFOPool<RowBucket<B, ?>> pool;

    static int estimateBytes(int rows, int cols) {
        return 16+6*4
                + 20*rows*cols*4
                + 20+BS.longsFor(rows)*8;
    }

    protected ObjBatchBucket(ObjBatchType<T, B> type, int rows, int cols) {
        this.rows         = rows;
        this.cols         = cols;
        this.batchType    = type;
        this.arrayAlloc   = arrayAlloc(type);
        this.terms        = arrayAlloc.createAtLeast(rows*cols);
        this.rowsCapacity = terms.length/Math.max(1, cols);
        this.has          = cleanLongsAtLeast(rowsCapacity);
    }

    public static <T, B extends ObjBatch<B, T>>
    Orphan<ObjBatchBucket<T, B>> create(ObjBatchType<T, B> type, int rows, int cols) {
        return new Concrete<>(type, rows, cols);
    }

    private static final class Concrete<T, B extends ObjBatch<B, T>>
            extends ObjBatchBucket<T, B>
            implements Orphan<ObjBatchBucket<T, B>> {
        public Concrete(ObjBatchType<T, B> type, int rows, int cols) {
            super(type, rows, cols);
        }
        @Override public ObjBatchBucket<T, B> takeOwnership(Object o) {return takeOwnership0(o);}
    }

    @Override public @Nullable ObjBatchBucket<T, B> recycle(Object currentOwner) {
        if (pool != null) {
            internalMarkRecycled(currentOwner);
            if (pool.offer(this) == null)
                return null;
            currentOwner = SpecialOwner.RECYCLED;
        }
        internalMarkGarbage(currentOwner);
        terms = arrayAlloc.offer(terms, terms.length);
        has   = recycleLongs(has);
        return null;
    }

    @Override public @This ObjBatchBucket<T, B> setPool(LIFOPool<RowBucket<B, ?>> pool) {
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
            terms = arrayAlloc.grow(terms, terms.length, nRows*safeCols);
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
            arrayAlloc.offer(terms, terms.length);
            terms        = arrayAlloc.createAtLeast(rows*cols);
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

    @Override public BatchType<B> batchType() { return batchType; }
    @Override public int               cols() { return cols; }
    @Override public int           capacity() { return rows; }

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

    @Override public void set(int dst, B batch, int row) {
        int cols = this.cols;
        if (cols != batch.cols)
            throw new IllegalArgumentException("cols mismatch");
        if (dst < 0 || dst >= rows)
            throw new IndexOutOfBoundsException("dst < 0 || dst >= capacity()");
        BS.set(has, dst);
        arraycopy(batch.arr, row*cols, terms, dst*cols, cols);
    }

    @Override public void set(int dst, RowBucket<B, ?> other, int src) {
        @SuppressWarnings("unchecked") ObjBatchBucket<T, B> bucket = (ObjBatchBucket<T, B>)other;
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

    @Override public void putRow(B dst, int srcRow) {
        if (BS.get(has, srcRow)) {
            if (dst.cols != cols)
                throw new IllegalArgumentException("cols mismatch");
            dst.putRow(terms, srcRow*cols);
        }
    }

    @Override public boolean equals(int row, B other, int otherRow) {
        int cols = this.cols;
        if (cols != other.cols)
            throw new IllegalArgumentException("cols mismatch");
        if (!BS.get(has, row))
            return false;
        T[] la = terms, ra = other.arr;
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
                var term = terms[i];
                if (term == null) dest.append(DUMP_NULL);
                else              dest.append(term);
                dest.append(',').append(' ');
            }
            if (cols > 0) dest.len -= 2;
            dest.append('[');
        }
    }

    @Override public String toString() { return "ObjBatchBucket{capacity="+capacity()+"}"; }

    @Override public @NonNull Iterator<B> iterator() {
        return new It();
    }

    private class It implements Iterator<B>, SafeCloseable {
        private B tmp = batchType.create(cols).takeOwnership(this);
        private int row = 0;
        private boolean filled = false;

        @Override public void close() { tmp = Batch.safeRecycle(tmp, this); }

        @Override public boolean hasNext() {
            boolean hasNext = tmp != null;
            if (hasNext && !filled) {
                filled = true;
                tmp.clear();
                ObjBatchBucket<T, B> bucket = ObjBatchBucket.this;
                T[] terms = bucket.terms;
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

        @Override public B next() {
            if (!hasNext()) throw new NoSuchElementException();
            filled = false;
            return tmp;
        }
    }
}
