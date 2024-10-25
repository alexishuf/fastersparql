package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.model.rope.*;
import com.github.alexishuf.fastersparql.sparql.expr.FinalTerm;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import com.github.alexishuf.fastersparql.sparql.expr.TermView;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.List;

import static com.github.alexishuf.fastersparql.FSProperties.BATCH_NO_INTERN_IRI;
import static com.github.alexishuf.fastersparql.FSProperties.batchNoInternIri;
import static com.github.alexishuf.fastersparql.batch.type.TermBatchType.TERM;
import static com.github.alexishuf.fastersparql.model.rope.FinalSegmentRope.EMPTY;

public abstract sealed class TermBatch extends ObjBatch<TermBatch, FinalTerm> {
    private static final Logger log = LoggerFactory.getLogger(TermBatch.class);
    private static final boolean NO_INTERN_IRI = batchNoInternIri();

    static {
        if (NO_INTERN_IRI)
            log.warn("{}=true, this will cause excess latency and garbage", BATCH_NO_INTERN_IRI);
    }

    /* --- --- --- lifecycle --- --- --- */

    /**
     * Creates a batch that holds {@code arr} <strong>BY REFERENCE</strong>. {@code arr} must
     * enumerate all terms of all rows in row-major order (column {@code c} of row {@code r} is
     * at index {@code r*cols + c}).
     *
     * @param arr row-major array of terms. held by <strong>REFERENCE</strong>
     * @param rows number of rows in {@code lst}
     * @param cols number of columns in {@code lst}
     * @throws IllegalArgumentException if {@code arr.length < rows*cols}
     */
    public TermBatch(FinalTerm[] arr, int rows, int cols) {
        super(arr, (short)rows, (short)cols);
    }

    protected final static class Concrete extends TermBatch implements Orphan<TermBatch> {
        @SuppressWarnings("unused") // add 64 bytes of padding against false sharing
        private volatile long l1_0, l1_1, l1_2, l1_3, l1_4, l1_5, l1_6, l1_7;
        public Concrete(FinalTerm[] arr, int rows, int cols) {
            super(arr, rows, cols);
        }
        @Override public TermBatch takeOwnership(Object o) {return takeOwnership0(o);}
    }

    @Override protected void schedNodeCleanup(TermBatch node, Object nodeOwner) {
        TermBatchCleaner.INSTANCE.sched(node, nodeOwner);
    }

    /**
     * Creates a batch that holds {@code lst} <strong>BY REFERENCE</strong>. {@code lst} must
     * enumerate all terms of all rows in row-major order (column {@code c} of row {@code r} is
     * at index {@code r*cols + c}).
     *
     * @param lst row-major list of terms. held by <strong>REFERENCE</strong>
     * @param rows number of rows in {@code lst}
     * @param cols number of columns in {@code lst}
     * @throws IllegalArgumentException if {@code lst.size() < rows*cols}
     */
    public static Orphan<TermBatch> rowMajor(List<? extends Term> lst, int rows, int cols) {
        int terms = rows*cols;
        if (lst.size() != terms)
            throw new IllegalArgumentException("lst.size() < terms");
        TermBatch b = TERM.create(cols).takeOwnership(lst);
        if (b.arr.length < terms) {
            b.recycle(lst);
            b = new TermBatch.Concrete(new FinalTerm[terms], rows, cols).takeOwnership(lst);
        }
        for (int i = 0; i < terms; i++)
            b.arr[i] = FinalTerm.asFinal(lst.get(i));
        return b.releaseOwnership(lst);
    }

    /** Create a {@link TermBatch} with a single row and {@code row.size()} columns. */
    @SafeVarargs
    public static Orphan<TermBatch> of(List<? extends Term>... rows) {
        int cols = rows.length == 0 ? 0 : rows[0].size();
        TermBatch b = TERM.create(cols).takeOwnership(rows);
        if (b.termsCapacity() < rows.length*cols) {
            b.recycle(rows);
            b = new TermBatch.Concrete(new FinalTerm[rows.length*cols], rows.length, cols)
                    .takeOwnership(rows);
        }
        for (List<? extends Term> row : rows)
            b.putRow(row);
        return b.releaseOwnership(rows);
    }

    /* --- --- --- batch accessors --- --- --- */

    @Override public TermBatchType type() { return TERM; }
    /* --- --- --- term accessors --- --- --- */

    @Override public @Nullable FinalTerm get(@NonNegative int row, @NonNegative int col) {
        return obj(row, col);
    }

    @Override public TermInfo.Type get(@NonNegative int row, @NonNegative int col, TermInfo info) {
        return info.setTerm(obj(row, col));
    }

    @Override public boolean getView(@NonNegative int row, @NonNegative int col, TermView dest) {
        Term t = obj(row, col);
        if (t == null) return false;
        dest.wrap(t);
        return true;
    }

    @Override
    public boolean getRopeView(@NonNegative int row, @NonNegative int col, TwoSegmentRope dest) {
        Term t = obj(row, col);
        if (t == null) return false;
        dest.wrapFirst(t.first());
        dest.wrapSecond(t.second());
        return true;
    }

    /* --- --- --- mutators --- --- --- */

    @Override protected void putTermConverting(int dstCol, Batch<?> other, int row, int col) {
        FinalTerm t =  NO_INTERN_IRI ? makeTermNoIntern(other, row, col) : other.get(row, col);
        putTerm(dstCol, t);
    }

    private FinalTerm makeTermNoIntern(Batch<?> other, int row, int col) {
        if (other.termType(row, col) == Term.Type.IRI) {
            try (var view = PooledTwoSegmentRope.ofEmpty()) {
                if (other.getRopeView(row, col, view)) {
                    var copy = new byte[view.len];
                    view.copy(0, view.len, copy, 0);
                    return new FinalTerm(EMPTY, new FinalSegmentRope(copy), false);
                }
                return null;
            }
        }
        return other.get(row, col);
    }

    @Override public void putTerm(int col, Term t) {
        var tail = this.tail;
        if (col < 0 || col >= tail.cols) throw new IndexOutOfBoundsException();
        tail.arr[tail.offerRowBase+col] = FinalTerm.asFinal(t);
    }

    @Override
    public void putTermLocalByReference(int col, FinalSegmentRope shared, MemorySegment local,
                                        byte @Nullable [] localU8, long localOff,
                                        int localLen, byte sharedKind) {
        var tail = this.tail;
        if (col < 0 || col >= tail.cols)
            throw new IndexOutOfBoundsException(col);
        FinalTerm term;
        if ((shared == null || shared.len == 0) && localLen == 0) {
            term = null;
        } else if (NO_INTERN_IRI && !SharedKind.isLit(sharedKind)) {
            term = makeTermNoIntern(shared, local, localOff, localLen);
        } else {
            var localRope = new SegmentRopeView().wrap(local, localU8, localOff, localLen);
            SegmentRope fst, snd;
            if (SharedKind.isSuffix(sharedKind)) { fst = localRope; snd =    shared; }
            else                                 { fst =    shared; snd = localRope; }
            term = Term.wrap(fst, snd);
        }
        tail.arr[tail.offerRowBase+col] = term;
    }

    private FinalTerm makeTermNoIntern(FinalSegmentRope shared, MemorySegment local,
                                       long localOff, int localLen) {
        int shLen = shared == null ? 0 : shared.len;
        byte[] copy = new byte[shLen + localLen];
        if (shLen > 0)
            shared.copy(0, shLen, copy, 0);
        MemorySegment.copy(local, ValueLayout.JAVA_BYTE, localOff, copy, shLen, localLen);
        return new FinalTerm(EMPTY, new FinalSegmentRope(copy), false);
    }
}
