package com.github.alexishuf.fastersparql.sparql.binding;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.batch.type.CompressedBatchType;
import com.github.alexishuf.fastersparql.batch.type.TermBatchType;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.ByteSink;
import com.github.alexishuf.fastersparql.model.rope.TwoSegmentRope;
import com.github.alexishuf.fastersparql.sparql.PrefixAssigner;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import com.github.alexishuf.fastersparql.store.batch.StoreBatchType;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

public class BatchBinding extends Binding {
    private static final BatchBinding[] EMPTY = new BatchBinding[BatchType.MAX_BATCH_TYPE_ID];
    static {
        for (var t : List.of(TermBatchType.TERM, CompressedBatchType.COMPRESSED, StoreBatchType.STORE)) {
            var b = t.create(0);
            b.beginPut();
            b.commitPut();
            EMPTY[t.id] = new BatchBinding(Vars.EMPTY).attach(b, 0);
        }
    }

    public @Nullable Batch<?> batch;
    public short row, cols;
    public Vars vars;
    public int sequence;
    public @Nullable BatchBinding remainder;

    public static BatchBinding ofEmpty(BatchType<?> type) {
        var bb = EMPTY[type.id];
        var b = bb == null ? null : bb.batch;
        if (b == null || b.rows != 1  || b.next != null || b.cols != 0) {
            (b = type.create(0)).beginPut();
            b.commitPut();
            EMPTY[type.id] = bb = new BatchBinding(Vars.EMPTY).attach(b, 0);
        }
        return bb;
    }

    public BatchBinding(Vars vars) { vars(vars); }

    @Override public final Vars vars() { return vars; }

    public final void vars(Vars vars) {
        this.vars = vars;
        int cols = vars.size();
        if (cols > Short.MAX_VALUE) throw new IllegalArgumentException("too many columns");
        this.cols = (short) cols;
    }

    public final BatchBinding attach(@Nullable Batch<?> batch, int row) {
        if (batch != null) {
            if (row < 0 || row >= batch.rows)
                throw new IndexOutOfBoundsException(row);
            cols = batch.cols;
        }
        this.batch = batch;
        this.row = (short)row;
        return this;
    }

    @Override final public @Nullable Term get(int i) {
        if (i >= cols) return getFromRemainder(i);
        var batch = this.batch;
        return batch == null ? null : batch.get(row, i);
    }

    private @Nullable Term getFromRemainder(int idx) {
        var name = vars.get(idx);
        int c = -1;
        var b = remainder;
        while (b != null && (c=b.vars.indexOf(name)) >= b.cols)
            b = b.remainder;
        if (b != null && c >= 0) {
            var batch = b.batch;
            if (batch == null) return null;
            return b.batch.get(b.row, c);
        }
        throw new IndexOutOfBoundsException("var not found");
    }

    public final <B extends Batch<B>> void putRow(B dstBatch) {
        int varsCount = vars.size();
        if (varsCount != dstBatch.cols)
            throw new IllegalArgumentException("dstBatch.cols != vars.size()");
        if (cols == varsCount) {
            dstBatch.putRowConverting(batch, row);
        } else {
            dstBatch.beginPut();
            for (int c = 0; c < varsCount; c++)
                putTerm(c, dstBatch, c);
            dstBatch.commitPut();
        }
    }

    public final <B extends Batch<B>> void putTerm(int dstCol, B dstBatch, int srcCol) {
        if (srcCol >= cols) {
            putTermFromRemainder(dstCol, dstBatch, srcCol);
        } else {
            Batch<?> srcBatch = this.batch;
            if (srcBatch != null) {
                if (srcBatch.type().equals(dstBatch.type())) //noinspection unchecked
                    dstBatch.putTerm(dstCol, (B) srcBatch, row, srcCol);
                else
                    dstBatch.putTerm(dstCol, srcBatch.get(row, srcCol));
            }
        }
    }

    private <B extends Batch<B>>
    void putTermFromRemainder(int dstCol, B dstBatch, int srcCol) {
        var name = vars.get(srcCol);
        int c = -1;
        var b = remainder;
        while (b != null && (c=b.vars.indexOf(name)) >= b.cols)
            b = b.remainder;
        if (b != null && c >= 0) {
            var batch = b.batch;
            if (batch == null) return;
            if (batch.type().equals(dstBatch.type()))//noinspection unchecked
                dstBatch.putTerm(dstCol, (B)batch, b.row, srcCol);
            else
                dstBatch.putTerm(dstCol, batch.get(b.row, srcCol));
        }
        throw new IndexOutOfBoundsException("var not found");
    }

    /**
     * If there is a term at column {@code i}, set {@code view} to it and return {@code true}.
     *
     * @param i the column to read from
     * @param view A mutable {@link Term} ({@link Term#mutable()}/{@link Term#pooledMutable()})
     *             that will have its segments replaced to be the same as the term at column
     *             {@code i}
     * @return {@code true} iff there was a non-null {@link Term} at column {@code i}, else,
     *         {@code false}
     * @throws IndexOutOfBoundsException if {@code i < 0 || i >= vars.size()}
     */
    final public boolean get(int i, Term view) {
        if (i >= cols) return getFromRemainder(i, view);
        var batch = this.batch;
        return batch != null && batch.getView(row, i, view);
    }

    private boolean getFromRemainder(int idx, Term view) {
        var name = vars.get(idx);
        int c = -1;
        var b = remainder;
        while (b != null && (c=b.vars.indexOf(name)) >= b.cols)
            b = b.remainder;
        if (b != null && c >= 0) {
            var batch = b.batch;
            if (batch == null) return false;
            return batch.getView(b.row, c, view);
        }
        throw new IndexOutOfBoundsException("var not found");
    }

    /**
     * If there is a ter at column {@code i}, set {@code view} to its N-Triples
     * form and return {@code true}.
     *
     * @param i the column (var index) to get
     * @param view A {@link TwoSegmentRope} that will be remapped if there is a term.
     * @return {@code true} iff there is a term at column {@code i}
     */
    final public boolean get(int i, TwoSegmentRope view) {
        if (i >= cols) return getFromRemainder(i, view);
        var batch = this.batch;
        return batch != null && batch.getRopeView(row, i, view);
    }

    private boolean getFromRemainder(int idx, TwoSegmentRope view) {
        var name = vars.get(idx);
        int c = -1;
        var b = remainder;
        while (b != null && (c=b.vars.indexOf(name)) >= b.cols)
            b = b.remainder;
        if (b != null && c >= 0) {
            var batch = b.batch;
            if (batch == null) return false;
            return batch.getRopeView(b.row, c, view);
        }
        throw new IndexOutOfBoundsException("var not found");
    }

    @Override public final boolean has(int i) {
        if (i >= cols) return hasInRemainder(i);
        var batch = this.batch;
        return batch != null && batch.termType(row, i) != null;
    }

    private boolean hasInRemainder(int idx) {
        var name = vars.get(idx);
        int c = -1;
        var b = remainder;
        while (b != null && (c=b.vars.indexOf(name)) >= b.cols)
            b = b.remainder;
        if (b != null && c >= 0) {
            var batch = b.batch;
            if (batch == null) return false;
            return batch.termType(b.row, c) != null;
        }
        throw new IndexOutOfBoundsException("var not found");
    }

    @Override public final int writeSparql(int i, ByteSink<?, ?> dest, PrefixAssigner assigner) {
        if (i >= cols) return writeSparqlRemainder(i, dest, assigner);
        var batch = this.batch;
        return batch == null ? 0 : batch.writeSparql(dest, row, i, assigner);
    }

    private int writeSparqlRemainder(int idx, ByteSink<?, ?> dest, PrefixAssigner assigner) {
        var name = vars.get(idx);
        int c = -1;
        var b = remainder;
        while (b != null && (c=b.vars.indexOf(name)) >= b.cols)
            b = b.remainder;
        if (b != null && c >= 0) {
            var batch = b.batch;
            if (batch == null) return 0;
            return batch.writeSparql(dest, b.row, c, assigner);
        }
        throw new IndexOutOfBoundsException("var not found");
    }

}