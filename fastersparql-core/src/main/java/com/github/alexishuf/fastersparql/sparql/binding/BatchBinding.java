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
    public int row, cols;
    public Vars vars;
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

    public BatchBinding(Vars vars) {
        this.vars = vars;
        this.cols = vars.size();
    }

    @Override public final Vars vars() { return vars; }

    public final void vars(Vars vars) {
        this.vars = vars;
        this.cols = vars.size();
    }

    public final BatchBinding attach(@Nullable Batch<?> batch, int row) {
        if (batch != null) {
            if (row < 0 || row >= batch.rows)
                throw new IndexOutOfBoundsException(row);
            this.cols = batch.cols;
        }
        this.batch = batch;
        this.row = row;
        return this;
    }

    @Override final public @Nullable Term get(int i) {
        var batch = this.batch;
        if (batch == null)
            return null;
        if (i >= cols) {
            if (remainder == null) throw new IndexOutOfBoundsException(i);
            return remainder.get(i-cols);
        }
        return batch.get(row, i);
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
        var batch = this.batch;
        if (batch == null)
            return false;
        if (i >= cols) {
            if (remainder == null) throw new IndexOutOfBoundsException(i);
            remainder.get(i-cols, view);
        }
        return batch.getView(row, i, view);
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
        var batch = this.batch;
        if (batch == null)
            return false;
        if (i >= cols) {
            if (remainder == null) throw new IndexOutOfBoundsException(i);
            return remainder.get(i-cols, view);
        }
        return batch.getRopeView(row, i, view);
    }

    @Override public final boolean has(int i) {
        var batch = this.batch;
        if (batch == null)
            return false;
        if (i >= cols) {
            if (remainder == null) throw new IndexOutOfBoundsException(i);
            return remainder.has(i-cols);
        }
        return batch.termType(row, i) != null;
    }

    @Override public final int writeSparql(int i, ByteSink<?, ?> dest, PrefixAssigner assigner) {
        Batch<?> batch = this.batch;
        if (batch == null)
            return 0;
        if (i > cols) {
            if (remainder == null) throw new IndexOutOfBoundsException(i);
            return writeSparql(i - cols, dest, assigner);
        }
        return batch.writeSparql(dest, row, i, assigner);
    }
}