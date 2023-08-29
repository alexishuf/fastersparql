package com.github.alexishuf.fastersparql.sparql.binding;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.ByteSink;
import com.github.alexishuf.fastersparql.sparql.PrefixAssigner;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import com.github.alexishuf.fastersparql.store.batch.StoreBatchType;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

public class BatchBinding<B extends Batch<B>> extends Binding {
    private static final BatchBinding<?>[] EMPTY = new BatchBinding[BatchType.MAX_BATCH_TYPE_ID];
    static {
        for (var t : List.of(Batch.TERM, Batch.COMPRESSED, StoreBatchType.INSTANCE)) {
            var b = t.create(1, 0, 0);
            b.beginPut();
            b.commitPut();
            EMPTY[t.id] = new BatchBinding<>(Vars.EMPTY).setRow(b, 0);
        }
    }

    static boolean SUPPRESS_SET_WARN = false;
    private final @Nullable BatchType<B> batchType;
    public @Nullable B batch;
    public int row;

    public static <B extends Batch<B>> BatchBinding<B> ofEmpty(BatchType<B> type) {
        //noinspection unchecked
        var bb = (BatchBinding<B>)EMPTY[type.id];
        B b = bb == null ? null : bb.batch;
        if (b == null || b.rows != 1  || b.cols != 0) {
            b = type.create(1, 0, 0);
            b.beginPut();
            b.commitPut();
            EMPTY[type.id] = bb = new BatchBinding<B>(Vars.EMPTY).setRow(b, 0);
        }
        return bb;
    }

    public BatchBinding(Vars vars) {
        super(vars);
        batchType = null;
    }

    public BatchBinding(@Nullable BatchType<B> batchType, Vars vars) {
        super(vars);
        this.batchType = batchType;
    }

    /**
     * Create a <strong>deep</strong> copy of another {@link BatchBinding}: the new binding
     * will own a batch with a copy of the row pointed to by {@code other}.
     *
     * <p><strong>Important:</strong> since {@link BatchBinding}s have no lifecycle, who holds
     * this {@link BatchBinding} is responsible for recycling {@link BatchBinding#batch}.</p>
     *
     * @param other a {@link BatchBinding} with a batch set.
     */
    public BatchBinding(BatchBinding<B> other) {
        super(other.vars);
        B batch = other.batch;
        int row = other.row;
        if (batch != null) {
            batchType = batch.type();
            if (row < batch.rows) {
                B copy = batch.type().create(1, batch.cols, batch.localBytesUsed(row));
                copy.putRow(batch, row);
                this.batch = copy;
                this.row = 0;
            }
        } else {
            this.batchType = null;
        }
    }

    public final BatchBinding<B> setRow(@Nullable Batch<?> batch, int row) {
        //noinspection unchecked
        this.batch = (B) batch;
        this.row = row;
        return this;
    }

    @Override public final Binding set(int column, @Nullable Term value) {
        assert SUPPRESS_SET_WARN : "BatchBinding.set() is terribly slow. Use ArrayBinding instead.";
        if (batchType == null)
            throw new UnsupportedOperationException("No batchType set");
        int n = vars.size();
        B next = batchType.createSingleton(n);
        next.beginPut();
        for (int c = 0; c < column; c++) next.putTerm(c, batch == null ? null : batch.get(row, c));
        next.putTerm(column, value);
        for (int c = column+1; c < n; c++) next.putTerm(c, batch == null ? null : batch.get(row, c));
        next.commitPut();
        batch = next;
        row = 0;
        return this;
    }

    @Override final public @Nullable Term get(int i) {
        if (i < 0 || i >= vars.size())
            throw new IndexOutOfBoundsException();
        if (batch == null || (row == 0 && batch.rows == 0))
            return null;
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
        if (i < 0 || i >= vars.size())
            throw new IndexOutOfBoundsException();
        if (batch == null || (row == 0 && batch.rows == 0))
            return false;
        return batch.getView(row, i, view);
    }

    @Override public final boolean has(int i) {
        return batch != null && batch.rows != 0 && batch.termType(row, i) != null;
    }

    @Override public final int writeSparql(int i, ByteSink<?, ?> dest, PrefixAssigner assigner) {
        return batch != null && batch.rows != 0
                ? batch.writeSparql(dest, row, i, assigner) : 0;
    }
}