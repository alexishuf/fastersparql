package com.github.alexishuf.fastersparql.sparql.binding;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.ByteSink;
import com.github.alexishuf.fastersparql.sparql.PrefixAssigner;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import org.checkerframework.checker.nullness.qual.Nullable;

public final class BatchBinding<B extends Batch<B>> extends Binding {
    static boolean SUPPRESS_SET_WARN = false;
    private final @Nullable BatchType<B> batchType;
    public @Nullable B batch;
    public int row;

    public BatchBinding(Vars vars) {
        super(vars);
        batchType = null;
    }

    public BatchBinding(@Nullable BatchType<B> batchType, Vars vars) {
        super(vars);
        this.batchType = batchType;
    }

    public BatchBinding<B> setRow(@Nullable Batch<?> batch, int row) {
        //noinspection unchecked
        this.batch = (B) batch;
        this.row = row;
        return this;
    }

    @Override public Binding set(int column, @Nullable Term value) {
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

    @Override public @Nullable Term get(int i) {
        if (i < 0 || i >= vars.size())
            throw new IndexOutOfBoundsException();
        if (batch == null || (row == 0 && batch.rows == 0))
            return null;
        return batch.get(row, i);
    }

    @Override public boolean has(int i) {
        return batch != null && batch.rows != 0 && batch.termType(row, i) != null;
    }

    @Override public int writeSparql(int i, ByteSink<?, ?> dest, PrefixAssigner assigner) {
        return batch != null && batch.rows != 0
                ? batch.writeSparql(dest, row, i, assigner) : 0;
    }
}