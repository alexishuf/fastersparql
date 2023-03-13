package com.github.alexishuf.fastersparql.batch.adapters;

import com.github.alexishuf.fastersparql.batch.Batch;
import com.github.alexishuf.fastersparql.batch.LazyBIt;
import com.github.alexishuf.fastersparql.batch.base.SPSCBufferedBIt;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.row.RowType;
import org.checkerframework.checker.nullness.qual.Nullable;

public abstract class LazySPSCBufferedBIt<T> extends SPSCBufferedBIt<T> implements LazyBIt<T> {
    private boolean started = false;

    public LazySPSCBufferedBIt(RowType<T> rowType, Vars vars) { super(rowType, vars); }

    /**
     * Method that will run once on first nextBatch()/hasNext() call and should feed this
     * BIt until completion/error.
     */
    protected abstract void run();

    @Override public void start() {
        boolean starting = false;
        lock();
        try {
            if (!started)
                starting = started = true;
        } finally { unlock(); }
        if (starting)
            try { run(); } catch (Throwable t) { complete(t); }
    }

    @Override protected @Nullable Batch<T> fetch() {
        if (!started)
            start();
        return super.fetch();
    }

    @Override public boolean hasNext() {
        if (!started)
            start();
        return super.hasNext();
    }
}
