package com.github.alexishuf.fastersparql.batch.adapters;

import com.github.alexishuf.fastersparql.batch.Batch;
import com.github.alexishuf.fastersparql.batch.LazyBIt;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.row.RowType;
import org.checkerframework.checker.nullness.qual.Nullable;

public abstract class LazyCallbackBIt<T> extends CallbackBIt<T> implements LazyBIt<T> {
    private boolean started = false;

    public LazyCallbackBIt(RowType<T> rowType, Vars vars) { super(rowType, vars); }

    /**
     * Method that will run once on first nextBatch()/hasNext() call and should feed this
     * BIt until completion/error.
     */
    protected abstract void run();

    @Override public void start() {
        lock.lock();
        try {
            if (!started) {
                started = true;
                try { run(); } catch (Throwable t) { complete(t); }
            }
        } finally { lock.unlock(); }
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
