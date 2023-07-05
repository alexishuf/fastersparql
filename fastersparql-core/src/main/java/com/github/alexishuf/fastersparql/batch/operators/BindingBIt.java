package com.github.alexishuf.fastersparql.batch.operators;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.EmptyBIt;
import com.github.alexishuf.fastersparql.batch.Timestamp;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchMerger;
import com.github.alexishuf.fastersparql.client.BindQuery;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.sparql.binding.BatchBinding;
import org.checkerframework.checker.nullness.qual.Nullable;

public abstract class BindingBIt<B extends Batch<B>> extends AbstractFlatMapBIt<B> {
    protected final BatchMerger<B> merger;
    protected final BindQuery<B> bindQuery;
    private @Nullable B lb, rb;
    private int leftRow = -1;
    private final BIt<B> empty;
    private final BatchBinding<B> tempBinding;
    private long bindingSeq;
    private boolean bindingEmpty = false;
    private @Nullable Thread safeCleanupThread;

    /* --- --- --- lifecycle --- --- --- */

    public BindingBIt(BindQuery<B> bindQuery, @Nullable Vars projection) {
        super(projection != null ? projection : bindQuery.resultVars(),
              EmptyBIt.of(bindQuery.bindings.batchType()));
        var left = bindQuery.bindings;
        Vars leftPublicVars = left.vars();
        Vars rFree = bindQuery.query.publicVars().minus(leftPublicVars);
        this.lb          = batchType.createSingleton(leftPublicVars.size());
        this.bindQuery   = bindQuery;
        this.empty       = inner;
        this.merger      = batchType.merger(vars(), leftPublicVars, rFree);
        this.tempBinding = new BatchBinding<>(batchType, leftPublicVars);
        this.metrics     = bindQuery.metrics;
    }

    @Override protected void cleanup(@Nullable Throwable cause) {
        super.cleanup(cause);
        if (safeCleanupThread == Thread.currentThread()) {
            // only recycle lb and rb if we are certain they are exclusively held by this BIt.
            lb = batchType.recycle(lb);
            rb = batchType.recycle(rb);
        }
        inner.close();
        merger.release();
        if (cause != null)
            bindQuery.bindings.close();
    }

    /* --- --- --- delegate control --- --- --- */

    @Override public @Nullable B recycle(B batch) {
        if (batch == null || super.recycle(batch) == null) return null;
        if (bindQuery.bindings.recycle(batch) == null) return null;
        if (inner.recycle(batch) == null) return null;
        return batch;
    }

    /* --- --- --- binding behavior --- --- --- */
    protected abstract BIt<B> bind(BatchBinding<B> binding);

    @Override public B nextBatch(@Nullable B b) {
        boolean re = false;
        if (lb == null) return null; // already exhausted
        try {
            long startNs = needsStartTime ? Timestamp.nanoTime() : Timestamp.ORIGIN;
            b = getBatch(b);
            do {
                if (inner == empty) {
                    bindingEmpty = true;
                    if (++leftRow >= lb.rows) {
                        leftRow = 0;
                        lb = bindQuery.bindings.nextBatch(lb);
                        if (lb == null) break; // reached end
                    }
                    inner = bind(tempBinding.setRow(lb, leftRow));
                    re = true;
                }
                if ((rb = inner.nextBatch(rb)) == null) {
                    inner = empty;
                }
                boolean pub = switch (bindQuery.type) {
                    case JOIN,EXISTS      -> rb != null;
                    case NOT_EXISTS,MINUS -> rb == null;
                    case LEFT_JOIN        -> { re &= rb == null; yield rb != null || re; }
                };
                if (pub) {
                    bindingEmpty = false;
                    switch (bindQuery.type) {
                        case JOIN,LEFT_JOIN          ->   b = merger.merge(b, lb, leftRow, rb);
                        case EXISTS,NOT_EXISTS,MINUS -> { b.putRow(lb, leftRow); inner = empty; }
                    }
                }
                if (inner == empty) {
                    if (bindingEmpty) bindQuery.emptyBinding(bindingSeq++);
                    else              bindQuery.nonEmptyBinding(bindingSeq++);
                }
            } while (readyInNanos(b.rows, startNs) > 0);
            if (b.rows == 0) b = handleEmptyBatch(b);
            else             onBatch(b);
        } catch (Throwable t) {
            lb = null; // signal exhaustion
            onTermination(t);
            throw t;
        }
        return b;
    }

    private B handleEmptyBatch(B batch) {
        batchType.recycle(recycle(batch));
        safeCleanupThread = Thread.currentThread();
        onTermination(null);
        safeCleanupThread = null;
        return null;
    }

    /* --- --- --- toString() --- --- --- */

    @Override protected String toStringNoArgs() {
        return super.toStringNoArgs()+'['+bindQuery.type+']';
    }

    @Override public String toString() {
        return toStringNoArgs()+'('+bindQuery.bindings+", "+bindQuery.query+')';
    }
}
