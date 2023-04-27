package com.github.alexishuf.fastersparql.batch.operators;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.Timestamp;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchMerger;
import com.github.alexishuf.fastersparql.model.BindType;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.operators.metrics.Metrics;
import com.github.alexishuf.fastersparql.sparql.binding.BatchBinding;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

public abstract class BindingBIt<B extends Batch<B>> extends AbstractFlatMapBIt<B> {
    protected final BatchMerger<B> merger;
    protected final BindType bindType;
    private @Nullable B lb, rb;
    private int leftRow = -1;
    private final BIt<B> left;
    private final BatchBinding<B> tempBinding;
    protected final Metrics.@Nullable JoinMetrics metrics;

    /* --- --- --- lifecycle --- --- --- */

    public BindingBIt(BIt<B> left, BindType type, Vars rightPublicVars,
                      @Nullable Vars projection,
                      Metrics. @Nullable JoinMetrics metrics) {
        super(left.batchType(),
              projection != null ? projection : type.resultVars(left.vars(), rightPublicVars));
        Vars leftPublicVars = left.vars();
        Vars rFree = rightPublicVars.minus(leftPublicVars);
        this.lb = batchType.createSingleton(leftPublicVars.size());
        this.left        = left;
        this.merger      = batchType.merger(vars(), leftPublicVars, rFree);
        this.bindType    = type;
        this.tempBinding = new BatchBinding<>(batchType, leftPublicVars);
        this.metrics     = metrics;
    }

    @Override protected void cleanup(@Nullable Throwable cause) {
        super.cleanup(cause);
        lb = batchType.recycle(lb);
        rb = batchType.recycle(rb);
        if (metrics != null) metrics.completeAndDeliver(cause, this);
        if (cause != null)
            left.close();
    }

    /* --- --- --- delegate control --- --- --- */

    @Override public @Nullable B recycle(B batch) {
        if (batch == null || super.recycle(batch) == null) return null;
        if (left.recycle(batch) == null) return null;
        if (inner != null && inner.recycle(batch) == null) return null;
        return batch;
    }

    /* --- --- --- binding behavior --- --- --- */
    protected abstract BIt<B> bind(BatchBinding<B> binding);

    @Override public B nextBatch(@Nullable B b) {
        boolean re = false;
        if (lb == null) return null; // already exhausted
        try {
            long startNs = needsStartTime ? Timestamp.nanoTime() : ORIGIN;
            b = getBatch(b);
            do {
                if (inner == null) {
                    if (++leftRow >= lb.rows) {
                        leftRow = 0;
                        lb = left.nextBatch(lb);
                        if (lb == null) break; // reached end
                    }
                    inner = bind(tempBinding.setRow(lb, leftRow));
                    re = true;
                }
                if ((rb = inner.nextBatch(rb)) == null) {
                    inner = null;
                }
                boolean pub = switch (bindType) {
                    case JOIN,EXISTS      -> rb != null;
                    case NOT_EXISTS,MINUS -> rb == null;
                    case LEFT_JOIN        -> { re &= rb == null; yield rb != null || re; }
                };
                if (pub) {
                    switch (bindType) {
                        case JOIN,LEFT_JOIN          ->   b = merger.merge(b, lb, leftRow, rb);
                        case EXISTS,NOT_EXISTS,MINUS -> { b.putRow(lb, leftRow); inner = null; }
                    }
                }
            } while (readyInNanos(b.rows, startNs) > 0);
            if (b.rows == 0) {
                b = handleEmptyBatch(b);
            } else {
                adjustCapacity(b);
                if (metrics != null) metrics.rightRowsReceived(b.rows);
            }
        } catch (Throwable t) {
            lb = null; // signal exhaustion
            onTermination(t);
            throw t;
        }
        return b;
    }

    private B handleEmptyBatch(B batch) {
        batchType.recycle(recycle(batch));
        onTermination(null);
        return null;
    }

    /* --- --- --- toString() --- --- --- */

    protected abstract Object rightUnbound();

    @Override protected String toStringNoArgs() {
        return super.toStringNoArgs()+'['+bindType+']';
    }

    @Override public String toString() {
        return toStringWithOperands(List.of(left, rightUnbound()));
    }
}
