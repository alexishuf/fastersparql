package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.dedup.Dedup;
import com.github.alexishuf.fastersparql.batch.dedup.WeakCrossSourceDedup;
import com.github.alexishuf.fastersparql.batch.dedup.WeakDedup;
import com.github.alexishuf.fastersparql.batch.operators.ConcatBIt;
import com.github.alexishuf.fastersparql.batch.operators.MergeBIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.emit.Emitter;
import com.github.alexishuf.fastersparql.emit.async.GatheringEmitter;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.operators.bit.DedupConcatBIt;
import com.github.alexishuf.fastersparql.operators.bit.DedupMergeBIt;
import com.github.alexishuf.fastersparql.operators.metrics.Metrics;
import com.github.alexishuf.fastersparql.sparql.binding.Binding;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Objects;

import static com.github.alexishuf.fastersparql.sparql.DistinctType.WEAK;


public final class Union extends Plan {
    public final boolean crossDedup;
    Boolean singleEndpoint;

    public Union(boolean crossDedup, Plan left, Plan right) {
        super(Operator.UNION);
        this.left = left;
        this.right = right;
        this.crossDedup = crossDedup;
    }
    public Union(boolean crossDedup, Plan ... operands) {
        super(Operator.UNION);
        replace(operands);
        this.crossDedup = crossDedup;
    }

    @Override public Union copy(@Nullable Plan[] ops) {
        if (ops == null) {
            if ((ops = operandsArray) != null) ops = Arrays.copyOf(ops, ops.length);
        }
        if (ops == null)
            return new Union(crossDedup, left, right);
        return new Union(crossDedup, ops);
    }

    public boolean singleEndpoint() {
        if (singleEndpoint != null)
            return singleEndpoint;
        String single = "";
        for (int i = 0, n = opCount(); i < n; i++) {
            if (op(i) instanceof Query q) {
                if (single.isEmpty()) single = q.client().endpoint().uri();
                else if (!single.equals(q.client().endpoint().uri()))
                    return singleEndpoint = Boolean.FALSE;
            }
        }
        return singleEndpoint = Boolean.TRUE;
    }

    @Override
    public <B extends Batch<B>> BIt<B> execute(BatchType<B> bt, @Nullable Binding binding,
                                               boolean weakDedup) {
        var sources = new ArrayList<BIt<B>>(opCount());
        for (int i = 0, n = opCount(); i < n; i++)
            sources.add(op(i).execute(bt, binding, weakDedup));
        Dedup<B> dedup = createDedup(bt, weakDedup);
        Metrics m = Metrics.createIf(this);
        if (singleEndpoint()) {//noinspection resource
            var it = dedup == null ? new ConcatBIt<>(sources, bt, publicVars())
                                   : new DedupConcatBIt<>(sources, publicVars(), dedup);
            return it.metrics(m);
        }
        return dedup == null ? new MergeBIt<>(sources, bt, publicVars(), m)
                             : new DedupMergeBIt<>(sources, publicVars(), m, dedup);
    }

    private <B extends Batch<B>> @Nullable Dedup<B> createDedup(BatchType<B> bt, boolean weak) {
        if (weak || crossDedup) {
            int cs = publicVars().size();
            return weak ? new WeakDedup<>(bt, cs, WEAK) : new WeakCrossSourceDedup<>(bt, cs);
        }
        return null;
    }

    @Override
    public <B extends Batch<B>> Emitter<B> doEmit(BatchType<B> type, Vars rebindHint,
                                                  boolean weakDedup) {
        Vars outVars = publicVars();
        var gather = new GatheringEmitter<>(type, outVars);
        for (int i = 0, n = opCount(); i < n; i++)
            gather.subscribeTo(op(i).emit(type, rebindHint, weakDedup));
        Dedup<B> dedup = createDedup(type, weakDedup);
        if (dedup == null)
            return gather;
        return type.filter(outVars, dedup).subscribeTo(gather);
    }

    @Override public boolean equals(Object o) {
        return o instanceof Union u && crossDedup == u.crossDedup && super.equals(o);
    }

    @Override public int hashCode() { return Objects.hash(crossDedup, super.hashCode()); }
}
