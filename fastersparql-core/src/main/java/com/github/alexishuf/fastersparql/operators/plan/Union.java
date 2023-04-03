package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.dedup.Dedup;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.operators.bit.DedupConcatBIt;
import com.github.alexishuf.fastersparql.operators.bit.DedupMergeBIt;
import com.github.alexishuf.fastersparql.operators.bit.MeteredConcatBIt;
import com.github.alexishuf.fastersparql.operators.bit.MeteredMergeBIt;
import com.github.alexishuf.fastersparql.sparql.binding.Binding;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.Objects;

import static com.github.alexishuf.fastersparql.FSProperties.dedupCapacity;

public final class Union extends Plan {
    public final int crossDedupCapacity;
    Boolean singleEndpoint;

    public Union(int crossDedupCapacity, Plan left, Plan right) {
        super(Operator.UNION);
        this.left = left;
        this.right = right;
        this.crossDedupCapacity = crossDedupCapacity;
    }
    public Union(int crossDedupCapacity, Plan ... operands) {
        super(Operator.UNION);
        replace(operands);
        this.crossDedupCapacity = crossDedupCapacity;
    }

    @Override public Union copy(@Nullable Plan[] ops) {
        if (ops == null) ops = operandsArray;
        if (ops == null) return new Union(crossDedupCapacity, left, right);
        return new Union(crossDedupCapacity, ops);
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
                                               boolean canDedup) {
        var sources = new ArrayList<BIt<B>>(opCount());
        for (int i = 0, n = opCount(); i < n; i++)
            sources.add(op(i).execute(bt, binding, canDedup));
        Dedup<B> dedup = null;
        if (canDedup || crossDedupCapacity > 0) {
            int cap = sources.size() * (crossDedupCapacity > 0 ? crossDedupCapacity
                                                               : dedupCapacity());
            int cols = publicVars().size();
            dedup = canDedup ? bt.dedupPool.getWeak(cap, cols)
                             : bt.dedupPool.getWeakCross(cap, cols);
        }
        if (singleEndpoint()) {
            return dedup == null ? new MeteredConcatBIt<>(sources, this)
                                 : new DedupConcatBIt<>(sources, this, dedup);
        }
        return dedup == null ? new MeteredMergeBIt<>(sources, this)
                             : new DedupMergeBIt<>(sources, this, dedup);
    }

    @Override public boolean equals(Object o) {
        return o instanceof Union u && crossDedupCapacity == u.crossDedupCapacity && super.equals(o);
    }

    @Override public int hashCode() { return Objects.hash(crossDedupCapacity, super.hashCode()); }
}