package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.FSProperties;
import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.model.row.RowType;
import com.github.alexishuf.fastersparql.model.row.dedup.Dedup;
import com.github.alexishuf.fastersparql.model.row.dedup.WeakCrossSourceDedup;
import com.github.alexishuf.fastersparql.model.row.dedup.WeakDedup;
import com.github.alexishuf.fastersparql.operators.bit.DedupConcatBIt;
import com.github.alexishuf.fastersparql.operators.bit.DedupMergeBIt;
import com.github.alexishuf.fastersparql.operators.bit.MeteredConcatBIt;
import com.github.alexishuf.fastersparql.operators.bit.MeteredMergeBIt;
import com.github.alexishuf.fastersparql.sparql.binding.Binding;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

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
    public <R> BIt<R> execute(RowType<R> rt, @Nullable Binding binding, boolean canDedup) {
        var sources = new ArrayList<BIt<R>>(opCount());
        for (int i = 0, n = opCount(); i < n; i++) {
            sources.add(op(i).execute(rt, binding, canDedup));
        }
        int dedupCapacity = crossDedupCapacity > 0 ? crossDedupCapacity
                          : FSProperties.dedupCapacity();
        return dispatch(sources, canDedup, crossDedupCapacity > 0,
                        singleEndpoint(), dedupCapacity, this);
    }

    private static <R> BIt<R> dispatch(List<BIt<R>> sources, boolean canDedup,
                                      boolean crossDedup, boolean sequential,
                                      int dedupCapacity, Union plan) {
        RowType<R> rt = sources.get(0).rowType();
        int totalCapacity = sources.size() * dedupCapacity;
        Dedup<R> dedup = null;
        if (canDedup)
            dedup = new WeakDedup<>(rt, totalCapacity);
        else if (crossDedup)
            dedup = new WeakCrossSourceDedup<>(rt, totalCapacity);
        if (sequential) {
            return dedup == null ? new MeteredConcatBIt<>(sources, plan)
                                 : new DedupConcatBIt<>(sources, plan, dedup);
        }
        return dedup == null ? new MeteredMergeBIt<>(sources, plan)
                             : new DedupMergeBIt<>(sources, plan, dedup);
    }

    @Override public boolean equals(Object o) {
        return o instanceof Union u && crossDedupCapacity == u.crossDedupCapacity && super.equals(o);
    }

    @Override public int hashCode() {
        return Objects.hash(crossDedupCapacity, super.hashCode());
    }
}
