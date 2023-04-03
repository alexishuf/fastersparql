package com.github.alexishuf.fastersparql.operators.bit;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.dedup.Dedup;
import com.github.alexishuf.fastersparql.batch.operators.SPSCUnitBIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.model.BindType;
import com.github.alexishuf.fastersparql.operators.metrics.Metrics;
import com.github.alexishuf.fastersparql.operators.metrics.Metrics.JoinMetrics;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import com.github.alexishuf.fastersparql.operators.plan.Query;
import com.github.alexishuf.fastersparql.operators.plan.Union;
import com.github.alexishuf.fastersparql.sparql.SparqlQuery;
import com.github.alexishuf.fastersparql.sparql.binding.Binding;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;

import static com.github.alexishuf.fastersparql.FSProperties.dedupCapacity;
import static com.github.alexishuf.fastersparql.sparql.SparqlQuery.DistinctType.WEAK;

public class NativeBind {
    private static boolean allNativeOperands(Union right) {
        for (int i = 0, n = right.opCount(); i < n; i++) {
            if (!(right.op(i) instanceof Query q && q.client.usesBindingAwareProtocol()))
                return false;
        }
        return true;
    }

    @SuppressWarnings("GrazieInspection")
    private static <B extends Batch<B>> BIt<B>
    multiBind(Plan join, BIt<B> left, BindType type, Union right, int rightIdx,
              @Nullable Binding binding, boolean canDedup, @Nullable JoinMetrics metrics) {
        //          left
        //    +-------^------+      Scatter-planName VThread copies each batch from left
        //    |              |      to each of the queues
        //    v              v
        // queue[0] ... queue[n-1]
        //    |              |      Each queue is a copy of left and is used as `bindings`
        //    v              v      in SparqlClient.query()
        // binds[0] ... binds[n-1]
        //    +------v-------+
        //           |              All bound BIts are merged (or concatenated) if Union.dispatch
        //           v              will also set up cross-source or full de-duplication.
        //     Union.dispatch()
        var bt = left.batchType();
        var vars = left.vars();
        var nOps = right.opCount();
        var queues = new ArrayList<SPSCUnitBIt<B>>(nOps);
        for (int i = 0; i < nOps; i++) {
            var q = new SPSCUnitBIt<>(bt, vars);
            q.eager().maxBatch(left.maxBatch());
            queues.add(q);
        }

        Thread.startVirtualThread(() -> {
            Thread.currentThread().setName("Scatter-"+join.id());
            Throwable error = null;
            try {
                left.tempEager();
                for (B b = null; (b = left.nextBatch(b)) != null; ) {
                    for (var q : queues) q.copy(b);
                }
            } catch (Throwable t) {
                error = t;
            }
            for (var q : queues) q.complete(error);
        });

        var boundIts = new ArrayList<BIt<B>>(queues.size());

        for (int i = 0; i < nOps; i++) {
            Query query = (Query) right.op(i);
            SparqlQuery sparql = query.sparql;
            if (canDedup)        sparql = sparql.toDistinct(WEAK);
            if (binding != null) sparql = sparql.bound(binding);
            boundIts.add(query.client.query(bt, sparql, queues.get(i), type));
        }
        int cdc = right.crossDedupCapacity;
        int dedupCols = join.publicVars().size();
        Dedup<B> dedup;
        if      (canDedup) dedup = bt.dedupPool.getWeak(nOps*dedupCapacity(), dedupCols);
        else if (cdc >  0) dedup = bt.dedupPool.getWeakCross(nOps*cdc, dedupCols);
        else               return new MeteredMergeBIt<>(boundIts, join, rightIdx, metrics);
        return new DedupMergeBIt<>(boundIts, dedup, join, rightIdx, metrics);
    }

    public static <B extends Batch<B>> BIt<B> preferNative(BatchType<B> batchType, Plan join,
                                                           @Nullable Binding binding, boolean canDedup) {
        BindType type = join.type.bindType();
        if (type == null) throw new IllegalArgumentException("Unsupported: Plan type");
        Metrics metrics = Metrics.createIf(join);
        BIt<B> left = join.op(0).execute(batchType, binding, canDedup);
        for (int i = 1, n = join.opCount(); i < n; i++) {
            var r = join.op(i);
            var jm = metrics == null ? null : metrics.joinMetrics[i];
            if (r instanceof Query q && q.client.usesBindingAwareProtocol()) {
                var sparql = q.query();
                if (binding != null) sparql = sparql.bound(binding);
                if (canDedup)        sparql = sparql.toDistinct(WEAK);
                left = q.client.query(batchType, sparql, left, type, jm);
            } else if (r instanceof Union rUnion && allNativeOperands(rUnion)) {
                left = multiBind(join, left, type, rUnion, i, binding, canDedup, jm);
            } else {
                if (binding != null) r = r.bound(binding);
                left = new PlanBindingBIt<>(left, type, r, canDedup, join.publicVars(), jm);
            }
        }
        return left;
    }
}