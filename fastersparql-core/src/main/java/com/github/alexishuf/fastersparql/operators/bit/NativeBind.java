package com.github.alexishuf.fastersparql.operators.bit;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.BoundedBIt;
import com.github.alexishuf.fastersparql.batch.operators.ShortQueueBIt;
import com.github.alexishuf.fastersparql.model.BindType;
import com.github.alexishuf.fastersparql.model.row.RowType;
import com.github.alexishuf.fastersparql.model.row.dedup.Dedup;
import com.github.alexishuf.fastersparql.model.row.dedup.WeakCrossSourceDedup;
import com.github.alexishuf.fastersparql.model.row.dedup.WeakDedup;
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
    private static <R> BIt<R>
    multiBind(Plan join, BIt<R> left, BindType type, Union right, int rightIdx,
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
        var rt = left.rowType();
        var vars = left.vars();
        var nOps = right.opCount();
        var queues = new ArrayList<ShortQueueBIt<R>>(nOps);
        for (int i = 0; i < nOps; i++)
            queues.add(new ShortQueueBIt<>(rt, vars));

        Thread.startVirtualThread(() -> {
            Thread.currentThread().setName("Scatter-"+join.id());
            if (left instanceof BoundedBIt<R> bounded)
                bounded.maxReadyBatches(8);
            Throwable error = null;
            try {
                for (var b = left.nextBatch(); b.size != 0; b = left.nextBatch(b)) {
                    for (var q : queues) q.copy(b);
                }
            } catch (Throwable t) {
                error = t;
            }
            for (var q : queues) q.complete(error);
        });

        var boundIts = new ArrayList<BIt<R>>(queues.size());

        for (int i = 0; i < nOps; i++) {
            Query query = (Query) right.op(i);
            SparqlQuery sparql = query.sparql;
            if (canDedup)        sparql = sparql.toDistinct(WEAK);
            if (binding != null) sparql = sparql.bound(binding);
            boundIts.add(query.client.query(rt, sparql, queues.get(i), type));
        }
        int cdc = right.crossDedupCapacity;
        Dedup<R> dedup;
        if      (canDedup) dedup = new WeakDedup<>(rt, nOps*dedupCapacity());
        else if (cdc >  0) dedup = new WeakCrossSourceDedup<>(rt, nOps*cdc);
        else               return new MeteredMergeBIt<>(boundIts, join, rightIdx, metrics);
        return new DedupMergeBIt<>(boundIts, dedup, join, rightIdx, metrics);
    }

    public static <R> BIt<R> preferNative(RowType<R> rowType, Plan join,
                                          @Nullable Binding binding,  boolean canDedup) {
        BindType type = join.type.bindType();
        if (type == null) throw new IllegalArgumentException("Unsupported: Plan type");
        Metrics metrics = Metrics.createIf(join);
        BIt<R> left = join.op(0).execute(rowType, binding, canDedup);
        for (int i = 1, n = join.opCount(); i < n; i++) {
            var r = join.op(i);
            var jm = metrics == null ? null : metrics.joinMetrics[i];
            if (r instanceof Query q && q.client.usesBindingAwareProtocol()) {
                var sparql = q.query();
                if (binding != null) sparql = sparql.bound(binding);
                if (canDedup)        sparql = sparql.toDistinct(WEAK);
                left = q.client.query(rowType, sparql, left, type, jm);
            } else if (r instanceof Union rUnion && allNativeOperands(rUnion)) {
                left = multiBind(join, left, type, rUnion, i, binding, canDedup, jm);
            } else {
                if (binding != null) r = r.bound(binding);
                left = new PlanBindingBIt<>(left, type, left.vars(), r, canDedup, join.publicVars(), jm);
            }
        }
        return left;
    }
}
