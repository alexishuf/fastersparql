package com.github.alexishuf.fastersparql.operators.bit;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.dedup.Dedup;
import com.github.alexishuf.fastersparql.batch.dedup.WeakCrossSourceDedup;
import com.github.alexishuf.fastersparql.batch.dedup.WeakDedup;
import com.github.alexishuf.fastersparql.batch.operators.MergeBIt;
import com.github.alexishuf.fastersparql.batch.operators.ProcessorBIt;
import com.github.alexishuf.fastersparql.batch.operators.ScatterBIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.client.EmitBindQuery;
import com.github.alexishuf.fastersparql.client.ItBindQuery;
import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.emit.Emitter;
import com.github.alexishuf.fastersparql.emit.Emitters;
import com.github.alexishuf.fastersparql.emit.async.GatheringEmitter;
import com.github.alexishuf.fastersparql.emit.async.ScatterStage;
import com.github.alexishuf.fastersparql.emit.stages.BindingStage;
import com.github.alexishuf.fastersparql.model.BindType;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.operators.metrics.Metrics;
import com.github.alexishuf.fastersparql.operators.metrics.Metrics.JoinMetrics;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import com.github.alexishuf.fastersparql.operators.plan.Query;
import com.github.alexishuf.fastersparql.operators.plan.TriplePattern;
import com.github.alexishuf.fastersparql.operators.plan.Union;
import com.github.alexishuf.fastersparql.sparql.SparqlQuery;
import com.github.alexishuf.fastersparql.sparql.binding.Binding;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;

import static com.github.alexishuf.fastersparql.sparql.DistinctType.WEAK;

public class NativeBind {
    private static boolean allNativeOperands(Union right) {
        for (int i = 0, n = right.opCount(); i < n; i++) {
            if (!(right.op(i) instanceof Query q && q.client.usesBindingAwareProtocol()))
                return false;
        }
        return true;
    }

    public static <B extends Batch<B>> BIt<B>
    multiBind(BIt<B> left, BindType type, Vars outVars, Union right,
              @Nullable Binding binding, boolean weakDedup,
              @Nullable JoinMetrics metrics, SparqlClient clientForAlgebra) {
        var bt       = left.batchType();
        int maxRows  = bt.preferredRowsPerBatch(left.vars());
        int nOps     = right.opCount();
        var scatter  = new ScatterBIt<>(left, nOps, maxRows);
        var boundIts = new ArrayList<BIt<B>>(nOps);
        for (int i = 0; i < nOps; i++) {
            var queue = scatter.consumer(i);
            Plan operand = right.op(i);
            SparqlClient client;
            SparqlQuery sparql;
            if (operand instanceof Query query) {
                sparql = query.sparql;
                client = query.client;
            } else {
                sparql = operand;
                client = clientForAlgebra;
            }
            if (weakDedup && !(client.isLocalInProcess() && sparql instanceof TriplePattern))
                sparql = sparql.toDistinct(client.cheapestDistinct());
            if (binding != null)
                sparql = sparql.bound(binding);
            boundIts.add(client.query(new ItBindQuery<>(sparql, queue, type)));
        }
        // If something goes wrong really fast, scatter could remove items from queues while
        // this tread is iterating it to fill boundIts.
        scatter.start();
        boolean crossDedup = right.crossDedup;
        int dedupCols = outVars.size();
        Dedup<B> dedup;
        if      ( weakDedup) dedup = new WeakDedup<>(bt, dedupCols, WEAK);
        else if (crossDedup) dedup = new WeakCrossSourceDedup<>(bt, dedupCols);
        else                 return new MergeBIt<>(boundIts, bt, outVars, metrics);
        return new DedupMergeBIt<>(boundIts, outVars, metrics, dedup);
    }

    public static <B extends Batch<B>> BIt<B> preferNative(BatchType<B> batchType, Plan join,
                                                           @Nullable Binding binding,
                                                           boolean weakDedup) {
        BindType type = join.type.bindType();
        if (type == null) throw new IllegalArgumentException("Unsupported: Plan type");
        Metrics metrics = Metrics.createIf(join);
        BIt<B> left = join.op(0).execute(batchType, binding, weakDedup);
        for (int i = 1, n = join.opCount(); i < n; i++) {
            var r = join.op(i);
            if (r instanceof Union u && u.right == null) r = u.left();
            var jm = metrics == null ? null : metrics.joinMetrics[i];
            var projection = i == n-1 ? join.publicVars()
                    : type.resultVars(left.vars(), r.publicVars());
            if (r instanceof Query q && q.client.usesBindingAwareProtocol()) {
                var sparql = q.query();
                if (binding != null) sparql = sparql.bound(binding);
                if (weakDedup)       sparql = sparql.toDistinct(q.client.cheapestDistinct());
                left = q.client.query(new ItBindQuery<>(sparql, left, type, jm));
            } else if (r instanceof Union rUnion && allNativeOperands(rUnion)) {
                left = multiBind(left, type, projection, rUnion, binding, weakDedup, jm, null);
            } else {
                if (binding != null) r = r.bound(binding);
                left = new PlanBindingBIt<>(new ItBindQuery<>(r, left, type, jm), weakDedup, projection);
            }
        }
        // if the join has a projection (due to reordering, not due to outer Modifier)
        // a sequence of native joins might not match that projection, thus we must project.
        // for non-native joins, the last join already honors Join.projection.
        return ProcessorBIt.project(join.publicVars(), left);
    }

    public static <B extends Batch<B>> Emitter<B>
    multiBindEmit(Emitter<B> left, BindType type, Vars outVars, Union right,
                  Vars rebindHints, boolean weakDedup, SparqlClient clientForAlgebra) {
        var bt = left.batchType();
        var scatter = new ScatterStage<>(left);
        var gather = new GatheringEmitter<>(left.batchType(), outVars);
        for (int i = 0, n = right.opCount(); i < n; i++) {
            Plan operand = right.op(i);
            SparqlClient client;
            SparqlQuery sparql;
            if (operand instanceof Query query) {
                sparql = query.sparql;
                client = query.client;
            } else {
                sparql = operand;
                client = clientForAlgebra;
            }
            if (weakDedup && !(client.isLocalInProcess() && sparql instanceof TriplePattern))
                sparql = sparql.toDistinct(client.cheapestDistinct());
            var conn = scatter.createConnector();
            var bind = client.emit(new EmitBindQuery<>(sparql, conn, type), rebindHints);
            gather.subscribeTo(bind);
        }

        boolean crossDedup = right.crossDedup;
        int dedupCols = outVars.size();
        Dedup<B> dedup;
        if      ( weakDedup) dedup = new WeakDedup<>(bt, dedupCols, WEAK);
        else if (crossDedup) dedup = new WeakCrossSourceDedup<>(bt, dedupCols);
        else                return gather;
        return bt.filter(outVars, dedup).subscribeTo(gather);
    }

    public static <B extends Batch<B>> Emitter<B>
    preferNativeEmit(BatchType<B> bType, Plan join, Vars rebindHint, boolean weakDedup) {
        BindType type = join.type.bindType();
        if (type == null) throw new IllegalArgumentException("Unsupported: Plan type");
        Emitter<B> left = join.op(0).emit(bType, rebindHint, weakDedup);
        for (int i = 1, n = join.opCount(); i < n; i++) {
            var r = join.op(i);
            if (r instanceof Union u && u.right == null) r = u.left();
            var projection = i == n-1 ? join.publicVars()
                    : type.resultVars(left.vars(), r.publicVars());
            if (r instanceof Query q && q.client.usesBindingAwareProtocol()) {
                var sparql = q.query();
                if (weakDedup)        sparql = sparql.toDistinct(q.client.cheapestDistinct());
                left = q.client.emit(new EmitBindQuery<>(sparql, left, type), rebindHint);
            } else if (r instanceof Union rUnion && allNativeOperands(rUnion)) {
                left = multiBindEmit(left, type, projection, rUnion, rebindHint,
                                     weakDedup, null);
            } else {
                left = new BindingStage<>(new EmitBindQuery<>(r, left, type), rebindHint,
                                          weakDedup, projection);
            }
        }
        // if the join has a projection (due to reordering, not due to outer Modifier)
        // a sequence of native joins might not match that projection, thus we must project.
        // for non-native joins, the last join already honors Join.projection.
        return Emitters.withVars(join.publicVars(), left);
    }

}
