package com.github.alexishuf.fastersparql.operators.bind;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.BoundedBIt;
import com.github.alexishuf.fastersparql.batch.operators.ShortQueueBIt;
import com.github.alexishuf.fastersparql.client.BindType;
import com.github.alexishuf.fastersparql.operators.FSOps;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import com.github.alexishuf.fastersparql.operators.plan.Query;
import com.github.alexishuf.fastersparql.operators.plan.Union;

import java.util.ArrayList;

import static com.github.alexishuf.fastersparql.operators.FSOpsProperties.dedupCapacity;
import static com.github.alexishuf.fastersparql.sparql.SparqlQuery.DistinctType.WEAK;

public class NativeBind {
    private static boolean allNativeOperands(Plan<?, ?> right) {
        for (Plan<?, ?> o : right.operands) {
            if (!(o instanceof Query<?,?> q && q.client().usesBindingAwareProtocol()))
                return false;
        }
        return true;
    }

    @SuppressWarnings("GrazieInspection")
    private static <R, I> BIt<R>
    multiBind(Plan<R, I> join, BIt<R> left, BindType type, Union<R, I> right, boolean canDedup) {
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
        var rClass = left.elementClass();
        var vars = left.vars();
        var operands = right.operands;
        var nOperands = operands.size();
        var queues = new ArrayList<ShortQueueBIt<R>>(nOperands);
        for (int i = 0; i < nOperands; i++)
            queues.add(new ShortQueueBIt<>(rClass, vars));

        Thread.ofVirtual().name("Scatter-"+ join.name).start(() -> {
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

        var binds = new ArrayList<BIt<R>>(queues.size());
        for (int i = 0; i < nOperands; i++) {
            Query<R, ?> q = (Query<R, I>) operands.get(i);
            var s = canDedup ? q.sparql().toDistinct(WEAK) : q.sparql();
            binds.add(q.client().query(s, queues.get(i), type));
        }

        boolean crossDedup = right.crossDedupCapacity > 0;
        int dedupCapacity = crossDedup ? right.crossDedupCapacity : dedupCapacity();
        return Union.dispatch(binds, canDedup, crossDedup, false, dedupCapacity, join);
    }

    public static <R, I> BIt<R> preferNative(Plan<R, I> join, boolean canDedup) {
        BindType type = FSOps.bindTypeOf(join);
        if (type == null)
            throw new IllegalArgumentException("Unsupported: join.getClass()="+join.getClass());
        var r = join.operands().get(1);
        var left = join.operands().get(0).execute(canDedup);
        if (r instanceof Query<R, I> q && q.client().usesBindingAwareProtocol()) {
            var sparql = canDedup ? q.sparql().toDistinct(WEAK) : q.sparql();
            return q.client().query(sparql, left, type);
        } else if (r instanceof Union<R, I> rUnion && allNativeOperands(r)) {
            return multiBind(join, left, type, rUnion, canDedup);
        } else {
            return new PlanBindingBIt<>(left, type, join.rowType, left.vars(), r,
                                        canDedup, join.publicVars());
        }
    }
}
