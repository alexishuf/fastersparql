package com.github.alexishuf.fastersparql.batch.operators;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.EmptyBIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.client.ItBindQuery;
import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import com.github.alexishuf.fastersparql.util.concurrent.Timestamp;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;

import java.util.ArrayList;

import static com.github.alexishuf.fastersparql.batch.type.Batch.peekRows;

public class GroupBindingBIt<B extends Batch<B>> extends AbstractFlatMapBIt<B> {

    private GroupBind<B> gb;
    private final BIt<B> left;
    private final BIt<B> empty;
    private final ArrayList<SparqlClient.Guard> guards;


    public GroupBindingBIt(Orphan<GroupBind<B>> gbOrphan) {
        super(GroupBind.resultVars(gbOrphan), EmptyBIt.of(GroupBind.batchType(gbOrphan)));
        this.gb     = gbOrphan.takeOwnership(this);
        this.left   = ((ItBindQuery<B>) gb.bindQuery).bindings;
        this.empty  = inner;
        this.guards = BindingBIt.GUARDS_ALLOC.create();
        gb.addGuards(guards);
    }

    @Override protected void cleanup(@Nullable Throwable cause) {
        super.cleanup(cause);
        if (guards != null) {
            for (SparqlClient.Guard g : guards)
                try {
                    g.close();
                } catch (Throwable t) {
                    reportCleanupError(t);
                }
        }
        gb = gb.recycle(this);
    }

    @Override public @This BIt<B> tempEager() {
        eager = true; // do not make inner eager
        updatedBatchConstraints();
        return this;
    }

    @Override public @Nullable Orphan<B> nextBatch(@Nullable Orphan<B> offer) {
        Orphan<B> out = null, nlb = null;
        var gb = this.gb;
        lock();
        try {
            long start = needsStartTime ? Timestamp.nanoTime() : Timestamp.ORIGIN;
            int rows = 0;
            do {
                if (plainState.isTerminated())
                    break;
                if (inner == empty) {
                    Plan plan = gb.startNextGroup();
                    if (plan == null) {
                        unlock();
                        try {
                            nlb = left.nextBatch(null);
                        } finally {lock();}
                        if (nlb != null && !plainState.isTerminated())
                            nlb = gb.enqueueLeftBatch(nlb);
                        else
                            break; // left exhausted or this cancelled
                        if ((plan = gb.startNextGroup()) == null)
                            continue; // dead branch
                    }
                    inner = plan.execute(batchType);
                }
                Orphan<B> orphan = gb.processRightBatch(inner.nextBatch(offer));
                offer = null;
                if (orphan == null) {
                    inner = empty; // inner exhausted
                    out   = gb.endCurrentGroup();
                } else if (Batch.peekRows(orphan) == 0) {
                    offer = orphan; // no results produced (e.g., negation)
                } else if (out == null) {
                    out = orphan; // results produced
                } else {
                    out = appendToOrphan(out, orphan); // cold, minBatch > out.rows
                }
                offer = null; // offer given to inner
            } while (readyInNanos((rows = peekRows(out)), start) > 0);
            if (rows == 0) {
                if (out != null) out = Orphan.safeRecycle(out);
                onTermination(null);
            }
            Orphan<B> ret = onNextBatch(out);
            out = null;
            return ret;
        } catch (Throwable t) {
            onTermination(t);
            throw t;
        } finally {
            unlock();
            if (offer != null) Orphan.safeRecycle(offer);
            if (out   != null) Orphan.safeRecycle(out);
            if (nlb   != null) Orphan.safeRecycle(nlb);
        }
    }

    private Orphan<B> appendToOrphan(Orphan<B> out, Orphan<B> orphan) {
        B tmp = out.takeOwnership(this);
        tmp.append(orphan);
        return tmp.releaseOwnership(this);
    }
}
