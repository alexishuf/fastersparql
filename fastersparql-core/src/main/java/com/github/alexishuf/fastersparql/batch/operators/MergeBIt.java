package com.github.alexishuf.fastersparql.batch.operators;

import com.github.alexishuf.fastersparql.FSProperties;
import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.BItReadClosedException;
import com.github.alexishuf.fastersparql.batch.base.SPSCBIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchProcessor;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.operators.metrics.MetricsFeeder;
import com.github.alexishuf.fastersparql.util.ExceptionCondenser;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.invoke.VarHandle;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static java.lang.invoke.MethodHandles.lookup;

public class MergeBIt<B extends Batch<B>> extends SPSCBIt<B> {
    private static final VarHandle N_WAITERS;
    private static final VarHandle ACTIVE_SOURCES;

    static {
        try {
            N_WAITERS = lookup().findVarHandle(MergeBIt.class, "nWaiters", int.class);
            ACTIVE_SOURCES = lookup().findVarHandle(MergeBIt.class, "activeSources", int.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    protected final List<? extends BIt<B>> sources;
    private final Thread[] feedWaiters;
    @SuppressWarnings("unused") // accessed through VarHandles
    private int nWaiters, activeSources;
    //private DebugJournal.RoleJournal journals[];

    public MergeBIt(Collection<? extends BIt<B>> sources, BatchType<B> batchType, Vars vars) {
        this(sources, batchType, vars, null);
    }

    public MergeBIt(Collection<? extends BIt<B>> sources, BatchType<B> batchType,
                    Vars vars, @Nullable MetricsFeeder metrics) {
        this(sources, batchType, vars, metrics, true);
    }
    protected MergeBIt(Collection<? extends BIt<B>> sources, BatchType<B> batchType,
                       Vars vars, @Nullable MetricsFeeder metrics, boolean autoStart) {
        super(batchType, vars, FSProperties.queueMaxRows());
        this.metrics = metrics;
        //noinspection unchecked
        int n = (this.sources = sources instanceof List<?> list
                ? (List<? extends BIt<B>>) list : new ArrayList<>(sources)).size();
        feedWaiters      = new Thread[n];
        N_WAITERS.setRelease(this, 0); // also publishes other fields
        //journals = new DebugJournal.RoleJournal[n];
        if (autoStart)
            start();
    }

    protected final void start() {
        int n = sources.size();
        if ((int)ACTIVE_SOURCES.compareAndExchangeRelease(this, 0, n) != 0)
            return; // previous start()
        if (n == 0) {
            complete(null);
        } else {
//            String parent = getClass().getSimpleName()+"@"+id();
//            var name = new StringBuilder(parent.length()+9+4);
//            name.append(parent).append("-drainer-");
//            int prefixLength = name.length();
//            for (int i = 0; i < n; i++) {
//                final int idx = i;
//                name.append(idx);
//                ofVirtual().name(name.toString()).start(() -> drainTask(idx));
//                name.setLength(prefixLength);
//            }
            for (int i = 0; i < n; i++) {
                final int idx = i;
                Thread.startVirtualThread(() -> drainTask(idx));
            }
        }
    }

    public List<? extends BIt<B>> sources() { return sources; }

    /* --- --- helper methods --- --- --- */

    protected final @Nullable B syncOffer(int source, B batch)
            throws CancelledException, TerminatedException {
        //var journal = journals[source];
        //journal.write("syncOffer: WAIT source=", source, "[0][0]=", batch.get(0, 0));
        feedWaiters[source] = Thread.currentThread();
        //  this thread has exclusive access to offer()
        while ((int) N_WAITERS.getAndAdd(this, 1) != 0)
            LockSupport.park(this);
        try {
            //journal.write("syncOffer: EXCL source=", source, "rows=", batch.rows);
            feedWaiters[source] = null;
            return offer(batch);
        } catch (CancelledException|TerminatedException e) {
            batchType.recycle(batch);
            throw e;
        } finally {
            //journal.write("syncOffer: RLS source=", source);
            N_WAITERS.setVolatile(this, 0); // release mutex
            for (int i = source+1; i != source; i++) { // wake one waiter
                if (i == feedWaiters.length) i = 0;
                if (i == source) break;
                Thread waiter = feedWaiters[i];
                if (waiter != null) {
                    LockSupport.unpark(waiter);
                    break;
                }
            }
        }
    }

    protected @Nullable BatchProcessor<B> createProcessor(int sourceIdx) {
        return batchType.projector(vars, sources.get(sourceIdx).vars());
    }

    private void drainTask(int i) {
        //var journal = journals[i] = DebugJournal.SHARED.role("Merge@"+id()+"."+i);
        BatchProcessor<B> processor = null;
        try (var source = sources.get(i)) {
            //journal.write("drainTask:", source);
            processor = createProcessor(i);
            //if (processor != null) journal.write("drainTask: processor=", processor);
            for (B b = stealRecycled(); (b = source.nextBatch(b)) != null;) {
                if (processor != null) {
                    b = processor.processInPlace(b);
                    if (b == null) break;
                }
                if (b.rows > 0) b = syncOffer(i, b);
            }
            //journal.write("drainTask src=", i, "exhausted");
        } catch (BItReadClosedException e) {
            if (e.it() != sources.get(i)) // ignore if caused by cleanup() calling source.close()
                complete(e);
        } catch (TerminatedException|CancelledException e) {
            if (!isTerminated())
                complete(new Exception("Unexpected "+e.getClass().getSimpleName()));
        } catch (Throwable t) {
            complete(t);
        } finally {
            if ((int)ACTIVE_SOURCES.getAndAdd(this, -1) == 1 && state() == State.ACTIVE)
                complete(null);
            if (processor != null)
                processor.release();
        }
    }

    /* --- --- overrides --- --- --- */

    @Override protected void updatedBatchConstraints() {
        super.updatedBatchConstraints();
        for (BIt<B> source : sources) {
            // maxWait must only be enforced when aggregating multiple Batches smaller than minBatch
            // enforcing maxWait at each source would lead to waiting when a batch could be built
            // by combining two incomplete batches.
            source.minBatch(minBatch).maxBatch(maxBatch)
                  .minWait(minWaitNs, TimeUnit.NANOSECONDS)
                  .maxWait(minWaitNs, TimeUnit.NANOSECONDS);
            if (eager) source.tempEager();
        }
    }

    @Override protected void cleanup(@Nullable Throwable cause) {
        super.cleanup(cause); // calls lock()/unlock(), which makes stopping = true visible to workers
        if ((int)ACTIVE_SOURCES.getAcquire(this) != 0)
            ExceptionCondenser.closeAll(sources);
//        for (DebugJournal.RoleJournal journal : journals) journal.close();
    }

    @Override public String toString() { return toStringWithOperands(sources); }
}
