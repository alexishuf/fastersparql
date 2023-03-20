package com.github.alexishuf.fastersparql.batch.operators;

import com.github.alexishuf.fastersparql.FSProperties;
import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.BItReadClosedException;
import com.github.alexishuf.fastersparql.batch.base.BItCompletedException;
import com.github.alexishuf.fastersparql.batch.base.SPSCBIt;
import com.github.alexishuf.fastersparql.batch.dedup.Dedup;
import com.github.alexishuf.fastersparql.batch.dedup.DedupPool;
import com.github.alexishuf.fastersparql.batch.dedup.WeakCrossSourceDedup;
import com.github.alexishuf.fastersparql.batch.dedup.WeakDedup;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchFilter;
import com.github.alexishuf.fastersparql.batch.type.BatchProcessor;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.util.ExceptionCondenser;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.invoke.VarHandle;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static java.lang.Thread.*;
import static java.lang.invoke.MethodHandles.lookup;

public class MergeBIt<B extends Batch<B>> extends SPSCBIt<B> {
    private static final int FS_FEEDING         = 0x80000000;
    private static final int FS_PARKING         = 0x40000000;
    private static final int FS_FST_PARKED_MASK = 0x0fffffff;
    private static final VarHandle FS;
    private static final VarHandle ACTIVE_SOURCES;

    static {
        try {
            FS = lookup().findVarHandle(MergeBIt.class, "feedState", int.class);
            ACTIVE_SOURCES = lookup().findVarHandle(MergeBIt.class, "activeSources", int.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    protected final List<? extends BIt<B>> sources;
    private final Thread[] feedWaiters;
    @SuppressWarnings({"unused", "FieldMayBeFinal"}) // accessed through VarHandles
    private int feedState, activeSources;


//    private DebugJournal.RoleJournal journals[];

    public MergeBIt(Collection<? extends BIt<B>> sources, BatchType<B> batchType,
                    Vars vars) {
        this(sources, batchType, vars, true);
    }
    public MergeBIt(Collection<? extends BIt<B>> sources, BatchType<B> batchType,
                    Vars vars, int maxBatches) {
        this(sources, batchType, vars, maxBatches, true);
    }
    protected MergeBIt(Collection<? extends BIt<B>> sources, BatchType<B> batchType,
                       Vars vars, boolean autoStart) {
        this(sources, batchType, vars, FSProperties.queueMaxBatches()+sources.size(), autoStart);
    }
    protected MergeBIt(Collection<? extends BIt<B>> sources, BatchType<B> batchType,
                       Vars vars, int maxBatches, boolean autoStart) {
        super(batchType, vars, maxBatches);
        //noinspection unchecked
        int n = (this.sources = sources instanceof List<?> list
                ? (List<? extends BIt<B>>) list : new ArrayList<>(sources)).size();
        if (n > FS_FST_PARKED_MASK)
            throw new IllegalArgumentException("too many sources");
        feedWaiters      = new Thread[n];
        activeSources    = 0;
        FS.setRelease(this, 0); // also publishes other fields
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
            String parent = getClass().getSimpleName()+"@"+id();
            var name = new StringBuilder(parent.length()+9+4);
            name.append(parent).append("-drainer-");
            int prefixLength = name.length();
            for (int i = 0; i < n; i++) {
                final int idx = i;
                name.append(idx);
                ofVirtual().name(name.toString()).start(() -> drainTask(idx));
                name.setLength(prefixLength);
            }
        }
    }

    public List<? extends BIt<B>> sources() { return sources; }

    /* --- --- helper methods --- --- --- */

    private int acquire(int s, int flag) {
        return (int)FS.compareAndExchangeAcquire(this, s&=~flag, s|flag);
    }

    private int release(int s, int clear, int set) {
        int n;
        for (int o; (o=(int)FS.compareAndExchangeRelease(this, s, n=(s&~clear)|set)) != s; s=o)
            onSpinWait();
        return n;
    }

    protected final @Nullable B syncOffer(int source, B batch) {
//        var journal = journals[source];
        int fs;
        Thread me = null;
        while (( (fs=acquire((int)FS.getOpaque(this), FS_FEEDING)) &FS_FEEDING ) != 0) {
            // try locking PARKING, if it fails, retry FEEDING
            if (( (fs=acquire(fs, FS_PARKING)) & FS_PARKING ) != 0) continue;
            // locked PARKING. publish our thread as parked and update lowest-index parked
            int fstParked = Math.min((fs & FS_FST_PARKED_MASK), source);
            try {
                feedWaiters[source] = me == null ? me = currentThread() : me;
                // before we park, we must ensure there is some feeder that will wake us
                if ((fs = acquire(fs, FS_FEEDING) & FS_FEEDING) == 0) { // no feeder
                    feedWaiters[source] = null; // do not self-unpark!
                    break;                      // we are the feeder now
                } // else: there is a feeder working or spinning on PARKING
            } finally {
                fs = release(fs|FS_PARKING, FS_PARKING|FS_FST_PARKED_MASK, fstParked);
            }
//            journal.write("syncOffer: src=", source, "park");
            LockSupport.park();
        }

        try {
            //journals[source].write("syncOffer: src=", source, "b[0][0]=", batch.get(0, 0));
            return offer(batch);
        } finally {
            release(fs|FS_FEEDING, FS_FEEDING, 0);
            for (int e; (fs = (int)FS.compareAndExchangeAcquire(this, e=fs&~FS_PARKING, fs|FS_PARKING)) != e; )
                Thread.onSpinWait();
            try { // locked FS_PARKING
                for (int i = fs&FS_FST_PARKED_MASK; i < feedWaiters.length; i++) {
                    Thread thread = feedWaiters[i];
                    if (thread != null) {
                        //journal.write("syncOffer: unpark i=", i);
                        LockSupport.unpark(thread);
                        feedWaiters[i] = null;
                        break;
                    }
                }
            } finally { //release PARKING and set incremented parked index
                //journal.write("syncOffer: exit");
                release(fs|FS_PARKING, FS_PARKING|FS_FST_PARKED_MASK, 0);
            }
        }
    }

    protected B process(int i, B b, @Nullable BatchProcessor<B> processor) {
        if (processor != null) {
            b = processor.processInPlace(b);
            if (b.rows == 0) return b;
        }
        return syncOffer(i, b);
    }

    protected @Nullable BatchProcessor<B> createProcessor(int sourceIdx) {
        Vars offer = sources.get(sourceIdx).vars();
        if (vars.equals(offer)) return null;
        return batchType.projector(vars, offer);
    }

    private void drainTask(int i) {
        //var journal = journals[i] = DebugJournal.SHARED.role("Merge@"+id()+"."+i);
        BatchProcessor<B> processor = null;
        try (var source = sources.get(i)) {
            //journal.write("drainTask:", source);
            processor = createProcessor(i);
            //if (processor != null) journal.write("drainTask: processor=", processor);
            for (B b = stealRecycled(); (b = source.nextBatch(b)) != null;) {
                //journal.write("drainTask: rows=", b.rows, "&b=", identityHashCode(b),"b[0][0]=", b.get(0, 0));
                if ((b = process(i, b, processor)) == null) {
                    b = stealRecycled();
                    //journal.write("drainTask: &(b=steal)=", identityHashCode(b), "rows", b == null ? 0 : b.rows, "[0][0]=", b==null ? null : b.get(0, 0));
                }
            }
            //journal.write("drainTask src=", i, "exhausted");
        } catch (BItCompletedException e) {
            if (e.it() != this) // ignore if another drainer or close() completed us
                complete(e);
        } catch (BItReadClosedException e) {
            if (e.it() != sources.get(i)) // ignore if caused by cleanup() calling source.close()
                complete(e);
        } catch (Throwable t) {
            complete(t);
        } finally {
            if ((int)ACTIVE_SOURCES.getAndAdd(this, -1) == 1 && !isCompleted())
                complete(null);
            if (processor != null) {
                if (processor instanceof BatchFilter<B> bf && bf.rowFilter instanceof Dedup<B> d) {
                    DedupPool<B> pool = batchType.dedupPool;
                    if (d instanceof WeakCrossSourceDedup<B> cd)
                        pool.offerWeakCross(cd);
                    else if (d instanceof WeakDedup<B> wd)
                        pool.offerWeak(wd);
                }
                processor.release();
            }
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
