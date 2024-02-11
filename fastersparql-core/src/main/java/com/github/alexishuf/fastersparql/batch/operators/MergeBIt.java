package com.github.alexishuf.fastersparql.batch.operators;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.BItReadCancelledException;
import com.github.alexishuf.fastersparql.batch.base.SPSCBIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchProcessor;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.operators.metrics.MetricsFeeder;
import com.github.alexishuf.fastersparql.util.ExceptionCondenser;
import com.github.alexishuf.fastersparql.util.StreamNode;
import com.github.alexishuf.fastersparql.util.StreamNodeDOT;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.VarHandle;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.batch.BIt.State.ACTIVE;
import static java.lang.invoke.MethodHandles.lookup;

public class MergeBIt<B extends Batch<B>> extends SPSCBIt<B> {
    private static final Logger log = LoggerFactory.getLogger(MergeBIt.class);
    private static final VarHandle ACTIVE_SOURCES;

    static {
        try {
            ACTIVE_SOURCES = lookup().findVarHandle(MergeBIt.class, "plainActiveSources", int.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    protected final List<? extends BIt<B>> sources;
    private final Condition canOffer = newCondition();
    private boolean offering;
    private final Thread[] drainerThreads;
    @SuppressWarnings("unused") private int plainActiveSources;

    public MergeBIt(Collection<? extends BIt<B>> sources, BatchType<B> batchType, Vars vars) {
        this(sources, batchType, vars, null);
    }

    public MergeBIt(Collection<? extends BIt<B>> sources, BatchType<B> batchType,
                    Vars vars, @Nullable MetricsFeeder metrics) {
        this(sources, batchType, vars, metrics, true);
    }
    protected MergeBIt(Collection<? extends BIt<B>> sources, BatchType<B> batchType,
                       Vars vars, @Nullable MetricsFeeder metrics, boolean autoStart) {
        super(batchType, vars);
        this.metrics = metrics;
        //noinspection unchecked
        int n = (this.sources = sources instanceof List<?> list
                ? (List<? extends BIt<B>>) list : new ArrayList<>(sources)).size();
        drainerThreads = new Thread[n];
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
            for (int i = 0; i < n; i++) {
                final int idx = i;
                drainerThreads[i] = Thread.startVirtualThread(() -> drainTask(idx));
            }
        }
    }

    public List<? extends BIt<B>> sources() { return sources; }

    @Override public Stream<? extends StreamNode> upstreamNodes() {
        return sources.stream();
    }

    /* --- --- helper methods --- --- --- */

    protected @Nullable BatchProcessor<B> createProcessor(int sourceIdx) {
        return batchType.projector(vars, sources.get(sourceIdx).vars());
    }

    private void drainTask(int i) {
        if (MergeBIt.class.desiredAssertionStatus())
            Thread.currentThread().setName(label(StreamNodeDOT.Label.MINIMAL)+'-'+i);
        BatchProcessor<B> processor = null;
        try (var source = sources.get(i)) {
            processor = createProcessor(i);
            for (B b = null; notTerminated() && (b = source.nextBatch(b)) != null; ) {
                if (processor != null && notTerminated()) {
                    b = processor.processInPlace(b);
                    if (b == null) break; // happens when LIMIT is reached
                }
                if (b.rows > 0) b = offer(b);
            }
        } catch (BItReadCancelledException e) {
            if (e.it() != sources.get(i)) // ignore if caused by cleanup() calling source.close()
                complete(e);
        } catch (TerminatedException|CancelledException e) {
            if (notTerminated())
                complete(new Exception("Unexpected "+e.getClass().getSimpleName()));
        } catch (Throwable t) {
            complete(t);
        } finally {
            if ((int)ACTIVE_SOURCES.getAndAdd(this, -1) == 1 && state() == ACTIVE)
                complete(null);
            if (processor != null)
                processor.release();
        }
    }

    public @Nullable B offer(B b) throws CancelledException, TerminatedException {
        boolean locked = lockAndGet();
        try {
            while (offering)
                canOffer.awaitUninterruptibly();
            offering = true;
            locked = unlockAndGet();
            return super.offer(b);
        } finally {
            if (!locked)
                lock();
            offering = false;
            try {
                canOffer.signal();
            } catch (Throwable t) {
                log.error("canOffer.signal() failed on {}", this, t);
            }
            unlock();
        }
    }

    @Override public void copy(B b) throws TerminatedException, CancelledException {
        throw new UnsupportedOperationException();
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
        super.cleanup(cause);
        if ((int)ACTIVE_SOURCES.getAcquire(this) != 0)
            ExceptionCondenser.closeAll(sources);
    }

    @Override public boolean tryCancel() {
        boolean did = super.tryCancel();
        if (did) {
            try {
                for (Thread t : drainerThreads)
                    t.join();
            } catch (InterruptedException e)  {
                log.error("Interrupted while joining drainer threads at tryCancel() for {}", this);
            }
        }
        return did;
    }

    @Override public String toString() { return toStringWithOperands(sources); }
}
