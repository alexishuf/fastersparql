package com.github.alexishuf.fastersparql.batch.operators;

import com.github.alexishuf.fastersparql.batch.*;
import com.github.alexishuf.fastersparql.batch.BatchQueue.CancelledException;
import com.github.alexishuf.fastersparql.batch.BatchQueue.TerminatedException;
import com.github.alexishuf.fastersparql.batch.base.SPSCBIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.emit.EmitterStats;
import com.github.alexishuf.fastersparql.exceptions.FSCancelledException;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.util.SafeCloseable;
import com.github.alexishuf.fastersparql.util.StreamNode;
import com.github.alexishuf.fastersparql.util.StreamNodeDOT;
import com.github.alexishuf.fastersparql.util.owned.Guard;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.time.Duration;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal.journal;

public class ScatterBIt<B extends Batch<B>> implements SafeCloseable, Runnable, StreamNode {
    private static final Logger log = LoggerFactory.getLogger(ScatterBIt.class);
    private static final VarHandle CANCELLED;
    static {
        try {
            CANCELLED = MethodHandles.lookup().findVarHandle(ScatterBIt.class, "plainCancelled", int.class);
        } catch (NoSuchFieldException|IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }
    private final BIt<B> upstream;
    private final Thread drainer;
    private final ConsumerQueue<B>[] queues;
    @SuppressWarnings("unused") private int plainCancelled;
    private boolean started;
    private final @Nullable EmitterStats stats = EmitterStats.createIfEnabled();

    public static final class ConsumerQueue<B extends Batch<B>> extends SPSCBIt<B> {
        private final ScatterBIt<B> parent;
        private boolean cancelled;

        private ConsumerQueue(ScatterBIt<B> parent, BatchType<B> batchType, Vars vars,
                              int maxItems) {
            super(batchType, vars, maxItems);
            this.parent = parent;
        }

        @Override public Stream<? extends StreamNode> upstreamNodes() {
            return Stream.of(parent);
        }

        @Override public boolean tryCancel() {
            boolean first = super.tryCancel(); // cancel this ConsumerQueue, will not affect upstream
            if (cancelled)
                return false; // previous tryCancel()
            lock();
            try {
                if (!cancelled) {
                    cancelled = true;
                    if ((int)CANCELLED.getAndAddRelease(parent, 1) == parent.queues.length-1)
                        parent.upstream.tryCancel();
                    return first; // can be false if completed before tryCancel()
                }
            } finally { unlock(); }
            return false; // another thread on tryCancel() cancelled before this one
        }

        @Override public void close() {
            super.close();
            try {
                parent.drainer.join();
            } catch (InterruptedException e) {
                journal(e, "during close on", this);
                log.error("Interrupted during drainer.join() on close() for {}", this);
            }
        }
    }

    public ScatterBIt(BIt<B> upstream, int nQueues, int maxQueueSize) {
        this.upstream = upstream;
        //noinspection unchecked
        this.queues = new ConsumerQueue[nQueues];
        BatchType<B> type = upstream.batchType();
        Vars vars = upstream.vars();
        for (int i = 0; i < queues.length; i++)
            queues[i] = new ConsumerQueue<>(this, type, vars, maxQueueSize);
        this.drainer = Thread.ofVirtual().unstarted(this);
    }

    public ConsumerQueue<B> consumer(int idx) { return queues[idx]; }

    public void start() {
        if (started) return;
        started = true;
        drainer.start();
    }

    @Override public void close() {
        // cancel all consumers, this will also trigger upstream.tryCancel()
        for (var q : queues)
            q.tryCancel();
        try {
            if (!drainer.join(Duration.ofSeconds(2))) {
                journal("timeout joining drainer from close on", this);
                log.info("Timeout joining drainer from close on {}", this);
            }
        } catch (InterruptedException e) {
            journal(e, "during close on", this);
            log.warn("InterruptedException during close() on {}", this);
        }
    }

    @Override public void run() {
        if (Thread.currentThread() != drainer)
            throw new IllegalStateException("Not called from drainer thread");
        drainer.setName(journalName());
        int lastIdx = queues.length-1;
        var last = queues[lastIdx];
        Throwable cause = null;
        try (var g = new Guard.ItGuard<>(this, upstream)) {
            // drain upstream
            for (B b; (b = g.nextBatch()) != null; ) {
                if (EmitterStats.ENABLED && stats != null)
                    stats.onBatchPassThrough(b);
                //copy b to all consumers, except last
                for (int i = 0; i < lastIdx; i++) {
                    var q = queues[i];
                    try {
                        if (!q.cancelled) q.copy(b);
                    } catch (CancelledException ignored) {
                    } catch (TerminatedException e) { handleTerminatedDuringOffer(e, q); }
                }
                // deliver b by reference to the last consumer
                try {
                    if (!last.cancelled) last.offer(g.take());
                } catch (CancelledException ignored) {
                } catch (TerminatedException e) { handleTerminatedDuringOffer(e, last); }
            }
        } catch (Throwable e) {
            if (e instanceof BItReadFailedException rfe) {
                cause = rfe.getCause();
            } else {
                cause = e;
                upstream.tryCancel(); // stop upstream, in cases e was not throw from nextBatch()
            }
        } finally {
            boolean cancelled = cause instanceof BItReadCancelledException;
            FSCancelledException surprise = null;
            for (ConsumerQueue<B> q : queues) {
                Throwable qCause;
                if (!q.cancelled && cancelled)
                    qCause = surprise == null ? surprise=new FSCancelledException() : surprise;
                else if (cancelled)
                    qCause = BItCancelledException.get(q);
                else
                    qCause = cause;
                q.complete(qCause);
            }
        }
    }

    private static <B extends Batch<B>> void handleTerminatedDuringOffer(BatchQueue.QueueStateException e, ConsumerQueue<B> q) {
        journal("Unexpected", e, "during offer to", q);
        log.warn("Unexpected {} at {} during offer()",
                 e.getClass().getSimpleName(), q);
        q.tryCancel(); // will safely mark cancelled
    }

    @Override public Stream<? extends StreamNode> upstreamNodes() {
        return Stream.of(upstream);
    }

    @Override public String journalName() {
        return "Scatter:"+upstream.journalName();
    }

    @Override public String label(StreamNodeDOT.Label type) {
        var sb = new StringBuilder().append("Scatter");
        if (type.showState())
            sb.append("[cancelled=").append((int)CANCELLED.getAcquire(this)).append(']');
        sb.append(':').append(upstream.label(StreamNodeDOT.Label.MINIMAL));
        if (EmitterStats.ENABLED && stats != null && type.showStats())
            stats.appendToLabel(sb);
        return sb.toString();
    }

    @Override public String toString() {
        return label(StreamNodeDOT.Label.WITH_STATE);
    }
}
