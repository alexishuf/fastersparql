package com.github.alexishuf.fastersparql.lrb.query;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.BItReadCancelledException;
import com.github.alexishuf.fastersparql.batch.BItReadFailedException;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.emit.Emitter;
import com.github.alexishuf.fastersparql.emit.Receiver;
import com.github.alexishuf.fastersparql.emit.ReceiverErrorFuture;
import com.github.alexishuf.fastersparql.exceptions.FSCancelledException;
import com.github.alexishuf.fastersparql.exceptions.RuntimeExecutionException;
import com.github.alexishuf.fastersparql.model.SparqlResultFormat;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.OutputStreamSink;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import com.github.alexishuf.fastersparql.sparql.results.serializer.ResultsSerializer;
import com.github.alexishuf.fastersparql.util.StreamNode;
import com.github.alexishuf.fastersparql.util.owned.*;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.sparql.results.serializer.ResultsSerializer.ignoreChunks;
import static java.lang.Thread.NORM_PRIORITY;

public final class QueryRunner {
    private static final Logger log = LoggerFactory.getLogger(QueryRunner.class);
    private static final ExecutorService drainerService
            = Executors.newCachedThreadPool(new ThreadFactory() {
        private final ThreadGroup group = Thread.currentThread().getThreadGroup();
        private final AtomicInteger nextThreadId = new AtomicInteger();

        @Override public Thread newThread(@NonNull Runnable r) {
            var t = new Thread(group, r, "QueryRunner-"+nextThreadId.getAndIncrement());
            t.setDaemon(true);
            if (t.getPriority() != NORM_PRIORITY)
                t.setPriority(NORM_PRIORITY);
            return t;
        }
    });

    public abstract static class BatchConsumer<B extends Batch<B>, C extends BatchConsumer<B, C>>
            extends AbstractOwned<C>
            implements Receiver<B> {
        protected final BatchType<B> batchType;
        protected @Nullable StreamNode upstreamNode;

        public BatchConsumer(BatchType<B> batchType) {
            this.batchType = batchType;
        }

        @Override public @Nullable C recycle(Object currentOwner) {
            internalMarkGarbage(currentOwner);
            return null;
        }

        public BatchType<B> batchType() { return batchType; }

        /** Called when iteration of an iterator starts (before {@link #onBatch(Orphan)}
         *  and {@link #onBatchByCopy(Batch)}). */
        public void start(Vars vars, @Nullable StreamNode upstreamNode) {
            this.upstreamNode = upstreamNode;
            start0(vars);
        }
        protected abstract void start0(Vars vars);
        @Override public Stream<? extends StreamNode> upstreamNodes() {
            return Stream.ofNullable(upstreamNode);
        }

        @Override public void onComplete()             {finish(null);}
        @Override public void onCancelled()            {finish(new FSCancelledException());}
        @Override public void onError(Throwable cause) {finish(cause);}

        protected abstract void finish(@Nullable Throwable cause);
    }

    /**
     * Calls {@code consumer.start()}, {@code consumer.accept()} for every non-nul
     * {@code it.nextBatch()} and {@code consumer.finish()}.
     */
    public static <B extends Batch<B>> void drain(BIt<B> it, BatchConsumer<B, ?> consumer) {
        boolean finished = false;
        try (var g = new Guard.ItGuard<>(DRAIN, it)) {
            consumer.start(it.vars(), it);
            while (g.advance())
                consumer.onBatch(g.poll());
            finished = true;
            consumer.onComplete();
        } catch (Throwable cause) {
            if (finished)
                throw cause;
            else if (cause instanceof BItReadCancelledException)
                consumer.onCancelled();
            else if (cause instanceof BItReadFailedException e)
                consumer.onError(e.rootCause());
            else
                consumer.onError(cause);
        }
    }
    private static final StaticMethodOwner DRAIN = new StaticMethodOwner("QueryRunner.drain");

    /**
     * Equivalent to {@link #drain(BIt, BatchConsumer)}, but ony checks if {@code it} and
     * {@code consumer} have the same batch type at runtime.
     *
     * @throws IllegalArgumentException if {@code it.batchType() != consumer.batchType}
     */
    @SuppressWarnings("unchecked") public static <B extends Batch<B>>
    void drainWild(BIt<?> it, BatchConsumer<?, ?> consumer) {
        if (!it.batchType().equals(consumer.batchType))
            throw new IllegalArgumentException("it.batchType() != consumer.batchType");
        drain((BIt<B>)it, (BatchConsumer<B, ?>)consumer);
    }

    /**
     * Version of {@link #drain(BIt, BatchConsumer)} that enforces a timeout of {@code timeoutMs}
     * milliseconds
     */
    public static <B extends Batch<B>> void drain(BIt<B> it, BatchConsumer<B, ?> consumer,
                                                  long timeoutMs) {
        if (it != null) {
            java.util.concurrent.Future<?> task = drainerService.submit(() -> drain(it, consumer));
            boolean interrupted = false, cancel;
            try {
                task.get(timeoutMs, TimeUnit.MILLISECONDS);
                cancel = false;
            } catch (InterruptedException e) {
                cancel = interrupted = true;
            } catch (ExecutionException e) {
                log.error("Unexpected {} while draining {} to {}",
                          e.getCause().getClass().getSimpleName(), it, consumer, e);
                cancel = false;
            } catch (TimeoutException e) {
                cancel = true;
            }
            if (cancel) {
                try {
                    it.tryCancel();
                } catch (Throwable t) {
                    log.info("Cancel after timeout on {} failed", it, t);
                    task.cancel(true);
                }
                try {
                    task.get();
                } catch (InterruptedException e) {
                    log.info("Interrupted, leaking post-cancel task for {}", it);
                } catch (ExecutionException t) {
                    log.warn("Unexpected exception by post-cancel task for {}", it, t);
                }
            }
            if (interrupted)
                Thread.currentThread().interrupt();
        }
    }

    /**
     * Equivalent to {@link #drain(BIt, BatchConsumer, long)}, but only checks it
     * {@code it.batchType() == consumer.batchType} at runtime
     *
     * @throws IllegalArgumentException if {@code consumer.batchType != it.batchType()}
     */
    @SuppressWarnings("unchecked")
    public static <B extends Batch<B>> void drainWild(BIt<?> it, BatchConsumer<?, ?> consumer,
                                                      long timeoutMs) {
        if (it.batchType() != consumer.batchType)
            throw new IllegalArgumentException("consumer.batchType != it.batchType");
        drain((BIt<B>)it, (BatchConsumer<B, ?>)consumer, timeoutMs);
    }

    /**
     * Route batches and termination from {@code emitter} to {@code consumer}. This method will
     * only return after a termination event is delivered to {@code consumer}.
     *
     * @param emitter An unstarted emitter
     * @param consumer callback that will receive batches and termination from {@code emitter}
     */
    public static <B extends Batch<B>> void drain(Orphan<? extends Emitter<B, ?>> emitter,
                                                  BatchConsumer<B, ?> consumer) {
        drain(emitter, consumer, Long.MAX_VALUE);
    }

    /**
     * Equivalent to {@link #drain(Orphan, BatchConsumer)}, but checks if {@code emitter} and
     * {@code consumer} have the same batch type only at runtime.
     *
     * @throws IllegalArgumentException if {@code emitter.batchType()} does not equals
     *                                  {@code consumer.batchType()}.
     */
    @SuppressWarnings("unchecked")
    public static <B extends Batch<B>> void drainWild(Orphan<? extends Emitter<?, ?>> emitter,
                                                     BatchConsumer<?, ?> consumer) {
        if (!consumer.batchType.equals(Emitter.peekBatchTypeWild(emitter)))
            throw new IllegalArgumentException("consumer and emitter batch type differ");
        drain((Orphan<? extends Emitter<B,?>>) emitter, (BatchConsumer<B, ?>)consumer);
    }

    /**
     * Similar to {@link #drain(Orphan, BatchConsumer)}, but will {@link Emitter#cancel()}
     * {@code emitter} if it does not terminate within {@code timeoutNs} nanoseconds.
     *
     * @param emitter An unstarted {@link Emitter}
     * @param consumer callback for the batches and the termination of {@code emitter}
     * @param timeoutMs maximum allowed milliseconds for {@code emitter} to terminate
     */
    public static <B extends Batch<B>> void drain(Orphan<? extends Emitter<B, ?>> emitter,
                                                  BatchConsumer<B, ?> consumer,
                                                  long timeoutMs) {
        var future = new DelegateFuture.Concrete<>(consumer).takeOwnership(DRAIN);
        try {
            future.subscribeTo(emitter);
            consumer.start(Emitter.peekVars(emitter), future.upstream());
            future.getSimple(timeoutMs, TimeUnit.MILLISECONDS); // wait
        } catch (RuntimeExecutionException ignored) {
        } catch (TimeoutException t) {
            future.cancel(true);
            Objects.requireNonNull(future).getSimple(); // wait for onCancelled()
        } catch (Throwable t) {
            consumer.onError(t);
        } finally {
            future.recycle(DRAIN);
        }
    }

    /**
     * Equivalent to {@link #drain(Orphan, BatchConsumer, long)}, but accepts a wildcard for
     * {@link Batch} type at compile time and checks if {@code emitter} and {@code consumer}
     * have the same {@link BatchType} at runtime.
     *
     * @throws IllegalArgumentException if {@code emitter.batchType()} does not equals
     *                                  {@code consumer.batchType()}.
     */
    @SuppressWarnings("unchecked") public static <B extends Batch<B>>
    void drainWild(Orphan<? extends Emitter<?, ?>> emitter,
                   BatchConsumer<?, ?> consumer, long timeoutMs) {
        if (!consumer.batchType.equals(Emitter.peekBatchTypeWild(emitter)))
            throw new IllegalArgumentException("mismatched batch types");
        drain((Orphan<? extends Emitter<B, ?>>)emitter, (BatchConsumer<B, ?>)consumer, timeoutMs);
    }

    private static abstract sealed class DelegateFuture<B extends Batch<B>>
            extends ReceiverErrorFuture<B, DelegateFuture<B>> {
        private final BatchConsumer<B, ?> batchConsumer;

        public DelegateFuture(BatchConsumer<B, ?> batchConsumer) {this.batchConsumer = batchConsumer;}

        private static final class Concrete<B extends Batch<B>>
                extends DelegateFuture<B> implements Orphan<DelegateFuture<B>> {
            public Concrete(BatchConsumer<B, ?> batchConsumer) {super(batchConsumer);}
            @Override public DelegateFuture<B> takeOwnership(Object o) {return takeOwnership0(o);}
        }

        public Emitter<B, ?> upstream() { return upstream; }

        @Override public void onBatch(Orphan<B> orphan) {batchConsumer.onBatch(orphan);}
        @Override public void onBatchByCopy(B batch)    {batchConsumer.onBatchByCopy(batch);}

        @Override public void onComplete() {
            try {
                batchConsumer.onComplete();
            } finally {super.onComplete();}
        }

        @Override public void onError(Throwable cause) {
            try {
                batchConsumer.onError(cause);
            } finally {super.onError(cause);}
        }

        @Override public void onCancelled() {
            try {
                batchConsumer.onCancelled();
            } finally {super.onCancelled();}
        }
    }


    /** Accumulates all rows in a single batch. */
    public static class Accumulator<B extends Batch<B>> extends BatchConsumer<B, Accumulator<B>> {
        private B batch;
        private @Nullable Throwable error;

        public static <B extends Batch<B>> Accumulator<B> create(BatchType<B> batchType) {
            return new Concrete<>(batchType);
        }

        protected Accumulator(BatchType<B> batchType) {
            super(batchType);
            this.batch = batchType.create(1).takeOwnership(this);
        }

        private static final class Concrete<B extends Batch<B>> extends Accumulator<B>
                implements Orphan<Accumulator<B>> {
            public Concrete(BatchType<B> batchType) {super(batchType);}
            @Override public Accumulator<B> takeOwnership(Object o) {return takeOwnership0(o);}
        }

        @Override public @Nullable Accumulator<B> recycle(Object currentOwner) {
            batch = Batch.safeRecycle(batch, this);
            return super.recycle(currentOwner);
        }

        public @Nullable Throwable error() {return error;}

        public B get() {
            return batch;
        }

        public Orphan<B> take() {
            B b = batch;
            batch = null;
            return Owned.releaseOwnership(b, this);
        }

        @Override protected void start0(Vars vars) {
            this.batch = batch == null ? batchType.create(vars.size()).takeOwnership(this)
                                       : batch.clear(vars.size());
        }

        @Override protected void finish(@Nullable Throwable cause) {
            this.error = cause;
        }

        @Override public void onBatchByCopy(B batch)   {this.batch.copy(batch);}
        @Override public void onBatch(Orphan<B> batch) {this.batch.append(batch);}
    }

    /** A {@link BatchConsumer} that serializes the query results */
    public static abstract class Serializer<B extends Batch<B>, S extends Serializer<B, S>>
            extends BatchConsumer<B, S> {
        private final OutputStreamSink sink;
        private boolean close;
        private final ResultsSerializer<?> serializer;
        private final ResultsSerializer.Reassemble<B> reassemble;
        private final ResultsSerializer.Recycler<B> recycler;

        protected Serializer(BatchType<B> batchType, OutputStream os,
                             SparqlResultFormat fmt, boolean close) {
            super(batchType);
            this.sink       = new OutputStreamSink(os);
            this.serializer = ResultsSerializer.create(fmt).takeOwnership(this);
            this.close      = close;
            this.reassemble = new ResultsSerializer.Reassemble<>();
            this.recycler   = ResultsSerializer.recycler(batchType);
        }


        @Override public @Nullable S recycle(Object currentOwner) {
            Owned.safeRecycle(serializer, this);
            return super.recycle(currentOwner);
        }

        private static final class Concrete<B extends Batch<B>> extends Serializer<B, Concrete<B>>
                implements Orphan<Concrete<B>> {
            public Concrete(BatchType<B> batchType, OutputStream os, SparqlResultFormat fmt,
                            boolean close) {
                super(batchType, os, fmt, close);
            }
            @Override public Concrete<B> takeOwnership(Object o) {return takeOwnership0(o);}
        }

        public static <B extends Batch<B>> Serializer<B, ?>
        closing(BatchType<B> bt, OutputStream os, SparqlResultFormat fmt) {
            return new Serializer.Concrete<>(bt, os, fmt, true);
        }

        public static <B extends Batch<B>> Serializer<B, ?>
        flushing(BatchType<B> bt, OutputStream os, SparqlResultFormat fmt) {
            return new Serializer.Concrete<>(bt, os, fmt, false);
        }

        public void output(OutputStream os, boolean close) {
            sink.os = os;
            this.close = close;
        }

        @Override protected void start0(Vars vars) {
            serializer.init(vars, vars, vars.isEmpty());
            serializer.serializeHeader(sink);
        }

        @Override public void onBatch(Orphan<B> batch) {
            serializer.serialize(batch, sink, Integer.MAX_VALUE, recycler, ignoreChunks());
        }

        @Override public void onBatchByCopy(B batch) {
            try (var tmpOwner = new TempOwner<>(batch)) {
                Orphan<B> orphan = tmpOwner.releaseOwnership();
                serializer.serialize(orphan, sink, Integer.MAX_VALUE, reassemble, ignoreChunks());
                tmpOwner.restoreOwnership(reassemble.take());
            }
        }

        protected void finish(@Nullable Throwable cause) {
            try {
                if (close) sink.os.close();
                else       sink.os.flush();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /** A {@link BatchConsumer} that simply counts the number of non-null terms. */
    @SuppressWarnings("unused")
    public static abstract class BoundCounter<B extends Batch<B>, C extends BoundCounter<B, C>>
            extends BatchConsumer<B, C> {
        private final int[] counts = new int[5];
        private int rows;

        public BoundCounter(BatchType<B> batchType) { super(batchType); }

        public int  nonNull() { return counts[0] + counts[1] + counts[2] + counts[3]; }
        public int literals() { return counts[Term.Type.LIT  .ordinal()]; }
        public int     iris() { return counts[Term.Type.IRI  .ordinal()]; }
        public int   bNodes() { return counts[Term.Type.BLANK.ordinal()]; }
        public int     vars() { return counts[Term.Type.VAR  .ordinal()]; }
        public int    nulls() { return counts[4]; }

        @Override protected void start0(Vars vars) {
            rows = 0;
            counts[0] = 0;
            counts[1] = 0;
            counts[2] = 0;
            counts[3] = 0;
            counts[4] = 0;
        }

        @Override public void onBatch(Orphan<B> orphan) {
            B b = orphan.takeOwnership(this);
            try {
                this.rows += b.rows;
                for (int r = 0, rows = b.rows, cols = b.cols; r < rows; r++) {
                    for (int c = 0; c < cols; c++) {
                        var type = b.termType(r, c);
                        counts[type == null ? 4 : type.ordinal()]++;
                    }
                }
            } finally {
                b.recycle(this);
            }
        }

        @Override public void onBatchByCopy(B b) {
            this.rows += b.rows;
            for (int r = 0, rows = b.rows, cols = b.cols; r < rows; r++) {
                for (int c = 0; c < cols; c++) {
                    var type = b.termType(r, c);
                    counts[type == null ? 4 : type.ordinal()]++;
                }
            }
        }
    }
}
