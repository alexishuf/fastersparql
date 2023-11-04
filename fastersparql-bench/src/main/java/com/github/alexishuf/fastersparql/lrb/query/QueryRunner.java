package com.github.alexishuf.fastersparql.lrb.query;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.emit.Emitter;
import com.github.alexishuf.fastersparql.emit.Receiver;
import com.github.alexishuf.fastersparql.emit.ReceiverErrorFuture;
import com.github.alexishuf.fastersparql.exceptions.RuntimeExecutionException;
import com.github.alexishuf.fastersparql.model.SparqlResultFormat;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.OutputStreamSink;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import com.github.alexishuf.fastersparql.sparql.results.serializer.ResultsSerializer;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.lang.Thread.ofPlatform;

public final class QueryRunner {
    private static final Logger log = LoggerFactory.getLogger(QueryRunner.class);

    public abstract static class BatchConsumer {
        public final BatchType<?> batchType;

        public BatchConsumer(BatchType<?> batchType) { this.batchType = batchType; }

        public final BatchType<?> batchType() { return batchType; }

        /** Called when iteration of an iterator starts (before {@link #accept(Batch)}). */
        public abstract void start(Vars vars);

        /** Called for every batch from {@link BIt#nextBatch(Batch)}. Ownership of {@code batch}
         * remains with the caller. Thus, a consumer must either copy the batch or completely
         * process it before returning.
         */
        public abstract void accept(Batch<?> batch);

        /**
         * Called for every {@link Receiver#onRow(Batch, int)} from upstream.
         *
         * @param batch a batch
         * @param row the only row in {@code batch} which should be treated as a result row
         */
        public abstract void accept(Batch<?> batch, int row);

        /**
         * Called after {@link BIt#nextBatch(Batch)} returns {@code null}. Or if an error
         * occurs before that
         *
         * @param error {@code null} if the {@link BIt} was exhausted without errors, else
         *              the {@link Throwable} thrown
         */
        public abstract void finish(@Nullable Throwable error);
    }

    /**
     * Calls {@code consumer.start()}, {@code consumer.accept()} for every non-nul
     * {@code it.nextBatch()} and {@code consumer.finish()}.
     */
    public static <B extends Batch<B>> void drain(BIt<B> it, BatchConsumer consumer) {
        boolean finished = false;
        try {
            consumer.start(it.vars());
            for (B b = null; (b = it.nextBatch(b)) != null; )
                consumer.accept(b);
            finished = true;
            consumer.finish(null);
        } catch (Throwable cause) {
            if (finished)
                throw cause;
            else
                consumer.finish(cause);
        } finally {
            try {
                if (it != null) it.close();
            } catch (Throwable t) {
                log.error("Ignoring {} while close()ing {} after successful iteration",
                        t.getClass().getSimpleName(), it, t);
            }
        }
    }

    /**
     * Version of {@link #drain(BIt, BatchConsumer)} that enforces a timeout of {@code timeoutMs}
     * milliseconds
     */
    public static <B extends Batch<B>> void drain(BIt<B> it, BatchConsumer consumer,
                                                  long timeoutMs) {
        if (it != null) {
            var thread = ofPlatform().name("QueryRunner").start(() -> drain(it, consumer));
            boolean interrupted = false, kill;
            try {
                kill = !thread.join(Duration.ofMillis(timeoutMs));
            } catch (InterruptedException e) {
                kill = interrupted = true;
            }
            if (kill) {
                it.close();
                while (true) {
                    try {
                        thread.join();
                        break;
                    } catch (InterruptedException e) { interrupted = true; }
                }
            }
            if (interrupted)
                Thread.currentThread().interrupt();
        }
    }

    /**
     * Route batches and termination from {@code emitter} to {@code consumer}. This method will
     * only return after {@link BatchConsumer#finish(Throwable)}.
     *
     * @param emitter An unstarted emitter
     * @param consumer callback that will receive batches and termination from {@code emitter}
     */
    public static <B extends Batch<B>> void drain(Emitter<B> emitter, BatchConsumer consumer) {
        drain(emitter, consumer, Long.MAX_VALUE);
    }

    /**
     * Similar to {@link #drain(Emitter, BatchConsumer)}, but will {@link Emitter#cancel()}
     * {@code emitter} if it does not terminate within {@code timeoutNs} nanoseconds.
     *
     * @param emitter An unstarted {@link Emitter}
     * @param consumer callback for the batches and the termination of {@code emitter}
     * @param timeoutMs maximum allowed milliseconds for {@code emitter} to terminate
     */
    public static <B extends Batch<B>> void drain(Emitter<B> emitter, BatchConsumer consumer,
                                                  long timeoutMs) {
        try {
            consumer.start(emitter.vars());
        } catch (Throwable t) {
            consumer.finish(t);
            emitter.cancel();
            throw t;
        }
        boolean finished = false;
        ReceiverErrorFuture<B> future = null;
        try {
            future = new ReceiverErrorFuture<>() {
                @Override public @Nullable B onBatch(B batch) {
                    consumer.accept(batch);
                    return batch;
                }
                @Override public void onRow(B batch, int row) {
                    consumer.accept(batch, row);
                }
            };
            future.subscribeTo(emitter);
            var error = future.getSimple(timeoutMs, TimeUnit.MILLISECONDS);
            finished = true;
            consumer.finish(error);
        } catch (RuntimeExecutionException e) {
            if (finished) throw e;
            else          consumer.finish(e.getCause());
        } catch (TimeoutException t) {
            emitter.cancel();
            consumer.finish(Objects.requireNonNull(future).getSimple());
        } catch (Throwable t) {
            if (finished) throw t;
            else          consumer.finish(t);
        }
    }


    /** Accumulates all rows in a single batch. */
    public static abstract class Accumulator<B extends Batch<B>> extends BatchConsumer {
        public B batch;
        public Accumulator(BatchType<B> batchType) {
            super(batchType);
            this.batch = batchType.create(1);
        }
        @Override public void start(Vars vars) { batch = batch.clear(vars.size()); }
        @Override public void accept(Batch<?> batch) {
            //noinspection unchecked
            this.batch.copy((B)batch);
        }
        @Override public void accept(Batch<?> batch, int row) {
            //noinspection unchecked
            this.batch.putRow((B)batch, row);
        }
    }

    /** A {@link BatchConsumer} that serializes the query results */
    public static class Serializer extends BatchConsumer {
        private final OutputStreamSink sink;
        private boolean close;
        private final ResultsSerializer serializer;

        protected Serializer(BatchType<?> batchType, OutputStream os, SparqlResultFormat fmt, boolean close) {
            super(batchType);
            this.sink = new OutputStreamSink(os);
            this.serializer = ResultsSerializer.create(fmt);
            this.close = close;
        }

        public static Serializer closing(BatchType<?> bt, OutputStream os, SparqlResultFormat fmt) {
            return new Serializer(bt, os, fmt, true);
        }

        public static Serializer flushing(BatchType<?> bt, OutputStream os, SparqlResultFormat fmt) {
            return new Serializer(bt, os, fmt, false);
        }

        public void output(OutputStream os, boolean close) {
            sink.os = os;
            this.close = close;
        }

        @Override public void start(Vars vars) {
            serializer.init(vars, vars, vars.isEmpty(), sink);
        }
        @Override public void accept(Batch<?> b) { serializer.serializeAll(b, sink); }
        @Override public void accept(Batch<?> b, int r) { serializer.serialize(b, r, 1, sink); }
        @Override public void finish(@Nullable Throwable error) {
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
    public static abstract class BoundCounter extends BatchConsumer {
        private final int[] counts = new int[5];
        private int rows;

        public BoundCounter(BatchType<?> batchType) { super(batchType); }

        public int  nonNull() { return counts[0] + counts[1] + counts[2] + counts[3]; }
        public int literals() { return counts[Term.Type.LIT  .ordinal()]; }
        public int    irirs() { return counts[Term.Type.IRI  .ordinal()]; }
        public int   bNodes() { return counts[Term.Type.BLANK.ordinal()]; }
        public int     vars() { return counts[Term.Type.VAR  .ordinal()]; }
        public int    nulls() { return counts[4]; }

        @Override public void start(Vars vars) {
            rows = 0;
            counts[0] = 0;
            counts[1] = 0;
            counts[2] = 0;
            counts[3] = 0;
            counts[4] = 0;
        }

        @Override public void accept(Batch<?> b) {
            this.rows += b.rows;
            for (int r = 0, rows = b.rows, cols = b.cols; r < rows; r++) {
                for (int c = 0; c < cols; c++) {
                    var type = b.termType(r, c);
                    counts[type == null ? 4 : type.ordinal()]++;
                }
            }
        }

        @Override public void accept(Batch<?> b, int r) {
            this.rows++;
            for (int c = 0, cols = b.cols; c < cols; c++) {
                var type = b.termType(r, c);
                counts[type == null ? 4 : type.ordinal()]++;
            }
        }
    }
}
