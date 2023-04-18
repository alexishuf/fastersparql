package com.github.alexishuf.fastersparql.lrb.query;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.model.SparqlResultFormat;
import com.github.alexishuf.fastersparql.model.rope.OutputStreamSink;
import com.github.alexishuf.fastersparql.sparql.SparqlQuery;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import com.github.alexishuf.fastersparql.sparql.results.serializer.ResultsSerializer;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.time.Duration;
import java.util.Arrays;

import static java.lang.Thread.ofPlatform;

public final class QueryRunner {
    private static final Logger log = LoggerFactory.getLogger(QueryRunner.class);

    public abstract static class BatchConsumer{
        public final BatchType<?> batchType;

        public BatchConsumer(BatchType<?> batchType) { this.batchType = batchType; }

        public final BatchType<?> batchType() { return batchType; }

        /** Called when iteration of an iterator starts (before {@link #accept(Batch)}). */
        public abstract void start(BIt<?> it);

        /** Called for every batch from {@link BIt#nextBatch(Batch)}. Ownership of {@code batch}
         * remains with the caller. Thus, a consumer must either copy the batch or completely
         * process it before returning.
         */
        public abstract void accept(Batch<?> batch);

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
     * Run {@code query} against {@code client} and consume all batches with {@code consumer}.
     *
     * @param client a {@link SparqlClient} to be queried. Ownership remains with caller.
     * @param query the query to execute
     * @param consumer {@link BatchConsumer} that will receive start/finish/failed events as well
     *                                  as all batches.
     */
    public static <B extends Batch<B>> void run(SparqlClient client, SparqlQuery query,
                                                BatchConsumer consumer) {
        BIt<B> it = createIt(client, query, consumer);
        if (it != null)
            drain(it, consumer);
    }

    /**
     * {@link #run(SparqlClient, SparqlQuery, BatchConsumer)} enforcing a timeout of
     * {@code timeoutMs} milliseconds.
     */
    public static <B extends Batch<B>> void run(SparqlClient client, SparqlQuery query,
                                                BatchConsumer consumer, long timeoutMs) {
        try {
            BIt<B> it = createIt(client, query, consumer);
            drain(it, consumer, timeoutMs);
        } catch (Throwable t) {
            consumer.finish(t);
        }
    }

    private static <B extends Batch<B>> @Nullable BIt<B>
    createIt(SparqlClient client, SparqlQuery query, BatchConsumer consumer) {
        BIt<B> it = null;
        try {//noinspection unchecked
            it = client.query((BatchType<B>) consumer.batchType(), query);
        } catch (Throwable cause) {
            consumer.finish(cause);
        }
        return it;
    }

    /**
     * Calls {@code consumer.start()}, {@code consumer.accept()} for every non-nul
     * {@code it.nextBatch()} and {@code consumer.finish()}.
     */
    public static <B extends Batch<B>> void drain(BIt<B> it, BatchConsumer consumer) {
        try {
            consumer.start(it);
            for (B b = null; (b = it.nextBatch(b)) != null; )
                consumer.accept(b);
            consumer.finish(null);
        } catch (Throwable cause) {
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
                                                  long timeoutNs) {
        if (it != null) {
            var thread = ofPlatform().name("QueryRunner").start(() -> drain(it, consumer));
            boolean interrupted = false, kill;
            try {
                kill = !thread.join(Duration.ofMillis(timeoutNs));
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



    /** Accumulates all rows in a single batch. */
    public static abstract class Accumulator<B extends Batch<B>> extends BatchConsumer {
        public final B batch;
        public Accumulator(BatchType<B> batchType) {
            super(batchType);
            this.batch = batchType.create(64*BIt.PREFERRED_MIN_BATCH, 3, 0);
        }
        @Override public void start(BIt<?> it) { batch.clear(it.vars().size()); }
        @Override public void accept(Batch<?> batch) {
            //noinspection unchecked
            this.batch.put((B)batch);
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

        @Override public void start(BIt<?> it) {
            serializer.init(it.vars(), it.vars(), it.vars().isEmpty(), sink);
        }
        @Override public void accept(Batch<?> b) { serializer.serialize(b, sink); }
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

        @Override public void start(BIt<?> it) {
            rows = 0;
            Arrays.fill(counts, 0);
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
    }
}
