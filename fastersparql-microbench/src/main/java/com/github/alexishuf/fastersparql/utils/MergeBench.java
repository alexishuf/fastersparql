package com.github.alexishuf.fastersparql.utils;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.Timestamp;
import com.github.alexishuf.fastersparql.batch.base.UnitaryBIt;
import com.github.alexishuf.fastersparql.batch.operators.MergeBIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.emit.Receiver;
import com.github.alexishuf.fastersparql.emit.async.AsyncEmitter;
import com.github.alexishuf.fastersparql.emit.async.ProducerTask;
import com.github.alexishuf.fastersparql.lrb.cmd.MeasureOptions;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import com.github.alexishuf.fastersparql.util.concurrent.Async;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.openjdk.jmh.annotations.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static com.github.alexishuf.fastersparql.emit.async.RecurringTaskRunner.TASK_RUNNER;

@SuppressWarnings("unchecked")
@State(Scope.Thread)
@Threads(1)
@Fork(value = 3, warmups = 0, jvmArgsPrepend = {"--enable-preview", "--add-modules", "jdk.incubator.vector"})
@Measurement(iterations = 10, time = 100, timeUnit = TimeUnit.MILLISECONDS)
@Warmup(iterations = 10, time = 100, timeUnit = TimeUnit.MILLISECONDS)
@BenchmarkMode({Mode.AverageTime})
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class MergeBench {

    @Param({"1000"}) private int tallHeight;
    @Param({"10"}) private int shortHeight;
    @Param({"1"}) private int nTall;
    @Param({"3"}) private int nShort;
    @Param({"10"}) private int nEmpty;
    @Param({"COMPRESSED"}) private MeasureOptions.BatchKind batchKind;

    private static final Vars X = Vars.of("x");
    private BatchType<?> type;
    private List<Batch<?>> columns;

    private <B extends Batch<B>> B makeBatch(int rows, int value) {
        BatchType<B> type = (BatchType<B>) batchKind.asType();
        B b = type.create(rows, 1, 0);
        for (int r = 0; r < rows; r++) {
            b.beginPut();
            b.putTerm(0, Term.valueOf("\""+(value++)+"\""));
            b.commitPut();
        }
        return b;
    }

    @Setup(Level.Trial) public void trialSetup() {
        System.out.println("Thermal cooldown: 5s...");
        Async.uninterruptibleSleep(5_000);
    }

    @SuppressWarnings("unused") @Setup(Level.Iteration) public <B extends Batch<B>> void setup() {
        type = batchKind.asType();
        this.columns = new ArrayList<>();
        int next = 0;
        for (int i = 0; i < nShort; i++, next += shortHeight)
            columns.add(makeBatch(shortHeight, next));
        for (int i = 0; i < nTall; i++, next += tallHeight)
            columns.add(makeBatch(tallHeight, next));
        for (int i = 0; i < nEmpty; i++, next += tallHeight)
            columns.add(type.create(1, 1, 0));
        Async.uninterruptibleSleep(100); // thermal slack
    }

    private static final class SourceBIt<B extends Batch<B>> extends UnitaryBIt<B> {
        private final B source;
        private int row;
        public SourceBIt(BatchType<B> batchType, B source) {
            super(batchType, X);
            this.source = source;
        }

        @Override protected boolean fetch(B dest) {
            if (row == source.rows) return false;
            dest.putRow(source, row++);
            return true;
        }
    }

    @Benchmark
    public <B extends Batch<B>> int bit() {
        BatchType<B> type = (BatchType<B>) this.type;
        ArrayList<BIt<B>> sources = new ArrayList<>(columns.size());
        for (var source : columns)
            sources.add(new SourceBIt<>(type, (B)source));
        int h = 0;
        try (var it = new MergeBIt<>(sources, type, X)) {
            for (B b = null; (b = it.nextBatch(b)) != null; ) {
                for (int r = 0, rows = b.rows; r < rows; r++)
                    h ^= b.hash(r);
            }
        }
        return h;
    }

    @Benchmark
    public <B extends Batch<B>> int emit() {
        BatchType<B> type = (BatchType<B>) this.type;
        var ae = new AsyncEmitter<>(type, X, TASK_RUNNER);
        for (Batch<?> source : columns)
            ae.registerProducer(new SourceProducer<>(ae, type, (B) source));
        var receiver = new BenchReceiver<B>();
        ae.subscribe(receiver);
        ae.request(Long.MAX_VALUE);
        return receiver.await();
    }

    private static final class BenchReceiver<B extends Batch<B>> implements Receiver<B> {
        int h;
        volatile boolean terminated;
        Throwable error;
        Thread waiter = Thread.currentThread();

        public int await() {
            while (!terminated)
                LockSupport.parkNanos(this, 1_000_000_000);
            if (error != null) throw new RuntimeException(error);
            return h;
        }

        @Override public B onBatch(B batch) {
            for (int r = 0, rows = batch.rows; r < rows; r++)
                h ^= batch.hash(r);
            return batch;
        }

        @Override public void onComplete() {
            this.terminated = true;
            this.error = null;
            LockSupport.unpark(waiter);
        }

        @Override public void onCancelled() {
            this.terminated = true;
            this.error = AsyncEmitter.CancelledException.INSTANCE;
            LockSupport.unpark(waiter);
        }

        @Override public void onError(Throwable cause) {
            this.terminated = true;
            this.error = cause;
            LockSupport.unpark(this.waiter);
        }
    }


    private static final class SourceProducer<B extends Batch<B>> extends ProducerTask<B> {
        private final B source;
        private final BatchType<B> type;
        private int row;
        public SourceProducer(AsyncEmitter<B> emitter, BatchType<B> type, B source) {
            super(emitter, TASK_RUNNER);
            this.type = type;
            this.source = source;
        }

        @Override protected @Nullable B produce(long limit, long deadline, @Nullable B dest) {
//            int n = (int) Math.min(limit, source.rows - row);
//            if (dest == null) dest = type.create(n, 1, 0);
//            for (int k, r = row; n > 0; row = r += k, n -= k) {
//                k = Math.min(4, n);
//                for (int i = 0; i < k; i++) dest.putRow(source, r+i);
//                if (Timestamp.nanoTime() > deadline) break;
//            }
//            return dest;

            int r = row, end = (int)Math.min(r+limit, source.rows);
            if (dest == null) dest = type.create(end-r, 1, 0);
            while (r != end) {
                dest.putRow(source, r++);
                if (Timestamp.nanoTime() > deadline) break;
            }
            row = r;
            return dest;
        }

        @Override protected boolean exhausted() {
            return row >= source.rows;
        }

        @Override protected void cleanup(@Nullable Throwable reason) {}
    }


}
