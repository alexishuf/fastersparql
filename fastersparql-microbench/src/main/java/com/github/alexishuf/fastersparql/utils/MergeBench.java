package com.github.alexishuf.fastersparql.utils;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.Timestamp;
import com.github.alexishuf.fastersparql.batch.base.UnitaryBIt;
import com.github.alexishuf.fastersparql.batch.operators.MergeBIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.emit.ReceiverFuture;
import com.github.alexishuf.fastersparql.emit.async.GatheringEmitter;
import com.github.alexishuf.fastersparql.emit.async.TaskEmitter;
import com.github.alexishuf.fastersparql.lrb.cmd.MeasureOptions;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.sparql.binding.BatchBinding;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import com.github.alexishuf.fastersparql.util.concurrent.Async;
import com.github.alexishuf.fastersparql.util.concurrent.PoolCleaner;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.openjdk.jmh.annotations.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.github.alexishuf.fastersparql.emit.async.EmitterService.EMITTER_SVC;

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
        B b = type.create(1);
        for (int r = 0; r < rows; r++) {
            b.beginPut();
            b.putTerm(0, Term.valueOf("\""+(value++)+"\""));
            b.commitPut();
        }
        return b;
    }

    @Setup(Level.Trial) public void trialSetup() {
        System.out.println("Thermal cooldown: 2s...");
        Async.uninterruptibleSleep(2_000);
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
            columns.add(type.create(1));
        Async.uninterruptibleSleep(100); // thermal slack
    }

    @TearDown(Level.Iteration) public void tearDown() {
        PoolCleaner.INSTANCE.sync();
    }

    private static final class SourceBIt<B extends Batch<B>> extends UnitaryBIt<B> {
        private @Nullable B current;
        private int row;
        public SourceBIt(BatchType<B> batchType, @NonNull B source) {
            super(batchType, X);
            this.current = source;
        }

        @Override protected B fetch(B dest) {
            if (current != null && row == current.rows) {
                current = current.next;
                row = 0;
            }
            if (current == null) exhausted = true;
            else                 dest.putRow(current, row++);
            return dest;
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
                for (var n = b; n != null; n = n.next) {
                    for (int r = 0, rows = n.rows; r < rows; r++)
                        h ^= n.hash(r);
                }
            }
        }
        return h;
    }

    @Benchmark
    public <B extends Batch<B>> int emit() {
        BatchType<B> type = (BatchType<B>) this.type;
        var gather = new GatheringEmitter<>(type, X);
        for (Batch<?> source : columns) {
            gather.subscribeTo(new SourceProducer<>(type, (B)source));
        }
        return new BenchReceiver<B>().subscribeTo(gather).getSimple().h;
    }

    private static final class BenchReceiver<B extends Batch<B>>
            extends ReceiverFuture<BenchReceiver<B>, B> {
        public int h;
        @Override public B onBatch(B batch) {
            for (var b = batch; b != null; b = b.next) {
                for (int r = 0, rows = b.rows; r < rows; r++)
                    h ^= b.hash(r);
            }
            return batch;
        }
        @Override public void onComplete() { complete(this); }
    }


    private static final class SourceProducer<B extends Batch<B>> extends TaskEmitter<B> {
        private static final int TICK_CHECK = 0x1f;
        private @Nullable B current;
        private int row;

        public SourceProducer(BatchType<B> type, @NonNull B source) {
            super(type, X, EMITTER_SVC, RR_WORKER, CREATED, TASK_EMITTER_FLAGS);
            this.current = source;
        }

        @Override protected int produceAndDeliver(int state) {
            int r = row;
            var curr = current;
            if (curr == null)
                return COMPLETED;
            B b = bt.createForThread(threadId, 1);
            int end = r+(int)Math.min(curr.rows-r, (long)REQUESTED.getOpaque(this));
            long deadline = Timestamp.nextTick(1);
            while (r < end) {
                b.putRow(curr, r++);
                if ((r&TICK_CHECK) == TICK_CHECK && Timestamp.nanoTime() > deadline)
                    break;
            }
            row = r;
            bt.recycleForThread(threadId, deliver(b));
            if (r >= curr.rows) {
                this.current = curr.next;
                return COMPLETED;
            }
            return state|MUST_AWAKE;
        }

        @Override public void rebind(BatchBinding binding) {
            throw new UnsupportedOperationException();
        }

        @Override public Vars bindableVars() { return Vars.EMPTY; }
    }


}
