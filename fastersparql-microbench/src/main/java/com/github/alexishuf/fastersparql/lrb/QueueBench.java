package com.github.alexishuf.fastersparql.lrb;


import com.github.alexishuf.fastersparql.batch.BatchQueue;
import com.github.alexishuf.fastersparql.batch.CallbackBIt;
import com.github.alexishuf.fastersparql.batch.base.SPSCBIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.RopeFactory;
import com.github.alexishuf.fastersparql.util.concurrent.Unparker;
import com.github.alexishuf.fastersparql.util.owned.Guard;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.openjdk.jmh.annotations.*;

import java.util.ArrayDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static com.github.alexishuf.fastersparql.util.concurrent.Timestamp.nanoTime;
import static java.util.Objects.requireNonNull;

@SuppressWarnings({"unchecked", "rawtypes"})
@State(Scope.Thread)
@Threads(1)
@Fork(value = 1, warmups = 0, jvmArgsPrepend = {"--enable-preview", "--add-modules", "jdk.incubator.vector"})
@Measurement(time = 50, iterations = 5, timeUnit = TimeUnit.MILLISECONDS)
@Warmup(iterations = 1, time = 50, timeUnit = TimeUnit.MILLISECONDS)
@BenchmarkMode({Mode.AverageTime})
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class QueueBench {
    @Param({"COMPRESSED"}) private String typeName;
    @Param({"ALL"})        private String sizeName;
    @Param({"100"})        private int    expectedInvocations;
    @Param({"16384"})      private int    maxItems;

    BatchType bt;
    private Vars vars;
    private Batch<?> seedInputs;
    private ArrayDeque<Batch> inputsQueue;
    private volatile @Nullable CallbackBIt it;
    private volatile boolean stopFeeder = false;
    private boolean warnedNoInputs;
    private Thread feederThread;
    private int invocations, maxInvocations;

    @Setup(Level.Trial) public void trialSetup() {
        stopFeeder = false;
        bt = Workloads.parseBatchType(typeName);
        inputsQueue = new ArrayDeque<>(expectedInvocations);
        long setupStart = nanoTime();
        System.out.println("trialSetup(): loading first invocation batches...");
        seedInputs = Workloads.fromName(bt, sizeName).takeOwnership(this);
        System.out.printf("trialSetup(): loaded first invocation batches in %.3fms\n", (nanoTime()-setupStart)/1_000_000.0);
        int cols = seedInputs.cols;
        vars = new Vars.Mutable(cols);
        for (int i = 0; i < cols; i++)
            vars.add(RopeFactory.make(4).add('x').add(i).take());
        feederThread = Thread.ofPlatform().name("feeder").start(this::feeder);
        try { Thread.sleep(500); } catch (InterruptedException ignored) {}
        System.out.printf("trialSetup() complete for %d invocations in %.3fs\n", expectedInvocations, (nanoTime()-setupStart)/1_000_000_000.0);
        System.gc();
    }

    @TearDown(Level.Trial) public void trialTearDown() throws InterruptedException {
        seedInputs.recycle(this);
        stopFeeder = true;
        Unparker.unpark(feederThread);
        feederThread.join(1_000);
        System.out.printf("Max invocations/iteration: %d\n", maxInvocations);
    }

    @Setup(Level.Iteration) public void setup() {
        warnedNoInputs = false;
        invocations = 0;
        Workloads.<Batch>repeat(seedInputs, expectedInvocations, inputsQueue, this);
        System.gc();
        vars = Workloads.makeVars(requireNonNull(inputsQueue.peekFirst()));
        invocations = 0;
        feederThread = Thread.ofVirtual().name("feeder").start(this::feeder);
        Workloads.cooldown(500);
    }

    @TearDown(Level.Iteration) public void tearDown() {
        System.gc();
        maxInvocations = Math.max(maxInvocations, invocations);
    }

    private void feeder() {
        while (!stopFeeder) {
            while (!stopFeeder && it == null)
                LockSupport.park(this);
            var it = this.it;
            if (it == null) continue;
            ++invocations;
            try {
                Batch batch = inputsQueue.pollFirst();
                if (batch == null) {
                    if (!warnedNoInputs)
                        System.out.println("NO MORE INPUTS!");
                    warnedNoInputs = true;
                } else {
                    try {
                        it.offer(batch.releaseOwnership(this));
                    } catch (BatchQueue.QueueStateException ignored) {}
                }
                it.complete(null);
            } catch (Throwable t) {
                it.complete(t);
            } finally {
                this.it = null;
            }
        }
    }
    private <B extends Batch<B>>int drain(CallbackBIt<B> it) {
        it.maxReadyItems(maxItems);
        while (this.it != null) Thread.onSpinWait();
        this.it = it;
        Unparker.unpark(this.feederThread);
        int count = 0;
        try (var g = new Guard.BatchGuard<B>(this)) {
            for (B b; (b = g.nextBatch(it)) != null; ) {
                for (var n = b; n != null; n = n.next) {
                    for (int r = 0, rows = n.rows, cols = n.cols; r < rows; r++) {
                        for (int c = 0; c < cols; c++) {
                            if (n.termType(r, c) != null) ++count;
                        }
                    }
                }
            }
        }
        return count;
    }

    @Benchmark public int  queue() { return drain(new SPSCBIt<>(bt, vars, 8)); }
    @Benchmark public int queue2() { return drain(new SPSCBIt<>(bt, vars, maxItems)); }
}
