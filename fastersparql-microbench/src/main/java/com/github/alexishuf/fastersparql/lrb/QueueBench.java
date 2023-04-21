package com.github.alexishuf.fastersparql.lrb;


import com.github.alexishuf.fastersparql.batch.CallbackBIt;
import com.github.alexishuf.fastersparql.batch.base.SPSCBIt;
import com.github.alexishuf.fastersparql.batch.operators.SPSCUnitBIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.Rope;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.openjdk.jmh.annotations.*;

import java.util.ArrayDeque;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static com.github.alexishuf.fastersparql.lrb.Workloads.uniformCols;
import static java.lang.System.nanoTime;
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
    private List<Batch> seedInputs;
    private ArrayDeque<List<Batch>> inputsQueue;
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
        seedInputs = uniformCols(Workloads.<Batch>fromName(bt, sizeName), bt);
        System.out.printf("trialSetup(): loaded first invocation batches in %.3fms\n", (nanoTime()-setupStart)/1_000_000.0);
        int cols = seedInputs.get(0).cols;
        vars = new Vars.Mutable(cols);
        for (int i = 0; i < cols; i++)
            vars.add(Rope.of("x", i));
        feederThread = Thread.ofPlatform().name("feeder").start(this::feeder);
        try { Thread.sleep(500); } catch (InterruptedException ignored) {}
        System.out.printf("trialSetup() complete for %d invocations in %.3fs\n", expectedInvocations, (nanoTime()-setupStart)/1_000_000_000.0);
        System.gc();
    }

    @TearDown(Level.Trial) public void trialTearDown() throws InterruptedException {
        stopFeeder = true;
        LockSupport.unpark(feederThread);
        feederThread.join(1_000);
        System.out.printf("Max invocations/iteration: %d\n", maxInvocations);
    }

    @Setup(Level.Iteration) public void setup() {
        warnedNoInputs = false;
        invocations = 0;
        Workloads.repeat(seedInputs, bt, expectedInvocations, inputsQueue);
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
                List<Batch> inputs = inputsQueue.pollFirst();
                if (inputs == null) {
                    if (!warnedNoInputs)
                        System.out.println("NO MORE INPUTS!");
                    warnedNoInputs = true;
                } else {
                    for (Batch b : inputs) {
                        bt.recycle(it.offer(b));
                    }
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
        LockSupport.unpark(this.feederThread);
        int count = 0;
        for (B b = null; (b = it.nextBatch(b)) != null; ) {
            for (int r = 0, rows = b.rows, cols = b.cols; r < rows; r++) {
                for (int c = 0; c < cols; c++) {
                    if (b.termType(r, c) != null) ++count;
                }
            }
        }
        return count;
    }

    @Benchmark public int   unit() { return drain(new SPSCUnitBIt<>(bt, vars)); }
    @Benchmark public int  queue() { return drain(new SPSCBIt<>(bt, vars, 8)); }
    @Benchmark public int queue2() { return drain(new SPSCBIt<>(bt, vars, maxItems)); }
}
