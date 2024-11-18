/* unconventional package because JMH profilers are hardcoded to genera aux result dirs using
* the FQCN, which increases the change the resulting directory exceeds maximum name or maximum
* path length limits of the underlying filesystem. */
package fastersparql;

import com.github.alexishuf.fastersparql.FS;
import com.github.alexishuf.fastersparql.FSProperties;
import com.github.alexishuf.fastersparql.FlowModel;
import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.batch.type.ScopedIdBatchType;
import com.github.alexishuf.fastersparql.client.SubqueriesStats;
import com.github.alexishuf.fastersparql.client.netty.NettySparqlServer;
import com.github.alexishuf.fastersparql.client.netty.util.SharedEventLoopGroupHolder;
import com.github.alexishuf.fastersparql.emit.Emitter;
import com.github.alexishuf.fastersparql.emit.EmitterStats;
import com.github.alexishuf.fastersparql.lrb.BenchmarkEvent;
import com.github.alexishuf.fastersparql.lrb.cmd.MeasureOptions.BatchKind;
import com.github.alexishuf.fastersparql.lrb.cmd.QueryOptions;
import com.github.alexishuf.fastersparql.lrb.query.PlanRegistry;
import com.github.alexishuf.fastersparql.lrb.query.QueryName;
import com.github.alexishuf.fastersparql.lrb.query.QueryRunner;
import com.github.alexishuf.fastersparql.lrb.query.QueryRunner.BatchConsumer;
import com.github.alexishuf.fastersparql.lrb.sources.FederationHandle;
import com.github.alexishuf.fastersparql.lrb.sources.LrbSource;
import com.github.alexishuf.fastersparql.lrb.sources.SelectorKind;
import com.github.alexishuf.fastersparql.lrb.sources.SourceKind;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.TwoSegmentRope;
import com.github.alexishuf.fastersparql.operators.metrics.Metrics;
import com.github.alexishuf.fastersparql.operators.metrics.MetricsListener;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import com.github.alexishuf.fastersparql.sparql.expr.TermView;
import com.github.alexishuf.fastersparql.sparql.parser.SparqlParser;
import com.github.alexishuf.fastersparql.util.IOUtils;
import com.github.alexishuf.fastersparql.util.OOMHandler;
import com.github.alexishuf.fastersparql.util.StreamNode;
import com.github.alexishuf.fastersparql.util.concurrent.*;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import com.github.alexishuf.fastersparql.util.owned.Owned;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.profile.AsyncProfiler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.LockSupport;
import java.util.function.IntSupplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.github.alexishuf.fastersparql.FSProperties.OP_CROSS_DEDUP;
import static com.github.alexishuf.fastersparql.FSProperties.OP_WEAKEN_DISTINCT;
import static com.github.alexishuf.fastersparql.util.ExceptionCondenser.throwAsUnchecked;
import static com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal.journal;
import static java.lang.System.setProperty;
import static java.util.concurrent.TimeUnit.*;

@State(Scope.Thread)
@Threads(1)
@Fork(value = 3, warmups = 0, jvmArgsPrepend = {"--enable-preview", "--add-modules", "jdk.incubator.vector"})
@Measurement(iterations = 10, time = 500, timeUnit = MILLISECONDS)
@Warmup(iterations = 3, time = 2_000, timeUnit = MILLISECONDS)
@BenchmarkMode({Mode.AverageTime})
@OutputTimeUnit(MILLISECONDS)
public class QueryBench {
    private static final Logger log = LoggerFactory.getLogger(QueryBench.class);

    @Param({"S.*"}) private String queries;

    @Param({"FS_STORE", "HDT_FILE"}) SourceKind srcKind;
    @Param({"COMPRESSED"}) BatchKind batchKind;
    @Param({"ITERATE", "EMIT"}) FlowModel flowModel;
    @Param({"false"}) boolean unionSource;
    /* The following @Params are hardcoded to stop the name of the aux results dir,
     * where profilers started by JMH store their results having a name that exceeds
     * filesystem maximum file (or path) length. */
//    @Param({"PREFERRED"}) SelectorKindType selKind;
//    @Param({"true"}) boolean builtinPlans;
//    @Param({"true", "false"}) boolean crossSourceDedup;
//    @Param({"false", "true"}) boolean weakenDistinct;
//    @Param({"true"}) boolean thermalCooldown;
    SelectorKindType selKind = SelectorKindType.PREFERRED;
    boolean builtinPlans = true;
    boolean crossSourceDedup = true;
    boolean weakenDistinct = true;
    boolean thermalCooldown = true;
//    @Param({"false","true"}) boolean alt;

    public enum SelectorKindType {
        PREFERRED,
        ASK,
        DICT;

        public SelectorKind forSource(SourceKind src) {
            return switch (this) {
                case ASK -> SelectorKind.ASK;
                case DICT -> SelectorKind.DICT;
                case PREFERRED -> src.isFsStore()&&!src.isServer()
                                ? SelectorKind.FS_STORE : SelectorKind.ASK;
            };
        }
    }

    private BatchType<?> batchType;
    private FederationHandle fedHandle;
    private List<Plan> plans;
    private List<QueryName> queryList;
    private BoundCounter<?> boundCounter;
    private RowCounter<?> rowCounter;
    private RopeLenCounter<?> ropeLenCounter;
    private TermLenCounter<?> termLenCounter;
    private int iterationNumber = 0;
    private long iterationStart, lastIterationMs;
    private final MetricsConsumer metricsConsumer = new MetricsConsumer();
    private int lastBenchResult;
    private final BenchmarkEvent jfrEvent = new BenchmarkEvent();
    private Blackhole bh;
    private int drainTimeoutMs;
    private long forkDeadline;
    private int forkTimeoutSecs;
    private boolean skip;
    private long nonSkippedMeasurementIterations;
    private long invocations, resultRows;
    private AsyncProfiler.JavaApi apInstance;

    private static class BoundCounter<B extends Batch<B>>
            extends QueryRunner.BoundCounter<B, BoundCounter<B>>
            implements Orphan<BoundCounter<B>> {
        public BoundCounter(BatchType<B> batchType) { super(batchType); }
        @Override public void finish(@Nullable Throwable error) { throwAsUnchecked(error); }
        @Override public BoundCounter<B> takeOwnership(Object newOwner) {return takeOwnership0(newOwner);}
    }

    private final class RowCounter<B extends Batch<B>>
            extends BatchConsumer<B, RowCounter<B>>
            implements Orphan<RowCounter<B>> {
        public int rows;
        public RowCounter(BatchType<B> batchType) {super(batchType);}
        public int rows() { return rows; }
        @Override protected void start0(Vars vars) {rows = 0;}

        @Override public RowCounter<B> takeOwnership(Object o) {return takeOwnership0(o);}

        @Override public void onBatch(Orphan<B> orphan) {
            if (orphan == null) return;
            B b = orphan.takeOwnership(this);
            rows += b.totalRows();
            b.recycle(this);
        }

        @Override public void onBatchByCopy(B batch) {
            rows += batch == null ? 0 : batch.totalRows();
        }

        @Override public void finish(@Nullable Throwable error) {
            if (EmitterStats.GLOBAL_ENABLED)
                resultRows += rows;
            throwAsUnchecked(error);
        }
    }

    private final class RopeLenCounter<B extends Batch<B>>
            extends BatchConsumer<B, RopeLenCounter<B>> implements Orphan<RopeLenCounter<B>> {
        private final TwoSegmentRope tmp = new TwoSegmentRope();
        private int acc;
        private long accRows;

        public RopeLenCounter(BatchType<B> batchType) {super(batchType);}

        @Override public RopeLenCounter<B> takeOwnership(Object o) {return takeOwnership0(o);}

        public int len() { return acc; }

        @Override protected void start0(Vars vars) {
            acc = 0;
            accRows = 0;
        }
        @Override public void finish(@Nullable Throwable error) {
            if (EmitterStats.GLOBAL_ENABLED)
                resultRows += accRows;
            throwAsUnchecked(error);
        }

        @Override public void onBatch(Orphan<B> orphan) {
            if (orphan == null) return;
            B b = orphan.takeOwnership(this);
            onBatchByCopy(b);
            b.recycle(this);
        }

        @Override public void onBatchByCopy(B batch) {
            for (var b = batch; b != null; b = b.next) {
                int rows = batch.rows;
                if (EmitterStats.GLOBAL_ENABLED)
                    accRows += rows;
                for (int r = 0, cols = b.cols; r < rows; r++) {
                    for (int c = 0; c < cols; c++) {
                        if (b.getRopeView(r, c, tmp))
                            acc += tmp.len;
                    }
                }
            }
        }
    }

    private final class TermLenCounter<B extends Batch<B>>
            extends BatchConsumer<B, TermLenCounter<B>> implements Orphan<TermLenCounter<B>> {
        private final TermView tmp = new TermView();
        private int acc;
        private long accRows;

        public TermLenCounter(BatchType<B> batchType) {super(batchType);}
        @Override public TermLenCounter<B> takeOwnership(Object o) {return takeOwnership0(o);}
        public int len() { return acc; }

        @Override public void start0(Vars vars) {
            acc = 0;
            accRows = 0;
        }
        @Override public void finish(@Nullable Throwable error) {
            if (EmitterStats.GLOBAL_ENABLED)
                resultRows += accRows;
            throwAsUnchecked(error);
        }

        @Override public void onBatch(Orphan<B> orphan) {
            if (orphan == null) return;
            B b = orphan.takeOwnership(this);
            onBatchByCopy(b);
            b.recycle(this);
        }

        @Override public void onBatchByCopy(B batch) {
            for (var b = batch; b != null; b = b.next) {
                int rows = batch.rows;
                if (EmitterStats.GLOBAL_ENABLED)
                    accRows += rows;
                for (int r = 0, cols = batch.cols; r < rows; r++) {
                    for (int c = 0; c < cols; c++) {
                        if (batch.getView(r, c, tmp))
                            acc += tmp.len;
                    }
                }
            }
        }
    }

    private final class MetricsConsumer implements MetricsListener {
        @Override public void accept(Metrics metrics) {
            if (metrics.error != null)
                throw new RuntimeException(metrics.error);
            bh.consume(metrics.rows);
            bh.consume(metrics.firstRowNanos);
            bh.consume(metrics.allRowsNanos);
            bh.consume(metrics.rows);
        }
    }

    private final class StopAsyncProfiler implements Runnable {
        private volatile boolean stopped = false;
        @Override public void run() {
            if (stopped)
                return;
            stopped = true;
            try {
                if (apInstance != null)
                    apInstance.execute("stop");
            } catch (IOException e) {
                System.err.println("IOException during async-profiler stop");
            }
        }
    }

    @Setup(Level.Trial) public void trialSetup(BenchmarkParams params) throws IOException {
        if (srcKind.isNoNative())
            System.setProperty(FSProperties.SAME_SOURCE_IDS, "false");
        if (batchKind == BatchKind.TERM_NI)
            System.setProperty(FSProperties.BATCH_NO_INTERN_IRI, "true");
        String forkTimeoutStr = System.getProperty("fastersparql.fork-timeout-secs");
        if (forkTimeoutStr == null || forkTimeoutStr.isEmpty()) // typo:
            forkTimeoutStr = System.getProperty("fastersparql.fastersparql.fork-timeout-secs");
        if (forkTimeoutStr != null && !forkTimeoutStr.isEmpty())
            forkTimeoutSecs = Integer.parseInt(forkTimeoutStr);
        else
            forkTimeoutSecs = Integer.MAX_VALUE;
        OOMHandler.exitOnOOM(23);
        OOMHandler.onOOM(StopAsyncProfiler.class.getSimpleName(), new StopAsyncProfiler());
        // comunica enforces its own timeout
        setProperty("fastersparql.comunica.timeout-secs",
                    String.valueOf(params.getTimeout().convertTo(SECONDS)));
//        SegmentRope.ALT = alt;
        setProperty(OP_CROSS_DEDUP, String.valueOf(crossSourceDedup));
        setProperty(OP_WEAKEN_DISTINCT, Boolean.toString(weakenDistinct));
        FSProperties.refresh();
        String dataDirPath = System.getProperty("fs.data.dir");
        if (dataDirPath == null || dataDirPath.isEmpty())
            throw new IllegalArgumentException("fs.data.dir property not set!");
        Path dataDir = Path.of(dataDirPath);
        if (!Files.isDirectory(dataDir))
            throw new IllegalArgumentException("-Dfs.data.dir="+dataDir+" is not a dir");
        Set<LrbSource> sources;
        if (unionSource) {
            sources = Set.of(LrbSource.LargeRDFBench_all);
            if (!srcKind.isFedX()) {
                String filename = LrbSource.LargeRDFBench_all.filename(srcKind);
                if (!Files.exists(dataDir.resolve(filename)))
                    throw new IOException("Missing " + filename + " from " + dataDir);
            }
        } else {
            sources = LrbSource.all();
            String missingSources = sources.stream()
                    .map(s -> s.filename(srcKind))
                    .filter(name -> !Files.exists(dataDir.resolve(name)))
                    .collect(Collectors.joining(", "));
            if (!missingSources.isEmpty())
                throw new IOException("Files/dirs missing from "+dataDir+": "+missingSources);
        }
        queryList = new QueryOptions(List.of(queries)).queries();
        if (queryList.isEmpty())
            throw new IllegalArgumentException("No queries selected");
        batchType = batchKind.asType(srcKind, this);
        boundCounter   = new BoundCounter<>  (batchType);
        rowCounter     = new RowCounter<>    (batchType);
        ropeLenCounter = new RopeLenCounter<>(batchType);
        termLenCounter = new TermLenCounter<>(batchType);
        jfrEvent.className = getClass().getName();
        File dataDirFile = dataDir.toFile();

        // -------------

        journal("trialSetup: building federation, dataDir=", dataDir);
        fedHandle = FederationHandle.builder(dataDirFile)
                .srcKind(srcKind)
                .subset(sources)
                .selKind(selKind.forSource(srcKind))
                .waitInit(true).create();
        setProperty(OP_CROSS_DEDUP, String.valueOf(crossSourceDedup));
        FSProperties.refresh();
        journal("trialSetup: builtinPlans=", builtinPlans ? 1 : 0);
        if (builtinPlans || unionSource) {
            PlanRegistry reg;
            if (unionSource) {
                var name2sparql = srcKind.isComunica()
                                ? PlanRegistry.NO_NULL_VAR_PROJECTION
                                : PlanRegistry.RAW_SPARQL;
                reg = PlanRegistry.unionSource(name2sparql);
            } else {
                reg = PlanRegistry.parseBuiltin();
            }
            reg.resolve(fedHandle.federation);
            plans = queryList.stream().map(reg::createPlan).toList();
            String missingPlans = IntStream.range(0, plans.size())
                    .filter(i -> plans.get(i) == null)
                    .mapToObj(i -> queryList.get(i).name()).collect(Collectors.joining(", "));
            if (!missingPlans.isEmpty())
                throw new IllegalArgumentException("No plan for "+missingPlans);
        } else {
            plans = queryList.stream().map(q -> {
                Plan parsed = q.parsed();
                return fedHandle.federation.plan(parsed);
            }).toList();
        }
        for (Plan plan : plans)
            plan.attach(metricsConsumer);
        triggerClassLoadingOfPooled();

        benchmarkId = params.id();
        watchdog = new Thread(this::watchdog, "watchdog"+(int)WATCHDOG_NEXT_THREAD_ID.getAndAdd(1));
        watchdog.start();
    }

    private void triggerClassLoadingOfPooled() {
        if (ArrayAlloc.longsAtLeast(0) != ArrayAlloc.EMPTY_LONG)
            throw new AssertionError("longsAtLeast(0) != EMPTY_LONG");
        if (Bytes.atLeast(16).takeOwnership(this).recycle(this) != null)
            throw new AssertionError("Full pool too early");
        if (SparqlParser.create().takeOwnership(this).recycle(this) != null)
            throw new AssertionError("Full pool too early");
        Primer.primeAll();
    }

    @TearDown(Level.Trial) public void trialTearDown() {
        if (EmitterStats.GLOBAL_ENABLED) {
            System.err.printf("""
                Stats for %,d invocations in %,d non-skipped measurement iterations:
                +-----------|--------------------|--------------------+--------------------+
                |           |            Batches |               Rows |  Intermediary Rows |
                | Received  | %,18.3f | %,18.3f | %,18.3f |
                | Delivered | %,18.3f | %,18.3f | %,18.3f |
                +-----------|--------------------|--------------------+--------------------+
                Subqueries sent: %,9f (%,.3f KiB)
                Bindings sent: %,.3f KiB
                Responses received: %,.3f KiB
                """, invocations, nonSkippedMeasurementIterations,
                    EmitterStats.globalBatchesReceived()/(double)invocations,
                    EmitterStats.globalRowsReceived()/(double)invocations,
                    EmitterStats.globalRowsReceived()/(double)invocations,
                    EmitterStats.globalBatchesDelivered()/(double)invocations,
                    EmitterStats.globalRowsDelivered()/(double)invocations,
                    (EmitterStats.globalRowsDelivered()-resultRows)/(double)invocations,
                    SubqueriesStats.subqueriesSent()/(double)invocations,
                    SubqueriesStats.subqueriesBytesSent()/1024.0/invocations,
                    SubqueriesStats.bindingsBytes()/1024.0/invocations,
                    SubqueriesStats.responseBytes()/1024.0/invocations
            );
        }
        journal("trialTearDown");
        //watchdog.interrupt();
        //try {
            //watchdog.join(Duration.ofSeconds(1));
        //} catch (InterruptedException ignored) {}
        fedHandle.close();
        if (batchType instanceof Owned<?> o)
            batchType = Owned.safeRecycle(o, this);
        SharedEventLoopGroupHolder.get().shutdownNowIfPossible(Integer.MAX_VALUE, SECONDS);
        NettySparqlServer.ACCEPT_ELG.shutdownNowIfPossible(Integer.MAX_VALUE, SECONDS);
        FS.shutdown();
        journal("trialTearDown: exit");
    }

    @Setup(Level.Iteration) public void iterationSetup(BenchmarkParams opts) throws IOException {
        journal("iterationSetup", iterationNumber);
        lastBenchResult = -1;
        BackgroundTasks.sync();
        IOUtils.fsync(500_000); // generous timeout because there should be no I/O

        var wOpts = opts.getWarmup();
        var mOpts = opts.getMeasurement();
        boolean warmup  = iterationNumber < wOpts.getCount();
        boolean first = iterationNumber == wOpts.getCount();
        var itOpts = warmup ? wOpts : mOpts;
        long estimateMs = itOpts.getTime().convertTo(MILLISECONDS);
        if (!first && lastIterationMs > estimateMs)
            estimateMs = lastIterationMs;
        int idle = thermalCooldown && !warmup
                 ? (int)Math.min(5_000, estimateMs) // aim for 50% work, 50% idle
                 : (int)Math.min(estimateMs/10, 100); // minimal sleep for background tasks
        long maxWarmupItMs = 5_000
                + wOpts.getCount()*wOpts.getTime().convertTo(MILLISECONDS)
                + mOpts.getCount()*mOpts.getTime().convertTo(MILLISECONDS);
        long drainTimeoutMs = warmup ? maxWarmupItMs : opts.getTimeout().convertTo(MILLISECONDS);
        this.drainTimeoutMs = (int)Math.min(Integer.MAX_VALUE, drainTimeoutMs);
        if (first) {
            try {
                apInstance = AsyncProfiler.JavaApi.getInstance("/non-existing-async-profiler");
            } catch (Throwable ignored) {}
            skip = false;
            invocations = 0;
            resultRows = 0;
            EmitterStats.resetGlobalStats();
            SubqueriesStats.reset();
            System.gc();
            if (thermalCooldown) {
                System.out.print("\nThermal cooldown of 5s...");
                System.out.flush();
                Async.uninterruptibleSleep(5_000);
                System.out.printf("\nWill sleep %dms before each subsequent iteration\n", idle);
            } else {
                Async.uninterruptibleSleep(250); // slack for GC & JIT
            }
            forkDeadline = System.nanoTime() + NANOSECONDS.convert(forkTimeoutSecs, SECONDS);
        } else {
            if (idle > 1_000 && iterationNumber == 0)
                System.out.printf("Will sleep %dms before each warmup iteration\n", idle);
            if (warmup) {
                if (iterationNumber == 1 && lastIterationMs > maxWarmupItMs) {
                    skip = true;
                    System.out.println("First warmup took more time than expected for the entire " +
                                       "trial, will skip remaining warmup iterations");
                }
            } else if (skip) {
                System.out.println("Skipping measurement iteration due to previous timeout");
            } else if (lastIterationMs > drainTimeoutMs) {
                skip = true;
                System.out.println("Last measurement iteration timed-out. Will skip this " +
                                   "and all subsequent measurement iterations.");
            } else if (System.nanoTime() > forkDeadline) {
                skip = true;
                System.out.printf("fastersparql.fork-timeout-secs=%d exceeded," +
                                 " skipping subsequent iterations", forkTimeoutSecs);
            }
            Async.uninterruptibleSleep(idle);
        }
        if (!skip && !warmup)
            nonSkippedMeasurementIterations++;
        iterationStart = System.nanoTime();
        jfrEvent.iterationNumber = iterationNumber;
        jfrEvent.warmup = warmup;
        jfrEvent.begin();
    }

    @TearDown(Level.Iteration) public void iterationTearDown(BenchmarkParams params) {
        jfrEvent.end();
        jfrEvent.commit();
        if (iterationNumber == 0) {
            Async.uninterruptibleSleep(50);
            Primer.primeAll();
            var sb = new StringBuilder().append("Parameters recap: ");
            for (String key : params.getParamsKeys())
                sb.append(key).append('=').append(params.getParam(key)).append(' ');
            sb.setLength(sb.length()-1);
            System.out.print(sb.append('\n'));
        }
        resetBatchType(); // recycles Scope and creates a new one if ScopedIdBatchType
        ++iterationNumber;
        lastIterationMs = (System.nanoTime()-iterationStart)/1_000_000L;
    }

    private int checkResult(int r) {
        if (lastBenchResult == -1)
            lastBenchResult = r;
        else if (lastBenchResult != r && !crossSourceDedup)
            throw new RuntimeException("Unstable r. Expected "+lastBenchResult+", got "+r);
        return r;
    }

    private static final VarHandle WATCHDOG_NEXT_THREAD_ID;
    static {
        try {
            WATCHDOG_NEXT_THREAD_ID = MethodHandles.lookup().findStaticVarHandle(QueryBench.class, "plainNextWatchdogId", int.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }
    @SuppressWarnings("unused") private static int plainNextWatchdogId;
    private volatile long watchdogData = 0x7fffffff00000000L;
    private @Nullable Plan watchdogPlan;
    private @Nullable StreamNode watchdogExecution;
    private String benchmarkId;
    private Thread watchdog;

    private void armWatchdog(Plan plan, StreamNode execution) {
        Watchdog.reset();
        watchdogExecution = execution;
        watchdogPlan = plan;
        //noinspection NonAtomicOperationOnVolatileField
        watchdogData = ((long)drainTimeoutMs << 32)
                     | ((watchdogData+1) & 0xffffffffL);
        LockSupport.unpark(watchdog);
    }
    private void disarmWatchdog() {
        watchdogPlan = null;
        watchdogExecution = null;
        //noinspection NonAtomicOperationOnVolatileField
        watchdogData = 0x7fffffff00000000L | ((watchdogData+1) & 0xffffffffL);
    }
    private void dump(Plan plan, @Nullable StreamNode streamNode, String tag) {
        try {
            // find QueryName for current plan
            QueryName query = null;
            for (int i = 0; i < plans.size(); i++) {
                if (plans.get(i) == plan)
                    query = queryList.get(i);
            }
            String name = query == null ? "UNNAMED" : query.name() + "." + tag;
            File dir = new File(benchmarkId);
            if (!dir.isDirectory() && !dir.mkdirs()) {
                log.error("Could not mkdir -p {}{}", dir, dir.isFile() ? ": exists as file" : "");
                dir = new File("");
                if (!dir.canWrite()) {
                    log.error("Current dir {} is not writable", dir.getAbsolutePath());
                    dir = null;
                }
            }
            Watchdog.Spec spec = Watchdog.spec(name)
                    .plan(plan)
                    .streamNode(streamNode)
                    .sparql(query == null ? null : query.opaque().sparql)
                    .emitterServiceStdOut();
            if (dir != null)
                spec.dir(dir);
            spec.run();
        } catch (Throwable e) {
            System.out.println();
            //noinspection CallToPrintStackTrace
            e.printStackTrace();
        }
    }

    private void watchdog() {
        while (true) {
            long data      = watchdogData;

            // wait until 90% or 30s before drainer timeout
            int  timeoutMs = (int)(data>>>32);
            int  dumpMs    = (int)Math.max(0.9*timeoutMs, timeoutMs-30_000);
            long rem       = NANOSECONDS.convert(dumpMs, MILLISECONDS);
            long deadline  = System.nanoTime()+rem;
            while (data == watchdogData && (rem=deadline-System.nanoTime()) > 0)
                LockSupport.parkNanos(rem);
            if (timeoutMs == Integer.MAX_VALUE)
                continue; // not armed

            // dump execution state if watchdog was not disarmed/re-armed
            if (watchdogData != data)
                continue; // disarmWatchdog()/armWatchdog()
            var plan      = watchdogPlan;
            var execution = watchdogExecution;
            if (watchdogData != data)
                continue; // disarmWatchdog()/armWatchdog()
            log.info("{}s/{}s elapsed for drainer timeout", dumpMs/1_000, timeoutMs/1_000);
            dump(plan, execution, watchdog.getName());

            // wait for drainer not responding to timeout cancellation
            timeoutMs = (timeoutMs-dumpMs) // wait until drainTimeoutMs
                      + 60_000;            // allow 1min grace after cancel()
            deadline = System.nanoTime() + NANOSECONDS.convert(timeoutMs, MILLISECONDS);
            while (watchdogData == data && (rem=deadline-System.nanoTime()) > 0)
                LockSupport.parkNanos(rem);

            if (watchdogData != data)
                continue; // disarmWatchdog()/armWatchdog()

            // retry cancel()
            log.info("Reissuing cancel() 1min after first cancel()..., execution={}", execution);
            if (execution instanceof BIt<?> i)
                i.tryCancel();
            else if (execution instanceof Emitter<?,?> e)
                e.cancel();

            // allow a few more seconds for cancel() to be done
            deadline = System.nanoTime() + NANOSECONDS.convert(30, SECONDS);
            while (watchdogData == data && (rem=deadline-System.nanoTime()) > 0)
                LockSupport.parkNanos(rem);

            // kill JVM if watchdog was not disarmed/re-armed
            if (watchdogData == data) {
                log.error("Unresponsive drainer for 1min30s after first cancel(), calling exit(27)");
                System.exit(27);
            } // else: disarmWatchdog()/armWatchdog()
        }
    }

    private BatchType<?> resetBatchType() {
        if (batchType instanceof ScopedIdBatchType.WithScope s)
            batchType = s.reset(this);
        return batchType;
    }

    private int execute(Blackhole bh, BatchConsumer<?, ?> consumer, IntSupplier resultGetter) {
        if (skip)
            return lastBenchResult;
        ++invocations;
        this.bh = bh;
        Plan currentPlan = null;
        StreamNode streamNode = null;
        try {
            switch (flowModel) {
                case ITERATE -> {
                    for (Plan plan : plans) {
                        var it = plan.execute(resetBatchType());
                        armWatchdog(currentPlan=plan, streamNode=it);
                        QueryRunner.drainWild(it, consumer, drainTimeoutMs);
                        disarmWatchdog();
                    }
                }
                case EMIT -> {
                    for (Plan plan : plans) {
                        var em = plan.emit(resetBatchType(), Vars.EMPTY);
                        armWatchdog(currentPlan=plan, streamNode=(StreamNode)em);
                        QueryRunner.drainWild(em, consumer, drainTimeoutMs);
                        disarmWatchdog();
                    }
                }
            }
        } catch (Throwable t) {
            disarmWatchdog();
            dump(currentPlan, streamNode, "fail");
        }
        return checkResult(resultGetter.getAsInt());
    }

    @Benchmark public int countTerms(Blackhole bh) { return execute(bh, boundCounter,   boundCounter::nonNull); }
    @Benchmark public int countRows(Blackhole bh)  { return execute(bh, rowCounter,     rowCounter::rows); }
    @Benchmark public int ropeLen(Blackhole bh)    { return execute(bh, ropeLenCounter, ropeLenCounter::len); }
    @Benchmark public int termLen(Blackhole bh)    { return execute(bh, termLenCounter, termLenCounter::len); }
}
