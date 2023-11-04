package com.github.alexishuf.fastersparql.lrb;

import com.github.alexishuf.fastersparql.FS;
import com.github.alexishuf.fastersparql.FSProperties;
import com.github.alexishuf.fastersparql.batch.Timestamp;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.batch.type.CompressedBatchType;
import com.github.alexishuf.fastersparql.batch.type.TermBatchType;
import com.github.alexishuf.fastersparql.hdt.batch.HdtBatchType;
import com.github.alexishuf.fastersparql.lrb.cmd.MeasureOptions;
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
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import com.github.alexishuf.fastersparql.store.batch.StoreBatchType;
import com.github.alexishuf.fastersparql.util.IOUtils;
import com.github.alexishuf.fastersparql.util.concurrent.Async;
import com.github.alexishuf.fastersparql.util.concurrent.PoolCleaner;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.IntSupplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.github.alexishuf.fastersparql.FSProperties.OP_CROSS_DEDUP;
import static com.github.alexishuf.fastersparql.util.ExceptionCondenser.throwAsUnchecked;
import static java.lang.System.setProperty;

@State(Scope.Thread)
@Threads(1)
@Fork(value = 3, warmups = 0, jvmArgsPrepend = {"--enable-preview", "--add-modules", "jdk.incubator.vector"})
@Measurement(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Warmup(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@BenchmarkMode({Mode.AverageTime})
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class QueryBench {

    @Param({"S.*"}) private String queries;

    @Param({"FS_STORE", "HDT_FILE"}) SourceKind srcKind;
    @Param({"PREFERRED"}) SelectorKindType selKind;
    @Param({"true"}) boolean builtinPlans;
    @Param({"true", "false"}) boolean crossSourceDedup;
    @Param({"COMPRESSED"}) BatchKind batchKind;
    @Param({"ITERATE", "EMIT"}) MeasureOptions.FlowModel flowModel;
//    @Param({"false","true"}) boolean alt;

    public enum SelectorKindType {
        PREFERRED,
        ASK,
        DICT;

        public SelectorKind forSource(SourceKind src) {
            return switch (this) {
                case ASK -> SelectorKind.ASK;
                case DICT -> SelectorKind.DICT;
                case PREFERRED -> src.isFsStore() ? SelectorKind.FS_STORE : SelectorKind.ASK;
            };
        }
    }

    public enum BatchKind {
        COMPRESSED,
        TERM,
        NATIVE;

        <B extends Batch<B>> BatchType<B> forSource(SourceKind src) {
            //noinspection unchecked
            return (BatchType<B>) switch (this) {
                case COMPRESSED -> CompressedBatchType.COMPRESSED;
                case TERM -> TermBatchType.TERM;
                case NATIVE -> switch (src) {
                    case HDT_FILE,HDT_TSV,HDT_JSON,HDT_WS,HDT_TSV_EMIT,HDT_JSON_EMIT,HDT_WS_EMIT -> HdtBatchType.HDT;
                    case FS_STORE,FS_TSV,FS_JSON,FS_WS,FS_TSV_EMIT,FS_JSON_EMIT,FS_WS_EMIT  -> StoreBatchType.STORE;
                };
            };
        }
    }


    private BatchType<?> batchType;
    private FederationHandle fedHandle;
    private List<Plan> plans;
    private List<QueryName> queryList;
    private BoundCounter boundCounter;
    private RowCounter rowCounter;
    private RopeLenCounter ropeLenCounter;
    private TermLenCounter termLenCounter;
    private long iterationStart = 0;
    private int iterationNumber = 0;
    private int iterationMs = 0;
    private final MetricsConsumer metricsConsumer = new MetricsConsumer();
    private int lastBenchResult;
    private Blackhole bh;

    private static class BoundCounter extends QueryRunner.BoundCounter {
        public BoundCounter(BatchType<?> batchType) { super(batchType); }
        @Override public void finish(@Nullable Throwable error) { throwAsUnchecked(error); }
    }

    private static final class RowCounter extends BatchConsumer {
        public int rows;
        public RowCounter(BatchType<?> batchType) {super(batchType);}
        public int rows() { return rows; }
        @Override public void start(Vars vars)                  { rows = 0; }
        @Override public void accept(Batch<?> batch)            { rows += batch.totalRows(); }
        @Override public void accept(Batch<?> batch, int row)   { rows++; }
        @Override public void finish(@Nullable Throwable error) { throwAsUnchecked(error); }
    }

    private static final class RopeLenCounter extends BatchConsumer {
        private final TwoSegmentRope tmp = new TwoSegmentRope();
        private int acc;

        public RopeLenCounter(BatchType<?> batchType) {super(batchType);}
        public int len() { return acc; }

        @Override public void start(Vars vars) { acc = 0; }
        @Override public void finish(@Nullable Throwable error) { throwAsUnchecked(error); }
        @Override public void accept(Batch<?> batch) {
            for (var b = batch; b != null; b = b.next) {
                for (int r = 0, rows = b.rows, cols = b.cols; r < rows; r++) {
                    for (int c = 0; c < cols; c++) {
                        if (b.getRopeView(r, c, tmp))
                            acc += tmp.len;
                    }
                }
            }
        }
        @Override public void accept(Batch<?> batch, int r) {
            for (int c = 0, cols = batch.cols; c < cols; c++) {
                if (batch.getRopeView(r, c, tmp))
                    acc += tmp.len;
            }
        }
    }

    private static final class TermLenCounter extends BatchConsumer {
        private final Term tmp = Term.pooledMutable();
        private int acc;

        public TermLenCounter(BatchType<?> batchType) {super(batchType);}
        public int len() { return acc; }

        @Override public void start(Vars vars) {
            acc = 0;
        }
        @Override public void finish(@Nullable Throwable error) {
            throwAsUnchecked(error);
        }
        @Override public void accept(Batch<?> batch) {
            for (var b = batch; b != null; b = b.next) {
                for (int r = 0, rows = batch.rows, cols = batch.cols; r < rows; r++) {
                    for (int c = 0; c < cols; c++) {
                        if (batch.getView(r, c, tmp))
                            acc += tmp.len;
                    }
                }
            }
        }
        @Override public void accept(Batch<?> batch, int r) {
            for (int c = 0, cols = batch.cols; c < cols; c++) {
                if (batch.getView(r, c, tmp))
                    acc += tmp.len;
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

    @Setup(Level.Trial) public void trialSetup() throws IOException {
        setProperty(OP_CROSS_DEDUP, String.valueOf(crossSourceDedup));
        FSProperties.refresh();
        String dataDirPath = System.getProperty("fs.data.dir");
        if (dataDirPath == null || dataDirPath.isEmpty())
            throw new IllegalArgumentException("fs.data.dir property not set!");
        Path dataDir = Path.of(dataDirPath);
        if (!Files.isDirectory(dataDir))
            throw new IllegalArgumentException("-Dfs.data.dir="+dataDir+" is not a dir");
        String missingSources = LrbSource.all().stream()
                .map(s -> s.filename(srcKind))
                .filter(name -> !Files.exists(dataDir.resolve(name)))
                .collect(Collectors.joining(", "));
        if (!missingSources.isEmpty())
            throw new IOException("Files/dirs missing from "+dataDir+": "+missingSources);
        queryList = new QueryOptions(List.of(queries)).queries();
        if (queryList.isEmpty())
            throw new IllegalArgumentException("No queries selected");
        batchType = batchKind.forSource(srcKind);
        boundCounter = new BoundCounter(batchType);
        rowCounter = new RowCounter(batchType);
        ropeLenCounter = new RopeLenCounter(batchType);
        termLenCounter = new TermLenCounter(batchType);
        File dataDir1 = dataDir.toFile();

        // -------------

        fedHandle = FederationHandle.builder(dataDir1).srcKind(srcKind)
                .selKind(selKind.forSource(srcKind))
                .waitInit(true).create();
        setProperty(OP_CROSS_DEDUP, String.valueOf(crossSourceDedup));
        FSProperties.refresh();
        if (builtinPlans) {
            var reg = PlanRegistry.parseBuiltin();
            reg.resolve(fedHandle.federation);
            plans = queryList.stream().map(reg::createPlan).toList();
            String missingPlans = IntStream.range(0, plans.size())
                    .filter(i -> plans.get(i) == null)
                    .mapToObj(i -> queryList.get(i).name()).collect(Collectors.joining(", "));
            if (!missingPlans.isEmpty())
                throw new IllegalArgumentException("No plan for "+missingPlans);
        } else {
            plans = queryList.stream().map(q -> fedHandle.federation.plan(q.parsed())).toList();
        }
        for (Plan plan : plans)
            plan.attach(metricsConsumer);

//        watchdog = new Thread(this::watchdog, "watchdog");
//        watchdog.start();
        System.out.println("\nThermal cooldown: 5s...");
        Async.uninterruptibleSleep(5_000);
    }

    @TearDown(Level.Trial) public void trialTearDown() {
//        watchdog.interrupt();
//        try {
//            watchdog.join(Duration.ofSeconds(1));
//        } catch (InterruptedException ignored) {}
        fedHandle.close();
        FS.shutdown();
    }

    @Setup(Level.Iteration) public void iterationSetup() throws IOException {
//        CompressedBatchType.ALT = alt;
        lastBenchResult = -1;
        // drop unreachable references inside pools, do I/O and call for GC
        PoolCleaner.INSTANCE.sync();
        IOUtils.fsync(50_000);
        Async.uninterruptibleSleep(10);
        if ((iterationNumber&1) == 0)
            System.gc();

        int slack = Math.min(2_000, 50+iterationMs);
        if (slack > 1_000)
            System.out.printf("dynamic thermal slack: %dms\n", slack);
        Async.uninterruptibleSleep(slack);
        iterationStart = Timestamp.nanoTime();
    }

    @TearDown(Level.Iteration) public void iterationTearDown() {
        ++iterationNumber;
        iterationMs = (int)Math.max(1, (Timestamp.nanoTime()-iterationStart)/1_000_000L);
    }

    private int checkResult(int r) {
        if (lastBenchResult == -1)
            lastBenchResult = r;
        else if (lastBenchResult != r && !crossSourceDedup)
            throw new RuntimeException("Unstable r. Expected "+lastBenchResult+", got "+r);
        return r;
    }

//    private volatile @Nullable Plan watchdogPlan;
//    private @Nullable StreamNode dbgExecution;
//    private static final long WATCHDOG_INTERVAL_NS = 5_000_000_000L;
//    private Thread watchdog;
//
//    private void armWatchdog(Plan plan, StreamNode execution) {
//        ThreadJournal.resetJournals();
//        ResultJournal.clear();
//        dbgExecution = execution;
//        watchdogPlan = plan;
//        LockSupport.unpark(watchdog);
//    }
//    private void disarmWatchdog() {
//        dbgExecution = null;
//        watchdogPlan = null;
//        LockSupport.unpark(watchdog);
//    }
//
//    @SuppressWarnings("CallToPrintStackTrace") private void watchdog() {
//        while (true) {
//            Plan plan;
//            while ((plan = watchdogPlan) == null) LockSupport.park(this);
//            StreamNode execution = dbgExecution;
//
//            long deadline = System.nanoTime()+WATCHDOG_INTERVAL_NS;
//            while (watchdogPlan == plan && System.nanoTime() < deadline)
//                LockSupport.parkNanos(this, WATCHDOG_INTERVAL_NS);
//            if (watchdogPlan != plan) // disarmed or armed for another query
//                continue;
//            watchdogPlan = null;
//
//            // find QueryName for current plan
//            QueryName queryName = null;
//            for (int i = 0; i < plans.size(); i++) {
//                if (plans.get(i) == plan)
//                    queryName = queryList.get(i);
//            }
//            System.out.printf("WATCHDOG FIRED for query %s\n", queryName);
//            if (queryName != null) { // write query SPARQL and .dot and .svg of execution tree
//                System.out.println(queryName.opaque().sparql());
//                File dot = new File("/tmp/"+queryName.name()+".dot");
//                File svg = new File(dot.getPath().replace(".dot", ".svg"));
//                try (var writer = new FileWriter(dot, UTF_8)) {
//                    assert execution != null : "execution must not be null";
//                    writer.append(execution.toDOT(WITH_STATE_AND_STATS));
//                    execution.renderDOT(svg, WITH_STATE_AND_STATS);
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//            }
//            // dump plan algebra, ResultJournal, ThreadJournal and EmitterService queues
//            System.out.println(plan);
//            try {
//                ResultJournal.dump(System.out);
//            } catch (IOException e) {e.printStackTrace();}
//            ThreadJournal.dumpAndReset(System.out, 100);
//            System.out.println(EmitterService.EMITTER_SVC.dump());
//        }
//    }

    private int execute(Blackhole bh, BatchConsumer consumer, IntSupplier resultGetter) {
        this.bh = bh;
        switch (flowModel) {
            case ITERATE -> {
                for (Plan plan : plans)
                    QueryRunner.drain(plan.execute(batchType), consumer);
            }
            case EMIT -> {
                for (Plan plan : plans)
                    QueryRunner.drain(plan.emit(batchType, Vars.EMPTY), consumer);

            }
        }
        return checkResult(resultGetter.getAsInt());
    }

    @Benchmark public int countTerms(Blackhole bh) { return execute(bh, boundCounter,   boundCounter::nonNull); }
    @Benchmark public int countRows(Blackhole bh)  { return execute(bh, rowCounter,     rowCounter::rows); }
    @Benchmark public int ropeLen(Blackhole bh)    { return execute(bh, ropeLenCounter, ropeLenCounter::len); }
    @Benchmark public int termLen(Blackhole bh)    { return execute(bh, termLenCounter, termLenCounter::len); }
}
