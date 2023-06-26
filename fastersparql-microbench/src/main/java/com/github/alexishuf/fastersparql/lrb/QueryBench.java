package com.github.alexishuf.fastersparql.lrb;

import com.github.alexishuf.fastersparql.FS;
import com.github.alexishuf.fastersparql.FSProperties;
import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.hdt.batch.HdtBatch;
import com.github.alexishuf.fastersparql.lrb.cmd.QueryOptions;
import com.github.alexishuf.fastersparql.lrb.query.PlanRegistry;
import com.github.alexishuf.fastersparql.lrb.query.QueryName;
import com.github.alexishuf.fastersparql.lrb.query.QueryRunner;
import com.github.alexishuf.fastersparql.lrb.query.QueryRunner.BatchConsumer;
import com.github.alexishuf.fastersparql.lrb.sources.FederationHandle;
import com.github.alexishuf.fastersparql.lrb.sources.LrbSource;
import com.github.alexishuf.fastersparql.lrb.sources.SelectorKind;
import com.github.alexishuf.fastersparql.lrb.sources.SourceKind;
import com.github.alexishuf.fastersparql.model.rope.TwoSegmentRope;
import com.github.alexishuf.fastersparql.operators.metrics.Metrics;
import com.github.alexishuf.fastersparql.operators.metrics.MetricsListener;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import com.github.alexishuf.fastersparql.store.batch.StoreBatch;
import com.github.alexishuf.fastersparql.util.IOUtils;
import com.github.alexishuf.fastersparql.util.concurrent.Async;
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

import static com.github.alexishuf.fastersparql.FSProperties.DEF_OP_CROSS_DEDUP_CAPACITY;
import static com.github.alexishuf.fastersparql.FSProperties.OP_CROSS_DEDUP_CAPACITY;
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
                case COMPRESSED -> Batch.COMPRESSED;
                case TERM -> Batch.TERM;
                case NATIVE -> switch (src) {
                    case HDT_FILE,HDT_TSV,HDT_JSON,HDT_WS -> HdtBatch.TYPE;
                    case FS_STORE,FS_TSV, FS_JSON, FS_WS  -> StoreBatch.TYPE;
                };
            };
        }
    }


    private File dataDir;
    private BatchType<?> batchType;
    private FederationHandle fedHandle;
    private List<Plan> plans;
    private List<QueryName> queryList;
    private BoundCounter boundCounter;
    private RowCounter rowCounter;
    private RopeLenCounter ropeLenCounter;
    private TermLenCounter termLenCounter;
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
        @Override public void start(BIt<?> it)                  { rows = 0; }
        @Override public void accept(Batch<?> batch)            { rows += batch.rows; }
        @Override public void finish(@Nullable Throwable error) { throwAsUnchecked(error); }
    }

    private static final class RopeLenCounter extends BatchConsumer {
        private final TwoSegmentRope tmp = new TwoSegmentRope();
        private int acc;

        public RopeLenCounter(BatchType<?> batchType) {super(batchType);}
        public int len() { return acc; }

        @Override public void start(BIt<?> it) { acc = 0; }
        @Override public void finish(@Nullable Throwable error) { throwAsUnchecked(error); }
        @Override public void accept(Batch<?> batch) {
            for (int r = 0, rows = batch.rows, cols = batch.cols; r < rows; r++) {
                for (int c = 0; c < cols; c++) {
                    if (batch.getRopeView(r, c, tmp))
                        acc += tmp.len;
                }
            }
        }
    }

    private static final class TermLenCounter extends BatchConsumer {
        private final Term tmp = Term.pooledMutable();
        private int acc;

        public TermLenCounter(BatchType<?> batchType) {super(batchType);}
        public int len() { return acc; }

        @Override public void start(BIt<?> it) {
            acc = 0;
        }
        @Override public void finish(@Nullable Throwable error) {
            throwAsUnchecked(error);
        }
        @Override public void accept(Batch<?> batch) {
            for (int r = 0, rows = batch.rows, cols = batch.cols; r < rows; r++) {
                for (int c = 0; c < cols; c++) {
                    if (batch.getView(r, c, tmp))
                        acc += tmp.len;
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

    @Setup(Level.Trial) public void setup() throws IOException {
        setProperty(OP_CROSS_DEDUP_CAPACITY,
                    String.valueOf(crossSourceDedup ? DEF_OP_CROSS_DEDUP_CAPACITY : 0));
        FSProperties.refresh();
        String dataDirPath = System.getProperty("fs.data.dir");
        if (dataDirPath == null || dataDirPath.isEmpty())
            throw new IllegalArgumentException("fs.data.dir property not set!");
        Path dataDir = Path.of(dataDirPath);
        if (!Files.isDirectory(dataDir))
            throw new IllegalArgumentException("-Dfs.data.dir="+dataDir+" is not a dir");
        String missing = LrbSource.all().stream()
                .map(s -> s.filename(srcKind))
                .filter(name -> !Files.exists(dataDir.resolve(name)))
                .collect(Collectors.joining(", "));
        if (!missing.isEmpty())
            throw new IOException("Files/dirs missing from "+dataDir+": "+missing);
        queryList = new QueryOptions(List.of(queries)).queries();
        if (queryList.isEmpty())
            throw new IllegalArgumentException("No queries selected");
        batchType = batchKind.forSource(srcKind);
        boundCounter = new BoundCounter(batchType);
        rowCounter = new RowCounter(batchType);
        ropeLenCounter = new RopeLenCounter(batchType);
        termLenCounter = new TermLenCounter(batchType);
        this.dataDir = dataDir.toFile();
        System.out.println("\nThermal cooldown: 7s...");
        Async.uninterruptibleSleep(7_000);
    }

    @Setup(Level.Iteration) public void iterationSetup() throws IOException {
//        CompressedBatchType.ALT = alt;
        fedHandle = FederationHandle.builder(dataDir).srcKind(srcKind)
                                    .selKind(selKind.forSource(srcKind))
                                    .waitInit(true).create();
        setProperty(OP_CROSS_DEDUP_CAPACITY,
                String.valueOf(crossSourceDedup ? DEF_OP_CROSS_DEDUP_CAPACITY : 0));
        FSProperties.refresh();
        if (builtinPlans) {
            var reg = PlanRegistry.parseBuiltin();
            reg.resolve(fedHandle.federation);
            plans = queryList.stream().map(reg::createPlan).toList();
            String missing = IntStream.range(0, plans.size())
                    .filter(i -> plans.get(i) == null)
                    .mapToObj(i -> queryList.get(i).name()).collect(Collectors.joining(", "));
            if (!missing.isEmpty())
                throw new IllegalArgumentException("No plan for "+missing);
        } else {
            plans = queryList.stream().map(q -> fedHandle.federation.plan(q.parsed())).toList();
        }
        for (Plan plan : plans)
            plan.attach(metricsConsumer);
        lastBenchResult = -1;
        System.gc();
        IOUtils.fsync(50_000);
        Async.uninterruptibleSleep(100);
        System.gc();
        Async.uninterruptibleSleep(500);
    }

    @TearDown(Level.Iteration) public void iterationTearDown() {
        fedHandle.close();
    }

    @TearDown(Level.Trial) public void tearDown() {
        FS.shutdown();
    }

    private int checkResult(int r) {
        if (lastBenchResult == -1)
            lastBenchResult = r;
        else if (lastBenchResult != r && !crossSourceDedup)
            throw new RuntimeException("Unstable r. Expected "+lastBenchResult+", got "+r);
        return r;
    }

    private int execute(Blackhole bh, BatchConsumer consumer, IntSupplier resultGetter) {
        this.bh = bh;
        for (Plan plan : plans)
            QueryRunner.drain(plan.execute(batchType), consumer);
        return checkResult(resultGetter.getAsInt());
    }

    @Benchmark public int countTerms(Blackhole bh) { return execute(bh, boundCounter,   boundCounter::nonNull); }
    @Benchmark public int countRows(Blackhole bh)  { return execute(bh, rowCounter,     rowCounter::rows); }
    @Benchmark public int ropeLen(Blackhole bh)    { return execute(bh, ropeLenCounter, ropeLenCounter::len); }
    @Benchmark public int termLen(Blackhole bh)    { return execute(bh, termLenCounter, termLenCounter::len); }
}
