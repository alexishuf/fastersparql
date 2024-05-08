package com.github.alexishuf.fastersparql.lrb.cmd;

import com.github.alexishuf.fastersparql.FlowModel;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.batch.type.CompressedBatchType;
import com.github.alexishuf.fastersparql.batch.type.TermBatchType;
import com.github.alexishuf.fastersparql.lrb.query.QueryGroup;
import com.github.alexishuf.fastersparql.lrb.query.QueryName;
import com.github.alexishuf.fastersparql.util.IOUtils;
import com.github.alexishuf.fastersparql.util.concurrent.BackgroundTasks;
import com.github.alexishuf.fastersparql.util.concurrent.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Random;

import static com.github.alexishuf.fastersparql.FSProperties.OP_WEAKEN_DISTINCT;
import static com.github.alexishuf.fastersparql.lrb.cmd.MeasureOptions.ResultsConsumer.COUNT;
import static com.github.alexishuf.fastersparql.util.concurrent.Async.uninterruptibleSleep;
import static java.lang.System.setProperty;

@Command
public class MeasureOptions {
    private static final Logger log = LoggerFactory.getLogger(MeasureOptions.class);

    public enum ResultsConsumer {
        COUNT,
        SAVE,
        SAVE_FIRST,
        CHECK
    }
    @Option(names = "--consumer", description = "What to do with results from queries.")
    public ResultsConsumer consumer = COUNT;

    @Option(names = {"--flow"}, description = "Whether to iterate over results (pull model) or react to results (push model)")
    public FlowModel flowModel = FlowModel.ITERATE;

    public enum BatchKind {
        TERM,
        COMPRESSED;

        public BatchType<?> asType() {
            return switch (this) {
                case TERM -> TermBatchType.TERM;
                case COMPRESSED -> CompressedBatchType.COMPRESSED;
            };
        }
    }
    public BatchType<?> batchType = CompressedBatchType.COMPRESSED;
    @Option(names = "--batch", description = "The Batch implementation to use consume " +
            "the results with")
    public void batchType(BatchKind kind) {
        batchType = kind.asType();
    }

    @Option(names = "--jfr", description = "record the execution of the queries using " +
            "JDK Flight Recorder. The JFR dump will be saved to the given file path. If this " +
            "argument is not given, not only the dump will not be written but JFR will also not " +
            "be started. Federation/sources setup will not be recorded.")
    public String jfr = null;

    @Option(names = "--jfr-config", description = "If --jfr is set, the value of this option " +
            "will be passed to jdk.jfr.Configuration.getConfiguration to obtain the JFR config " +
            "for the recording. The JDK ships with \"default\" and \"profile\" configs")
    public String jfrConfigName = "default";

    @Option(names = "--prof-warmup", description = "Combined with --jfr or --async-profiler," +
            " will cause the warmup queries to also be profiled.")
    public boolean profWarmup = false;

    @Option(names = {"--async-profiler", "--ap"}, description = "Enable async-profiler during " +
            "execution of queries. The async profiler will dump a JFR file to the location" +
            "given in this option. If this option is not given async-profiler will not even " +
            "be started. Federation/sources setup will not be recorded.")
    public String asyncProfiler = null;

    @Option(names = {"--async-profiler-exclude-warmup", "--ap-exclude-warmpup"}, description =
            "If set async profiler will not be started before warmup queries are completed")
    public boolean apExcludeWarmup = false;

    @Option(names = "--builtin-plans-json", description = "Same effect as --plans-json, but uses " +
            "a built-in JSON file")
    public boolean builtinPlansJson = false;

    @Option(names = "--plans-json", description = "If this option is given, there will be no " +
            "source selection/agglutination/optimization by the federation for each SPARQL query." +
            "Instead, a plan will be loaded from a JSON file, placeholder URLs will be replaced" +
            " with actual SparqlClients in the federation and the plan will be executed.")
    public File plansJson = null;

    @Option(names = "--dest-dir", description = "Writes results to given dir. By default, " +
            "a dir with current date and time will be created to hold the results of " +
            "each invocation.")
    private String destDir = null;
    public File destDir() throws IOException {
        var file = destDir == null ? new File("results-" + LocalDateTime.now())
                : new File(destDir);
        if (file.isDirectory()) return file;
        if (file.exists()) throw new IOException("destDir="+destDir+" exists as non-dir");
        if (!file.mkdirs()) throw new IOException("could not create "+destDir);
        return file;
    }

    @Option(names = "--warm-secs", description = "Warmup by executing the scheduled" +
            " queries until this number of seconds has elapsed")
    public int warmupSecs = 5;
    @Option(names = "--warm-cool-ms", description = "After warmup (if --warm-secs > 0), " +
            "sleep this many milliseconds")
    public int warmupCooldownMs = 2_000;
    @Option(names = "--cool-ms", description = "After each non-warmup repetition, sleep this " +
            "number of milliseconds")
    public int cooldownMs = 1_000;


    @Option(names = "--no-call-gc", negatable = true, description = "Calls System.gc() during " +
            "cooldown (after all warmup repetitions and after every measured " +
            "(non-warmup) repetition")
    public boolean callGC = true;

    public void cooldown(int ms) {
        if (ms < 0)
            return;
        if (ms > 1_000)
            log.info("cooldown({})", ms);
        var runtime = Runtime.getRuntime();
        double freeBefore = runtime.freeMemory()/(double)runtime.totalMemory();

        BackgroundTasks.sync();
        if (callGC) {
            if (ms > 10 || asyncProfiler != null) // null writes SHOULD be visible anyway
                uninterruptibleSleep(10);
            if (callGC)
                System.gc();
        }

        long start = Timestamp.nanoTime();
        if (callGC && asyncProfiler != null) // ap sees into GC code, but we are not profiling GC
            uninterruptibleSleep(50);

        IOUtils.fsync(ms - (ms > 10 ? 10 : 0));
        uninterruptibleSleep(ms - (int)((Timestamp.nanoTime()-start)/1_000_000));

        if (callGC) {
            double after = runtime.freeMemory()/(double)runtime.totalMemory();
            log.info("cooldown(): +{}% free memory", String.format("%.2f", 100*(after-freeBefore)));
        }
    }

    public void       cooldown() { cooldown(cooldownMs); }
    public void warmupCooldown() { cooldown(warmupCooldownMs); }

    @Option(names = "--reps", description = "Measure each (query, sources configuration) pair " +
            "this number of times")
    public int repetitions = 7;
    @Option(names = "--budget-secs", description = "Execute less than --reps repetitions for a " +
            "(query, sources configuration) pair if this number of seconds has already been " +
            "spent with that pair")
    public int budgetSecs = 60*5;
    @Option(names = "--timeout-secs", description = "Abort a query if it has been executing " +
            "for more than this number of seconds")
    public int timeoutSecs = 60*5;

    public Random random = new Random();
    @Option(names = "--seed", description = "Seed for the random generator")
    public void seed(long seed) {
        random = new Random(seed);
    }

    @Option(names = {"--weaken-distinct"}, negatable = true, description = "Use fixed-cost" +
            "implementation of distinct for all queries other than B* queries.")
    public boolean weakenDistinct = false;

    @Option(names = {"--weaken-distinct-B"}, negatable = true, defaultValue = "true",
            fallbackValue = "true", description = "Use fixed-cost cheaper distinct " +
            "implementation that may not remove all duplicates")
    public boolean weakenDistinctForBQueries = true;

    void updateWeakenDistinct(QueryName queryName) {
        boolean weaken = queryName.group() == QueryGroup.B
                ? weakenDistinctForBQueries
                : weakenDistinct;
        setProperty(OP_WEAKEN_DISTINCT, Boolean.toString(weaken));
    }
}
