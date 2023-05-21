package com.github.alexishuf.fastersparql.lrb.cmd;

import com.github.alexishuf.fastersparql.batch.Timestamp;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.util.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Random;

import static com.github.alexishuf.fastersparql.lrb.cmd.MeasureOptions.ResultsConsumer.COUNT;
import static com.github.alexishuf.fastersparql.util.concurrent.Async.uninterruptibleSleep;

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

    public enum BatchKind {
        TERM,
        COMPRESSED;

        public BatchType<?> asType() {
            return switch (this) {
                case TERM -> Batch.TERM;
                case COMPRESSED -> Batch.COMPRESSED;
            };
        }
    }
    public BatchType<?> batchType = Batch.COMPRESSED;
    @Option(names = "--batch", description = "The Batch implementation to use consume " +
            "the results with")
    public void batchType(BatchKind kind) {
        batchType = kind.asType();
    }

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

    public void cooldown(int ms) {
        if (ms > 1_000)
            log.info("cooldown({})", ms);
        long start = Timestamp.nanoTime();
        var runtime = Runtime.getRuntime();
        double freeBefore = runtime.freeMemory()/(double)runtime.totalMemory();

        System.gc();
        IOUtils.fsync(ms-(Timestamp.nanoTime()-start)/1_000_000);
        do {
            uninterruptibleSleep(250);
            System.gc();
        } while ((Timestamp.nanoTime()-start) < 250_000_000);

        double freeAfter = runtime.freeMemory()/(double)runtime.totalMemory();
        log.info("cooldown(): {}% free memory", String.format("%.2f", freeAfter-freeBefore));
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
}
