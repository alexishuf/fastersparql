package com.github.alexishuf.fastersparql.lrb.cmd;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.client.netty.util.NettyChannelDebugger;
import com.github.alexishuf.fastersparql.emit.Emitter;
import com.github.alexishuf.fastersparql.emit.async.Stateful;
import com.github.alexishuf.fastersparql.fed.FedMetrics;
import com.github.alexishuf.fastersparql.fed.FedMetricsListener;
import com.github.alexishuf.fastersparql.fed.Federation;
import com.github.alexishuf.fastersparql.lrb.query.PlanRegistry;
import com.github.alexishuf.fastersparql.lrb.query.QueryChecker;
import com.github.alexishuf.fastersparql.lrb.query.QueryName;
import com.github.alexishuf.fastersparql.lrb.query.QueryRunner;
import com.github.alexishuf.fastersparql.lrb.query.QueryRunner.BatchConsumer;
import com.github.alexishuf.fastersparql.lrb.sources.FederationHandle;
import com.github.alexishuf.fastersparql.lrb.sources.LrbSource;
import com.github.alexishuf.fastersparql.model.SparqlResultFormat;
import com.github.alexishuf.fastersparql.model.rope.OutputStreamSink;
import com.github.alexishuf.fastersparql.operators.metrics.Metrics;
import com.github.alexishuf.fastersparql.operators.metrics.MetricsListener;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import com.github.alexishuf.fastersparql.sparql.results.serializer.ResultsSerializer;
import com.github.alexishuf.fastersparql.util.StreamNode;
import com.github.alexishuf.fastersparql.util.concurrent.*;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import jdk.jfr.Configuration;
import jdk.jfr.Recording;
import jdk.jfr.consumer.RecordingFile;
import one.profiler.AsyncProfiler;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.ParseException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import static com.github.alexishuf.fastersparql.lrb.cmd.MeasureOptions.ResultsConsumer.SAVE;
import static com.github.alexishuf.fastersparql.lrb.query.QueryRunner.drainWild;
import static com.github.alexishuf.fastersparql.model.SparqlResultFormat.TSV;
import static com.github.alexishuf.fastersparql.model.Vars.EMPTY;
import static java.lang.System.nanoTime;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.util.Objects.requireNonNull;

@Command(name = "measure", description = "Mediate queries and measure wall-clock time")
public class Measure implements Callable<Void>{
    public static final Logger log = LoggerFactory.getLogger(Measure.class);

    private @Mixin LogOptions logOp;
    private @Mixin QueryOptions qryOp;
    private @Mixin SourceOptions srcOp;
    private @Mixin MeasureOptions msrOp;

    private @MonotonicNonNull File destDir;
    public PlanRegistry plans;

    @Override public Void call() throws Exception {
        loadProfilers();
        destDir = msrOp.destDir();
        var tasks = qryOp.queries().stream()
                         .map(q -> new MeasureTask(q, srcOp.selKind, srcOp.srcKind))
                         .collect(Collectors.toCollection(ArrayList::new));
        Collections.shuffle(tasks, msrOp.random);

        Set<LrbSource> sources = srcOp.lrbSources();
        if (sources.size() == 1 && sources.iterator().next().equals(LrbSource.LargeRDFBench_all)) {
            plans = PlanRegistry.unionSource();
        } else if (msrOp.builtinPlansJson) {
            plans = PlanRegistry.parseBuiltin();
        }
        if (msrOp.plansJson != null) {
            if (msrOp.builtinPlansJson)
                log.error("--builtin-plans-json and --plans-json cannot both be set");
            plans = PlanRegistry.parse(msrOp.plansJson);
        }
        try (var fedHandle = FederationHandle.builder(srcOp.dataDir)
                                             .selKind(srcOp.selKind)
                                             .srcKind(srcOp.srcKind)
                                             .subset(sources)
                                             .waitInit(true).create()) {
            var fed = fedHandle.federation;
            if (plans != null) {
                plans.resolve(fed);
                var missing = qryOp.queries().stream().filter(q -> plans.createPlan(q) == null).toList();
                if (!missing.isEmpty()) {
                    log.error("Missing queries from --plans-json {}: {}", msrOp.plansJson, missing);
                    return null;
                }
            }
            fed.addFedListener(fedListener);
            fed.addPlanListener(planListener);
            if (jfr != null && msrOp.profWarmup)
                jfr.start();
            asyncProfilerAllowed = asyncProfiler != null && msrOp.profWarmup;
            try {
                warmup(tasks, fed);
                if (jfr != null && !msrOp.profWarmup)
                    jfr.start();
                asyncProfilerAllowed = asyncProfiler != null;
                measure(tasks, fed);
            } finally {
                stopAsyncProfilerIfActive();
                if (jfr != null)  {
                    jfr.close();
                    postProcessJFR(jfrDest);
                }
                for (var it = checkerConsumers.entrySet().iterator(); it.hasNext(); ) {
                    Checker<?> c = it.next().getValue();
                    c.recycle(this);
                    it.remove();
                    if (consumer == c)
                        consumer = null;
                }
                if (consumer != null)
                    consumer = consumer.recycle(this);
                BackgroundTasks.sync();
            }
        }
        return null;
    }

    private void mkdir(String string) throws IOException {
        File file = new File(string);
        var dir = file.getParentFile();
        if (dir == null)
            return; // cwd already exists
        if (!dir.isDirectory() && !dir.mkdirs())
            throw new IOException("Could not mkdir dir for file "+file);
        if (file.isDirectory())
            throw new IOException("File already exists as dir: "+file);
    }

    private void loadProfilers() throws IOException, ParseException {
        if (msrOp.jfr != null && !msrOp.jfr.isEmpty()) {
            mkdir(msrOp.jfr);
            jfrDest = Path.of(msrOp.jfr);
            jfr = new Recording(Configuration.getConfiguration(msrOp.jfrConfigName));
            jfr.setDumpOnExit(true);
            jfr.setDestination(jfrDest);
            log.info("Will dump a JFR recording to {} at exit{}", jfrDest,
                     msrOp.profWarmup ? " (including warmup)" : "");
        }
        if (msrOp.asyncProfiler != null && !msrOp.asyncProfiler.isEmpty()) {
            mkdir(msrOp.asyncProfiler);
            asyncProfiler = AsyncProfiler.getInstance();
            asyncProfilerDest = Path.of(msrOp.asyncProfiler).toAbsolutePath();
            asyncProfilerCmd = "resume,event=cpu,alloc,jfr,file="+msrOp.asyncProfiler;
            log.info("Will dump a JFR by async-profiler to {} at exit{}",
                     asyncProfilerDest, msrOp.profWarmup ? " (including warmup)" : "");
        }
    }

    private static final int AP_WINDOW_MS = 10;
    private void resumeAsyncProfilerIfAllowed() {
        if (asyncProfilerAllowed && !asyncProfilerActive && asyncProfiler != null) {
            Thread.yield();
            Async.uninterruptibleSleep(AP_WINDOW_MS);
            try {
                asyncProfiler.execute(asyncProfilerCmd);
                asyncProfilerActive  = true;
                Thread.yield();
                Async.uninterruptibleSleep(AP_WINDOW_MS);
            } catch (IOException e) {
                try {
                    Files.deleteIfExists(asyncProfilerDest);
                } catch (IOException de) {
                    log.error("Failed to delete stale {}: {}", asyncProfilerDest, de.getMessage());
                }
                log.error("Failed to start async-profiler", e);
            }
        }

    }
    private void stopAsyncProfilerIfActive() {
        if (asyncProfilerActive && asyncProfiler != null) {
            try {
                asyncProfiler.execute("stop");
                Thread.yield();
                Async.uninterruptibleSleep(AP_WINDOW_MS);
            } catch (IOException e) {
                log.error("failed to stop async-profiler", e);
            } finally {
                asyncProfilerActive = false;
            }
        }
    }

    private static final String EXEC_SAMPLE = "jdk.ExecutionSample";
    private static final String NATIVE_SAMPLE = "jdk.NativeMethodSample";
    private static final String INTELLIJ = "com.intellij.rt";

    static void postProcessJFR(Path dest) {
        try (var f = new RecordingFile(dest)) {
            var filtered = dest.resolveSibling(dest.getFileName()+".tmp");
            f.write(filtered, e -> {
                var t = e.getEventType().getName();
                if (!t.equals(EXEC_SAMPLE) && !t.equals(NATIVE_SAMPLE))
                    return true;
                var trace = e.getStackTrace().getFrames();
                return trace.isEmpty() ||
                        !trace.getLast().getMethod().getType().getName().startsWith(INTELLIJ);
            });
            Files.move(filtered, dest, REPLACE_EXISTING);
        } catch (Throwable t) {
            log.error("Failed to post-process {}", dest);
        }
    }

    @SuppressWarnings("unused") private static String resolveUris(Federation fed, String string) {
        String[] out = {string};
        Async.waitStage(fed.forEachSource((src, handler) -> {
            String name = src.spec().getString("lrb-name");
            if (name == null) name = src.spec().getString("name");
            String uri = src.client.endpoint().uri().replace(".", "\\.").replace("*", "\\*")
                                                    .replace("+", "\\+").replace("?", "\\?");
            out[0] = out[0].replaceAll(uri, name);
            handler.apply(null, null);
        }));
        return out[0];
    }

    private void warmup(List<MeasureTask> tasks, Federation client) {
        int rep = -1, taskIdx = 0;
        for (int remMs = msrOp.warmupSecs*1_000, ms; remMs > 0; remMs -= ms) {
            var task = tasks.get(taskIdx);
            log.info("Starting warmup {} of {}, src={}, flow={}...",
                    -rep, task, task.source(), msrOp.flowModel);
            ms = run(client, task, rep, remMs);
            log.info("Warmup {} of {} in {}ms", -rep, task, ms);
            if ((taskIdx = (taskIdx+1) % tasks.size()) == 0)
                --rep;
        }
        msrOp.warmupCooldown();
    }


    private void measure(List<MeasureTask> schedule, Federation client) {
        int budgetMs = msrOp.budgetSecs * 1_000;
        int[] spent = new int[schedule.size()];
        for (int rep = 0; rep < msrOp.repetitions; rep++) {
            for (int taskIdx = 0, nTasks = schedule.size(); taskIdx < nTasks; taskIdx++) {
                int timeoutMs = Math.min(msrOp.timeoutSecs*1_000, budgetMs - spent[taskIdx]);
                var task = schedule.get(taskIdx);
                if (timeoutMs <= 0) {
                    log.warn("No time budget for rep {} of {}", rep, task);
                    continue;
                }
                log.info("Starting rep {} of {} with sel={}, src={}, flow={}...",
                         rep, task.query(), task.selector(), task.source(), msrOp.flowModel);
                int ms = run(client, task, rep, timeoutMs);
                log.info("Measured rep {} of {} with sel={} and src={} in {}ms",
                        rep, task.query(), task.selector(), task.source(), ms);
                spent[taskIdx] += ms;
                msrOp.cooldown();
            }
        }
    }

    private int run(Federation fed, MeasureTask task, int rep, int timeoutMs) {
        QueryName query = task.query();
        msrOp.updateWeakenDistinct(query);
        BatchConsumer<?, ?> consumer = consumer(task, rep);
        NettyChannelDebugger.reset();
        ResultJournal.clear();
        ThreadJournal.resetJournals();
        try {
            Files.deleteIfExists(Path.of("/tmp/"+ query +".journal"));
        } catch (IOException ignored) { }
        BIt<?> it = null;
        Orphan<? extends Emitter<?, ?>> emOrphan = null;
        StreamNode results = null;
        resumeAsyncProfilerIfAllowed();
        long startNs = nanoTime(), stopNs;
        try {
            if (plans != null) {
                var plan        = requireNonNull(plans.createPlan(query));
                currentPlan     = plan;
                fedMetrics      = new FedMetrics(fed, task.parsed());
                fedMetrics.plan = plan;
                plan.attach(planListener);
                results = (StreamNode)switch (msrOp.flowModel) {
                    case ITERATE -> it       = plan.execute(msrOp.batchType);
                    case EMIT    -> emOrphan = plan.    emit(msrOp.batchType, EMPTY);
                };
            } else {
                results = (StreamNode) switch (msrOp.flowModel) {
                    case ITERATE -> it       = fed.query(msrOp.batchType, task.parsed());
                    case EMIT    -> emOrphan = fed. emit(msrOp.batchType, task.parsed(), EMPTY);
                };
            }
//            if (debugPlan != null)
//                System.out.println(debugPlan);
            try (var w = spec(query, rep, ".30s", currentPlan, results).startSecs(30)) {
                switch (msrOp.flowModel) {
                    case ITERATE -> drainWild(requireNonNull(it), consumer, timeoutMs);
                    case EMIT -> drainWild(requireNonNull(emOrphan), consumer, timeoutMs);
                }
                w.stop();
            }
            stopNs = nanoTime();
        } catch (Throwable t) {
            stopNs = nanoTime();
            consumer.onError(t);
            log.error("Error during rep {} of task={}:", rep, task, t);
        } finally {
            stopAsyncProfilerIfActive();
        }
        if (results instanceof Stateful<?> s && s.stateName().contains("FAILED"))
            spec(query, rep, "", currentPlan, results).run();
        if (consumer instanceof Checker<?> c && !c.isValid())
            spec(query, rep, "", currentPlan, results).run();
        return (int)((stopNs - startNs)/1_000_000L);
    }

    private Watchdog.Spec spec(QueryName qry, int rep, String suffix, Plan plan, StreamNode sn) {
        String name = qry.name()+"."+rep+suffix;
        return Watchdog.spec(name).plan(plan).sparql(qry.opaque().sparql).streamNode(sn);
    }

    /* --- --- --- metrics collection --- --- --- */

    private @Nullable MeasureTask currTask;
    private int currRep = -1;
    private @Nullable Plan currentPlan;
    private @Nullable FedMetrics fedMetrics;
    private @Nullable Metrics planMetrics;
    private @MonotonicNonNull BatchConsumer<?, ?> consumer;
    private @MonotonicNonNull Recording jfr;
    private @MonotonicNonNull Path jfrDest;
    private @MonotonicNonNull AsyncProfiler asyncProfiler;
    private boolean asyncProfilerAllowed, asyncProfilerActive;
    private @MonotonicNonNull Path asyncProfilerDest;
    private @MonotonicNonNull String asyncProfilerCmd;
    private final Map<QueryName, Checker<?>> checkerConsumers = new HashMap<>();
    private final FedMetricsListener fedListener = new FedMetricsListener() {
        @Override public void accept(FedMetrics metrics) {
            if (currTask != null && metrics.input == currTask.parsed()) {
                currentPlan = metrics.plan;
                fedMetrics = metrics;
            }
        }
    };
    private final MetricsListener planListener = new MetricsListener() {
        @Override public void accept(Metrics metrics) {
            if (currTask != null && metrics.plan == currentPlan)
                planMetrics = metrics;
        }
    };

    private File taskFile(String suffix) {
        if (currTask == null)
            throw new IllegalStateException("No currTask");
        String name = currTask.query().name()
                    + '-' + currTask.source()
                    + '-' + currTask.selector()
                    + '-' + currRep + suffix;
        return new File(destDir, name);
    }

    private BatchConsumer<?, ?> consumer(MeasureTask task, int rep) {
        if (currTask != null) throw new IllegalStateException("Concurrent measure()");
        currTask = task;
        currentPlan = null;
        currRep = rep;
        planMetrics = null;
        fedMetrics = null;
        BatchType<?> bt = msrOp.batchType;
        return consumer = switch (msrOp.consumer) {
            case COUNT -> consumer == null ? new Counter<>(bt) : (Counter<?>)consumer;
            case SAVE,SAVE_FIRST -> {
                var s = consumer == null ? new Serializer<>(bt, null, TSV, true)
                                         : (Serializer<?>) consumer;
                File f = taskFile(".tsv");
                if (msrOp.consumer == SAVE || rep == -1 || rep == 0) {
                    try {
                        s.output(new FileOutputStream(f), true);
                    } catch (Throwable t) {
                        log.error("Could not write to {}. Reverting to COUNT consumer", f, t);
                        yield new Counter<>(bt);
                    }
                } else {
                    s.output(new NullOutputStream(), true);
                }
                yield s;
            }
            case CHECK ->
                checkerConsumers.computeIfAbsent(currTask.query(),
                                                 k -> new Checker<>(bt, k));
        };
    }

    private static final class NullOutputStream extends OutputStream  {
        @Override public void write(int b) { }
    }

    private class Serializer<B extends Batch<B>>
            extends QueryRunner.Serializer<B, Serializer<B>>
            implements Orphan<Serializer<B>>  {
        public Serializer(BatchType<B> batchType, OutputStream os, SparqlResultFormat fmt,
                          boolean close) {
            super(batchType, os, fmt, close);
            takeOwnership(Measure.this);
        }

        @Override public Serializer<B> takeOwnership(Object o) {return takeOwnership0(o);}

        @Override protected void finish(@Nullable Throwable error) {
            saveMeasurement(error);
            super.finish(error);
        }
    }

    private class Counter<B extends Batch<B>>
            extends QueryRunner.BoundCounter<B, Counter<B>>
            implements Orphan<Counter<B>> {
        public Counter(BatchType<B> batchType) {
            super(batchType);
            takeOwnership(Measure.this);
        }
        @Override public Counter<B> takeOwnership(Object newOwner) {return takeOwnership0(newOwner);}
        @Override public void finish(@Nullable Throwable error) { saveMeasurement(error); }
    }

    private class Checker<B extends Batch<B>> extends QueryChecker<B>
            implements Orphan<QueryChecker<B>> {
        public Checker(BatchType<B> batchType, QueryName queryName) {
            super(batchType, queryName);
            takeOwnership(Measure.this);
        }

        @Override public QueryChecker<B> takeOwnership(Object newOwner) {
            return takeOwnership0(newOwner);
        }

        private void delete(String suffix) {
            if (currTask != null) {
                File file = taskFile(suffix);
                if (file.exists() && !file.delete())
                    log.error("Could not delete {}", file);
            }
        }
        @Override public void doFinish(@Nullable Throwable error) {
            try {
                if (error == null) {
                    if (isValid()) {
                        var query    = currTask == null ? null : currTask.query();
                        var selector = currTask == null ? null : currTask.selector();
                        var source   = currTask == null ? null : currTask.source();
                        log.info("No missing/unexpected rows among the {} received for rep {} of {}, sel={}, source={}",
                                 rows(), currRep, query, selector, source);
                        delete(".missing.tsv");
                        delete(".unexpected.tsv");
                    } else if (currTask != null) {
                        log.error("Bad results for rep {} of {}:\n{}",
                                  currRep, currTask, explanation());
                        error = new Exception("bad results: "+explanation().replace("\n", "\\n"));
                        var sink = new OutputStreamSink(null);
                        File file = taskFile(".missing.tsv");
                        var ser = ResultsSerializer.create(TSV).takeOwnership(this);
                        try (var os = new FileOutputStream(file)) {
                            sink.os = os;
                            ser.init(vars, vars, false);
                            ser.serializeHeader(sink);
                            forEachMissing((b, r) -> {
                                ser.serialize(b, sink, r);
                                return true;
                            });
                            ser.serializeTrailer(sink);
                        } catch (Throwable t) {
                            log.error("Failed to write {}", file, t);
                        } finally {
                            ser.recycle(this);
                        }
                        if (unexpected != null) {
                            file = taskFile(".unexpected.tsv");
                            try (var os = new FileOutputStream(file)) {
                                sink.os = os;
                                ser.init(vars, vars, false);
                                ser.serializeHeader(sink);
                                ser.serialize(unexpected, this, sink);
                                ser.serializeTrailer(sink);
                            } catch (Throwable t) {
                                log.error("Failed to write {}", file, t);
                            }
                        }
                    }
                }
            } finally {
                saveMeasurement(error);
            }
        }
    }

    private void saveMeasurement(@Nullable Throwable error) {
        var file = new File(destDir, "measurements.csv");
        try {
            if (fedMetrics == null && error == null)
                throw new IllegalStateException("no fedMetrics");
            if (planMetrics == null && error == null)
                throw new IllegalStateException("no planMetrics");
            var m = new Measurement(currTask, currRep, fedMetrics, planMetrics, error);
            MeasurementCsv.appendRow(file, m);
        } catch (IOException e) {
            log.error("Could not save measurements for {} of {} to {}", currRep, currTask, file);
        } finally {
            currTask = null;
            fedMetrics = null;
            planMetrics = null;
        }
    }
}
