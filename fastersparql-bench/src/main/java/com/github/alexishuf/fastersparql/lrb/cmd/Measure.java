package com.github.alexishuf.fastersparql.lrb.cmd;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.fed.FedMetrics;
import com.github.alexishuf.fastersparql.fed.FedMetricsListener;
import com.github.alexishuf.fastersparql.fed.Federation;
import com.github.alexishuf.fastersparql.lrb.query.PlanRegistry;
import com.github.alexishuf.fastersparql.lrb.query.QueryChecker;
import com.github.alexishuf.fastersparql.lrb.query.QueryName;
import com.github.alexishuf.fastersparql.lrb.query.QueryRunner;
import com.github.alexishuf.fastersparql.lrb.query.QueryRunner.BatchConsumer;
import com.github.alexishuf.fastersparql.lrb.sources.FederationHandle;
import com.github.alexishuf.fastersparql.model.SparqlResultFormat;
import com.github.alexishuf.fastersparql.model.rope.OutputStreamSink;
import com.github.alexishuf.fastersparql.operators.metrics.Metrics;
import com.github.alexishuf.fastersparql.operators.metrics.MetricsListener;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import com.github.alexishuf.fastersparql.sparql.results.serializer.ResultsSerializer;
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
import java.util.*;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import static com.github.alexishuf.fastersparql.lrb.cmd.MeasureOptions.ResultsConsumer.SAVE;
import static com.github.alexishuf.fastersparql.model.SparqlResultFormat.TSV;
import static java.lang.System.nanoTime;
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
        destDir = msrOp.destDir();
        var tasks = qryOp.queries().stream()
                         .map(q -> new MeasureTask(q, srcOp.selKind, srcOp.srcKind))
                         .collect(Collectors.toCollection(ArrayList::new));
        Collections.shuffle(tasks, msrOp.random);

        if (msrOp.builtinPlansJson)
            plans = PlanRegistry.parseBuiltin();
        if (msrOp.plansJson != null) {
            if (msrOp.builtinPlansJson)
                log.error("--builtin-plans-json and --plans-json cannot both be set");
            plans = PlanRegistry.parse(msrOp.plansJson);
        }
        try (var fedHandle = FederationHandle.builder(srcOp.dataDir)
                                             .selKind(srcOp.selKind)
                                             .srcKind(srcOp.srcKind)
                                             .subset(srcOp.lrbSources())
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
            warmup(tasks, fed);
            measure(tasks, fed);
        }
        return null;
    }

    private void warmup(List<MeasureTask> tasks, Federation client) {
        int rep = -1, taskIdx = 0;
        for (int remMs = msrOp.warmupSecs*1_000, ms; remMs > 0; remMs -= ms) {
            var task = tasks.get(taskIdx);
            log.info("Starting warmup {} of {}...", -rep, task);
            ms = run(client, task, rep, remMs);
            log.info("Warmup {} of {} in {}ms", -rep, task, ms);
            if ((taskIdx = (taskIdx+1) % tasks.size()) == 0) --rep;
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
                log.info("Starting rep {} of {}...", rep, task);
                int ms = run(client, task, rep, timeoutMs);
                log.info("Measured rep {} of {} in {}ms", rep, task, ms);
                spent[taskIdx] += ms;
                msrOp.cooldown();
            }
        }
    }

    private int run(Federation fed, MeasureTask task, int rep, int timeoutMs) {
        BatchConsumer consumer = consumer(task, rep);
        long start = nanoTime();
        try {
            BIt<?> it;
            if (plans != null) {
                Plan plan = requireNonNull(plans.createPlan(task.query()));
                currentPlan = plan;
                fedMetrics = new FedMetrics(fed, task.parsed());
                fedMetrics.plan = plan;
                plan.attach(planListener);
                it = plan.execute(msrOp.batchType);
            } else {
                it = fed.query(msrOp.batchType, task.parsed());
            }
            QueryRunner.drain(it, consumer, timeoutMs);
        } catch (Throwable t) {
            consumer.finish(t);
        }
        return (int)((nanoTime()-start)/1_000_000);
    }

    /* --- --- --- metrics collection --- --- --- */

    private @Nullable MeasureTask currTask;
    private int currRep = -1;
    private @Nullable Plan currentPlan;
    private @Nullable FedMetrics fedMetrics;
    private @Nullable Metrics planMetrics;
    private @MonotonicNonNull BatchConsumer consumer;
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

    private BatchConsumer consumer(MeasureTask task, int rep) {
        if (currTask != null) throw new IllegalStateException("Concurrent measure()");
        currTask = task;
        currentPlan = null;
        currRep = rep;
        planMetrics = null;
        fedMetrics = null;
        BatchType<?> bt = msrOp.batchType;
        return consumer = switch (msrOp.consumer) {
            case COUNT -> consumer == null ? new Counter(bt) : consumer;
            case SAVE,SAVE_FIRST -> {
                Serializer s = consumer instanceof Serializer se ? se : null;
                if (s == null)
                    consumer = s =new Serializer(bt, null, TSV, true);
                File f = taskFile(".tsv");
                if (msrOp.consumer == SAVE || rep == -1 || rep == 0) {
                    try {
                        s.output(new FileOutputStream(f), true);
                    } catch (Throwable t) {
                        log.error("Could not write to {}. Reverting to COUNT consumer", f, t);
                        yield new Counter(bt);
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

    private class Serializer extends QueryRunner.Serializer {
        public Serializer(BatchType<?> batchType, OutputStream os, SparqlResultFormat fmt,
                          boolean close) {
            super(batchType, os, fmt, close);
        }

        @Override public void finish(@Nullable Throwable error) {
            saveMeasurement(error);
            super.finish(error);
        }
    }

    private class Counter extends QueryRunner.BoundCounter {
        public Counter(BatchType<?> batchType) {
            super(batchType);
        }
        @Override public void finish(@Nullable Throwable error) { saveMeasurement(error); }
    }

    private class Checker<B extends Batch<B>> extends QueryChecker<B> {

        public Checker(BatchType<B> batchType, QueryName queryName) {
            super(batchType, queryName);
        }

        private void delete(String suffix) {
            if (currTask != null) {
                File file = taskFile(suffix);
                if (file.exists() && !file.delete())
                    log.error("Could not delete {}", file);
            }
        }
        @Override public void finish(@Nullable Throwable error) {
            try {
                if (error == null) {
                    if (isValid()) {
                        log.info("No missing/unexpected rows for rep {} of {}", currRep, currTask);
                        delete(".missing.tsv");
                        delete(".unexpected.tsv");
                    } else if (currTask != null) {
                        log.error("Bad results for rep {} of {}:\n{}",
                                  currRep, currTask, explanation());
                        var ser = ResultsSerializer.create(TSV);
                        var sink = new OutputStreamSink(null);
                        File file = taskFile(".missing.tsv");
                        try (var os = new FileOutputStream(file)) {
                            sink.os = os;
                            ser.init(vars, vars, false, sink);
                            forEachMissing((b, r) -> {
                                ser.serialize(b, r, 1, sink);
                                return true;
                            });
                            ser.serializeTrailer(sink);
                        } catch (Throwable t) {
                            log.error("Failed to write {}", file, t);
                        }
                        file = taskFile(".unexpected.tsv");
                        try (var os = new FileOutputStream(file)) {
                            sink.os = os;
                            ser.init(vars, vars, false, sink);
                            ser.serialize(unexpected, sink);
                            ser.serializeTrailer(sink);
                        } catch (Throwable t) {
                            log.error("Failed to write {}", file, t);
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