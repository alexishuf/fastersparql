package com.github.alexishuf.fastersparql.lrb;


import com.github.alexishuf.fastersparql.FlowModel;
import com.github.alexishuf.fastersparql.lrb.cmd.MeasureOptions;
import com.github.alexishuf.fastersparql.lrb.sources.SourceKind;
import com.google.gson.Gson;
import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import fastersparql.QueryBench;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.io.*;
import java.lang.reflect.Type;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME;
import static java.util.Objects.requireNonNullElse;

@Command(name = "json2csv", showDefaultValues = true, mixinStandardHelpOptions = true,
         description = "Scans directories for JSON files produced by JMH using -rff json, " +
                 "select the last modified for any combination of benchmark parameters and " +
                 "convert the score of each iteration into a row in the resulting CSV file.")
public class Jsons2Csv implements Callable<Void> {
    private static final Logger log = LoggerFactory.getLogger(Jsons2Csv.class);

    @Option(names = {"-o", "--output"}, required = true,
            description = "Destination csv file, which will be overwritten")
    private File destFile;
    @Parameters(paramLabel = "DIRS", arity = "1..*",
                description = "A directory to be recursively scanned for JSON files")
    private List<File> roots;

    private record Params(
            String queries,
            SourceKind srcKind,
            QueryBench.SelectorKindType selKind,
            @Nullable Boolean builtinPlans,
            @Nullable Boolean crossSourceDedup,
            MeasureOptions.BatchKind batchKind,
            FlowModel flowModel,
            @Nullable Boolean weakenDistinct,
            @Nullable Boolean thermalCooldown,
            boolean unionSource
    ) { }


    private record PrimaryMetric(
            double score, double scoreError,
            double[] scoreConfidence,
            Map<Double, Double> scorePercentiles,
            String scoreUnit,
            double[][] rawData
    ) { }

    private static final class JmhResults {
        transient File originFile;
        transient long lastModified;
        transient boolean oom;
        transient boolean failed;
        String benchmark;
        String jvm;
        String jdkVersion;
        String vmVersion;
        String warmupTime;
        String measurementTime;
        int threads;
        int forks;
        int warmupIterations;
        int measurementIterations;
        List<String> jvmArgs;
        Params params;
        PrimaryMetric primaryMetric;
    }

    public static void main(String[] args) {
        new CommandLine(new Jsons2Csv()).execute(args);
    }

    @Override public Void call() throws Exception {
        Map<Params, JmhResults> param2results = collectResults();
        try (var out = new PrintStream(destFile)) {
            out.println("originDatetime,method,jvm,jdkVersion,vmVersion,warmupTime,warmupIterations,measurementTime,measurementIterations,threads,forks,jvmArgs,queries,source,selector,builtinPlans,crossSourceDedup,batch,flow,weakenDistinct,thermalCooldown,unionSource,fork,iteration,ms,timedout,failed,oom\r\n");
            StringBuilder shared = new StringBuilder();
            for (var r : param2results.values()) {
                shared.setLength(0);
                var dt = LocalDateTime.ofEpochSecond(r.lastModified / 1_000L,
                        (int)(r.lastModified%1_000),
                        ZoneOffset.ofHours(0));
                shared.append(dt.format(ISO_LOCAL_DATE_TIME)).append(',');
                shared.append(r.benchmark).append(',');
                shared.append('"').append(r.jvm.replaceAll("\"", "")).append("\",");
                shared.append('"').append(r.jdkVersion.replaceAll("\"", "")).append("\",");
                shared.append('"').append(r.vmVersion.replaceAll("\"", "")).append("\",");
                shared.append(r.warmupTime).append(',');
                shared.append(r.warmupIterations).append(',');
                shared.append(r.measurementTime).append(',');
                shared.append(r.measurementIterations).append(',');
                shared.append(r.threads).append(',');
                shared.append(r.forks).append(',');
                String jvmArgs = String.join(" ", r.jvmArgs).replaceAll("\"", "\"\"");
                shared.append('"').append(jvmArgs).append("\",");
                Params p = r.params;
                shared.append(p.queries).append(',');
                shared.append(p.srcKind).append(',');
                shared.append(p.selKind).append(',');
                shared.append(requireNonNullElse(p.builtinPlans, true)).append(',');
                shared.append(requireNonNullElse(p.crossSourceDedup, true)).append(',');
                shared.append(p.batchKind).append(',');
                shared.append(p.flowModel).append(',');
                shared.append(requireNonNullElse(p.weakenDistinct, false)).append(',');
                shared.append(requireNonNullElse(p.thermalCooldown, true)).append(',');
                shared.append(p.unionSource).append(',');
                if (!r.primaryMetric.scoreUnit.equals("ms/op"))
                    throw new IllegalArgumentException("Expected scoreUnit=ms/op");
                for (int fork = 0; fork < r.primaryMetric.rawData.length; fork++) {
                    double[] forkData = r.primaryMetric.rawData[fork];
                    if (forkData.length == 0) continue;
                    int toutIteration = Integer.MAX_VALUE;
                    double timeoutMs = -1;
                    for (int i = 0; i < forkData.length-1 && i < toutIteration; i++) {
                        timeoutMs = forkData[i];
                        boolean isTimeout = true;
                        for (int after = i+1; isTimeout && after < forkData.length; after++)
                            isTimeout = forkData[after] < Math.min(timeoutMs/1_000.0, 10);
                        if (isTimeout)
                            toutIteration = i;
                    }
                    if (toutIteration < forkData.length) {
                        log.info("Timeout at {}s for {} src={} flow={} batch={}",
                                format("%,7.3f", timeoutMs/1_000.0),
                                format("%-7s", p.queries),
                                format("%-25s", p.srcKind+(p.unionSource?"(union)": "")),
                                p.flowModel.name().substring(0, 2),
                                format("%-10s", p.batchKind));
                    }
                    var notTimeout = forkData.length > 1 ? "false"  : "";
                    for (int iteration = 0; iteration < forkData.length; iteration++) {
                        out.append(shared);
                        out.append(Integer.toString(fork)).append(',');
                        out.append(Integer.toString(iteration)).append(',');
                        double ms = iteration >= toutIteration ? timeoutMs : forkData[iteration];
                        out.append(Double.toString(ms)).append(',');
                        out.append(iteration >= toutIteration ? "true" : notTimeout);
                        out.append(",false,false\r\n");
                    }
                }
                if (r.failed) {
                    out.append(shared);
                    out.append(Integer.toString(r.primaryMetric.rawData.length)); //fork
                    out.append(",0,,false,true,"); // ,iteration,ms,timedout,failed,
                    out.append(Boolean.toString(r.oom)).append("\r\n");
                }
            }
        }
        return null;
    }

    private Map<Params, JmhResults> collectResults() throws IOException {
        var collector = new ResultsCollector();
        for (File dir : roots)
            collector.visit(dir);
        return collector.param2res;
    }

    private static final class ResultsCollector {
        private static final String PARAMS_LINE_PREFIX = "# Parameters: (";
        private static final String OOM = "OutOfMemoryError";
        private static final String FAILED = "<forked VM failed with exit code ";
        private static final Pattern BATCH_KIND = Pattern.compile("[( ]batchKind = (\\w+)");
        private static final Pattern FLOW_MODEL = Pattern.compile("[( ]flowModel = (\\w+)");
        private static final Pattern QUERIES = Pattern.compile("[( ]queries = (\\w+)");
        private static final Pattern SRC_KIND = Pattern.compile("[( ]srcKind = (\\w+)");
        private static final Pattern UNION_SOURCE = Pattern.compile("[( ]unionSource = (\\w+)");
        private static final Pattern JSON_SUFF = Pattern.compile("\\.json$");
        private static final Type LIST_OF_RESULTS = new TypeToken<List<JmhResults>>(){}.getType();
        private final Map<Params, JmhResults> param2res = new HashMap<>();
        private final Gson gson = new Gson();
        void visit(File dir) throws IOException {
            File[] files = dir.listFiles();
            if (files == null)
                throw new IOException("Could not list files in "+dir);
            for (File f : files) {
                if (f.getName().toLowerCase().endsWith(".json")) {
                    visitJson(f);
                    visitLog(new File(dir, JSON_SUFF.matcher(f.getName()).replaceAll(".log")));
                } else if (f.isDirectory()) {
                    visit(f);
                }
            }
        }

        private void visitJson(File f) throws IOException {
            if (f.length() == 0)
                return; // empty
            try (var reader = new FileReader(f, UTF_8)) {
                List<JmhResults> list = gson.fromJson(reader, LIST_OF_RESULTS);
                for (var results : list) {
                    results.originFile   = f;
                    results.lastModified = f.lastModified();
                    JmhResults old = param2res.get(results.params);
                    if (old == null || old.lastModified < results.lastModified) {
                        if (old != null)
                            log.info("Replacing {} with {}", old.originFile, f);
                        param2res.put(results.params, results);
                    }
                }
            } catch (JsonSyntaxException e) {
                log.warn("Ignoring invalid JSON at {}", f.getAbsolutePath(), e);
            } catch (JsonIOException e) {
                throw new IOException("Failed to read from "+ f.getAbsolutePath(), e);
            }
        }

        private void visitLog(File f) {
            if (!f.isFile() || f.length() == 0)
                return;
            try (var reader = new BufferedReader(new FileReader(f))) {
                boolean oom = false;
                Params params = null;
                for (String line; (line=reader.readLine()) != null; ) {
                    if (line.startsWith(PARAMS_LINE_PREFIX)) {
                        params = null;
                        oom = false;
                        Matcher m = BATCH_KIND.matcher(line);
                        if (!m.find())
                            continue;
                        var batchKind = MeasureOptions.BatchKind.valueOf(m.group(1));
                        if (!(m = FLOW_MODEL.matcher(line)).find())
                            continue;
                        var flowModel = FlowModel.valueOf(m.group(1));
                        if (!(m=QUERIES.matcher(line)).find())
                            continue;
                        String queries = m.group(1);
                        if (!(m=SRC_KIND.matcher(line)).find())
                            continue;
                        SourceKind srcKind;
                        try {
                            srcKind = SourceKind.valueOf(m.group(1));
                        } catch (IllegalArgumentException e) {
                            log.warn("Ignoring bogus srcKind={} at {}", m.group(1), f.getPath());
                            continue;
                        }
                        if (!(m=UNION_SOURCE.matcher(line)).find())
                            continue;
                        boolean unionSource = Boolean.TRUE.equals(Boolean.valueOf(m.group(1)));
                        params = new Params(queries, srcKind, null,
                                null, null, batchKind, flowModel,
                                null, null, unionSource);
                    } else if (line.contains(OOM)) {
                        oom = true;
                    } else if (line.startsWith(FAILED) && params != null) {
                        var results             = new JmhResults();
                        results.originFile      = f;
                        results.lastModified    = f.lastModified();
                        results.failed          = true;
                        results.oom             = oom;
                        results.benchmark       = "";
                        results.jvmArgs         = List.of();
                        results.jvm             = "";
                        results.jdkVersion      = "";
                        results.vmVersion       = "";
                        results.warmupTime      = "";
                        results.measurementTime = "";
                        results.params          = params;
                        results.primaryMetric   = new PrimaryMetric(Double.NaN, Double.NaN,
                                new double[0], Map.of(), "ms/op",
                                new double[0][]);
                        if (param2res.getOrDefault(params, null) == null) {
                            param2res.put(params, results);
                            log.info("Recorded Forked VM failure for {} from {}",
                                     params, f.getPath());
                        }
                        params = null;
                        oom = false;
                    }
                }
            } catch (IOException e) {
                log.warn("Ignoring {} reading from {}: {}", e.getClass().getSimpleName(),
                         f.getAbsolutePath(), e.getMessage());
            }
        }
    }
}
