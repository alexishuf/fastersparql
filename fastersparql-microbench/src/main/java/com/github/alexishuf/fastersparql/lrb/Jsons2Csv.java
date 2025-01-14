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
import java.util.*;
import java.util.concurrent.Callable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.Math.round;
import static java.lang.String.format;
import static java.lang.System.arraycopy;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME;
import static java.util.Arrays.copyOf;
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

    @Option(names = {"-a", "--add-root"}, arity = "0..*",
            description = "Any directory will be recursively scanned and iterations " +
                    "found in JSON files will be added to iterations already parsed from " +
                    "other JSON files. I.e., this will add iteration measurements as a new fork," +
                    "instead of replacing all data.")
    private List<File> addRoots;

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


    private static final class PrimaryMetric {
        private final double score;
        private final double scoreError;
        private final double[] scoreConfidence;
        private final Map<Double, Double> scorePercentiles;
        private final String scoreUnit;
        private double[][] rawData;

        private PrimaryMetric(double score, double scoreError,
                              double[] scoreConfidence,
                              Map<Double, Double> scorePercentiles,
                              String scoreUnit, double[][] rawData) {
            this.score = score;
            this.scoreError = scoreError;
            this.scoreConfidence = scoreConfidence;
            this.scorePercentiles = scorePercentiles;
            this.scoreUnit = scoreUnit;
            this.rawData = rawData;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) return true;
            if (obj == null || obj.getClass() != this.getClass()) return false;
            var that = (PrimaryMetric) obj;
            return Double.doubleToLongBits(this.score) == Double.doubleToLongBits(that.score) &&
                    Double.doubleToLongBits(this.scoreError) == Double.doubleToLongBits(that.scoreError) &&
                    Arrays.equals(this.scoreConfidence, that.scoreConfidence) &&
                    Objects.equals(this.scorePercentiles, that.scorePercentiles) &&
                    Objects.equals(this.scoreUnit, that.scoreUnit) &&
                    Arrays.deepEquals(this.rawData, that.rawData);
        }

        @Override
        public int hashCode() {
            return Objects.hash(score, scoreError, Arrays.hashCode(scoreConfidence),
                    scorePercentiles, scoreUnit, Arrays.deepHashCode(rawData));
        }

        @Override
        public String toString() {
            return "PrimaryMetric[" +
                    "score=" + score + ", " +
                    "scoreError=" + scoreError + ", " +
                    "scoreConfidence=" + Arrays.toString(scoreConfidence) + ", " +
                    "scorePercentiles=" + scorePercentiles + ", " +
                    "scoreUnit=" + scoreUnit + ", " +
                    "rawData=" + Arrays.deepToString(rawData) + ']';
        }
    }

    private static final class JmhResults {
        transient File originFile;
        transient long lastModified;
        transient boolean[][] oom = new boolean[0][];
        transient boolean[][] timeout = new boolean[0][];
        transient boolean[][] failed = new boolean[0][];
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

        void setIterations(int forks, int measIterations) {
            this.forks                 = forks;
            this.measurementIterations = measIterations;
            oom     = copyOf(oom,     forks);
            failed  = copyOf(failed,  forks);
            timeout = copyOf(timeout, forks);
            boolean[] empty = new boolean[0];
            for (int i = 0; i < forks; i++) {
                oom[i]     = copyOf(requireNonNullElse(oom[i],     empty), measIterations);
                failed[i]  = copyOf(requireNonNullElse(failed[i],  empty), measIterations);
                timeout[i] = copyOf(requireNonNullElse(timeout[i], empty), measIterations);
            }
            double[][] rawData = primaryMetric.rawData;
            int actualForks = rawData.length;
            if (forks >  actualForks) {
                primaryMetric.rawData = rawData = Arrays.copyOf(rawData, forks);
                for (int i = actualForks; i < forks; i++)
                    Arrays.fill(rawData[i] = new double[measIterations], Double.NaN);
            }

        }
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
                                format("%,9.3f", timeoutMs/1_000.0),
                                format("%-7s", p.queries),
                                format("%-27s", p.srcKind+(p.unionSource?"(union)": "")),
                                p.flowModel.name().substring(0, 2),
                                format("%-10s", p.batchKind));
                    }
                    var notTimeout = forkData.length > 1 ? "false"  : "";
                    for (int iteration = 0; iteration < forkData.length; iteration++) {
                        boolean timeout = r.timeout[fork][iteration] || iteration >= toutIteration;
                        boolean failed = r.failed[fork][iteration];
                        boolean oom = r.oom[fork][iteration];
                        if ((Double.isNaN(forkData[iteration]) && !(timeout|failed|oom))
                                 && (fork > 0 || iteration > 0)) {
                            continue;
                        }
                        out.append(shared);
                        out.append(Integer.toString(fork)).append(',');
                        out.append(Integer.toString(iteration)).append(',');
                        double ms = iteration >= toutIteration ? timeoutMs : forkData[iteration];
                        out.append(Double.toString(ms)).append(',');
                        out.append(timeout ? "true" : notTimeout).append(',');
                        out.append(Boolean.toString(r.failed[fork][iteration])).append(',');
                        out.append(Boolean.toString(r.oom[fork][iteration])).append("\r\n");
                    }
                }
            }
        }
        return null;
    }

    private Map<Params, JmhResults> collectResults() throws IOException {
        var collector = new ResultsCollector();
        for (File dir : roots)
            collector.visit(dir);
        if (addRoots != null) {
            for (File dir : addRoots)
                collector.visitAdd(dir);
        }
        return collector.param2res;
    }

    private static final class ResultsCollector {
        private static final String PARAMS_LINE_PREFIX = "# Parameters: (";
        private static final String FAILED = "<forked VM failed with exit code ";
        private static final String EXIT_CODE_OOM  = "exit code 23";
        private static final Pattern EXIT_CODE_HANG = Pattern.compile("exit code (27|137)");
        private static final Pattern OOM = Pattern.compile("OutOfMemoryError|Java heap space|OOM-killed");
        private static final Pattern EXCEPTION = Pattern.compile("(IndexOutOfBounds|IllegalArgument|NumberFormat|QueryResultParse|InvalidSparqlResults|JsonParse|InvalidTerm)Exception|(OutOfMemory)Error");
        private static final Pattern ITERATION = Pattern.compile("^Iteration +(\\d+):(?: +(\\d+\\.?\\d*) +ms/op)?");
        private static final Pattern ITERATION_TIME = Pattern.compile("^(\\d+\\.?\\d*) +ms/op$");
        private static final Pattern FORK = Pattern.compile("^# Fork: (\\d+) +of +(\\d+)");
        private static final Pattern ITERATIONS = Pattern.compile("^# +Measurement: +(\\d+) +iterations");
        private static final Pattern BATCH_KIND = Pattern.compile("[( ]batchKind = (\\w+)");
        private static final Pattern FLOW_MODEL = Pattern.compile("[( ]flowModel = (\\w+)");
        private static final Pattern QUERIES = Pattern.compile("[( ]queries = ([^ ,]+)");
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

        void visitAdd(File dir) throws IOException {
            File[] files = dir.listFiles();
            if (files == null)
                throw new IOException("Could not list files in "+dir);
            for (File f : files) {
                if (f.getName().toLowerCase().endsWith(".json")) {
                    visitJsonAdd(f);
                } else if (f.isDirectory()) {
                    visitAdd(f);
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
                    results.setIterations(results.forks, results.measurementIterations);
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

        private void visitJsonAdd(File f) throws IOException {
            if (f.length() == 0)
                return; // empty
            try (var reader = new FileReader(f, UTF_8)) {
                List<JmhResults> list = gson.fromJson(reader, LIST_OF_RESULTS);
                for (var results : list) {
                    results.originFile   = f;
                    results.lastModified = f.lastModified();
                    results.setIterations(results.forks, results.measurementIterations);
                    JmhResults old = param2res.get(results.params);
                    if (old == null || old.lastModified < results.lastModified) {
                        if (old == null) {
                            param2res.put(results.params, results);
                        } else {
                            double[][] oForks =     old.primaryMetric.rawData;
                            double[][] nForks = results.primaryMetric.rawData;
                            log.info("Adding {} forks, {} iterations from {} for {}",
                                     nForks.length,
                                     Arrays.stream(nForks).mapToInt(a -> a.length).sum(),
                                     f, results.params);
                            var merged = copyOf(oForks, oForks.length+nForks.length);
                            arraycopy(nForks, 0, merged, oForks.length, nForks.length);
                            PrimaryMetric opm = old.primaryMetric;
                            old.primaryMetric = new PrimaryMetric(
                                    opm.score, opm.scoreError, opm.scoreConfidence,
                                    opm.scorePercentiles, opm.scoreUnit, merged
                            );
                        }
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
            var corrJson = f.toPath().resolveSibling(f.getName().replace(".log", ".json")).toFile();
            try (var reader = new BufferedReader(new FileReader(f))) {
                boolean oom = false;
                boolean exception = false;
                int fork = 0, forks = 0, iteration = -1, measIterations = 1;
                double itTime = Double.NaN;
                Matcher itMatcher = ITERATION.matcher("");
                Matcher itTimeMatcher = ITERATION_TIME.matcher("");
                Matcher forkMatcher = FORK.matcher("");
                Matcher itersMatcher = ITERATIONS.matcher("");
                Matcher oomMatcher = OOM.matcher("");
                Matcher exceptionMatcher = EXCEPTION.matcher("");
                Params params = null;
                for (String line; (line=reader.readLine()) != null; ) {
                    if (forkMatcher.reset(line).find()) {
                        if (params != null && iteration >= 0
                                && (!Double.isNaN(itTime) || oom || exception)) {
                            updateOrAddResults(f, iteration, params, corrJson, forks,
                                               measIterations, fork, itTime,
                                               exception, oom, false);
                        }
                        fork = Integer.parseInt(forkMatcher.group(1)) - 1;
                        forks = Integer.parseInt(forkMatcher.group(2));
                        iteration = -1;
                        oom = exception = false;
                        itTime = Double.NaN;
                    } else if (itersMatcher.reset(line).find()) {
                        measIterations = Integer.parseInt(itersMatcher.group(1));
                    } else if (line.startsWith(PARAMS_LINE_PREFIX)) {
                        params = null;
                        oom = exception = false;
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
                    } else if (oomMatcher.reset(line).find()) {
                        oom = true;
                    } else if (exceptionMatcher.reset(line).find()) {
                        exception = true;
                    } else if (itTimeMatcher.reset(line).find()) {
                        itTime = Double.parseDouble(itTimeMatcher.group(1));
                    } else if (line.startsWith(FAILED) && params != null) {
                        boolean timeout = EXIT_CODE_HANG.matcher(line).find();
                        if (line.contains(EXIT_CODE_OOM))
                            oom = true;
                        updateOrAddResults(f, iteration, params, corrJson, forks,
                                           measIterations, fork, itTime, exception, oom, timeout);
                        iteration = -1;
                        oom = exception = false;
                    } else if (itMatcher.reset(line).find() && params  != null) {
                        if (iteration >= 0) {
                            updateOrAddResults(f, iteration, params, corrJson, forks,
                                               measIterations, fork, itTime,
                                               exception, oom, false);
                        }
                        iteration = Integer.parseInt(itMatcher.group(1))-1;
                        exception = false;
                        if (itMatcher.group(2) != null) {
                            itTime = Double.parseDouble(itMatcher.group(2));
                            updateOrAddResults(f, iteration, params, corrJson,
                                               forks, measIterations, fork, itTime,
                                               exception, oom, false);
                            iteration = -1;
                        } else {
                            itTime = Double.NaN;
                        }
                    }
                }
                if (params != null && iteration >= 0 && (!Double.isNaN(itTime)|oom|exception)) {
                    updateOrAddResults(f, iteration, params, corrJson, forks,
                            measIterations, fork, itTime,
                            exception, oom, false);
                }
            } catch (IOException e) {
                log.warn("Ignoring {} reading from {}: {}", e.getClass().getSimpleName(),
                         f.getAbsolutePath(), e.getMessage());
            }
        }

        private void updateOrAddResults(File f, int iteration, Params params, File corrJson,
                                        int forks, int measurementIterations, int fork,
                                        double itTime,
                                        boolean exception, boolean oom, boolean timeout) {
            int iterationOrZero = Math.max(0, iteration);
            var results = param2res.getOrDefault(params, null);
            boolean novel = false;
            if (results == null
                    || (results.originFile.getName().endsWith(".log")
                    && results.originFile.lastModified() < f.lastModified())) {
                novel = true;
                results                 = new JmhResults();
                results.originFile      = f;
                results.lastModified    = f.lastModified();
                results.benchmark       = "";
                results.jvmArgs         = List.of();
                results.jvm             = "";
                results.jdkVersion      = "";
                results.vmVersion       = "";
                results.warmupTime      = "";
                results.measurementTime = "";
                results.params          = params;
                double[][] rawData = new double[forks][];
                for (int i = 0; i < forks; i++)
                    Arrays.fill(rawData[i] = new double[measurementIterations], Double.NaN);
                results.primaryMetric   = new PrimaryMetric(Double.NaN, Double.NaN,
                        new double[]{Double.NaN, Double.NaN},
                        Map.of(), "ms/op", rawData);
            }
            if (results.originFile == f || results.originFile.equals(corrJson)) {
                results.setIterations(forks, measurementIterations);
                results.failed [fork][iterationOrZero] = exception|oom|timeout;
                results.oom    [fork][iterationOrZero] = oom;
                results.timeout[fork][iterationOrZero] = timeout;
                novel |= exception|oom|timeout;
                if (iteration >= 0) {
                    double[][] rawData = results.primaryMetric.rawData;
                    int dstFork = -1;
                    for (int i = 0; i <= fork; i++) {
                        if (iteration >= rawData[i].length)
                            continue; // #iterations mismatch
                        if (round(rawData[i][iteration]*1000) == round(itTime*1000))
                            dstFork = Integer.MAX_VALUE; // itTime already present in json
                        if (Double.isNaN(rawData[i][iteration]) && dstFork < 0)
                            dstFork = i; // itTime could be lost
                    }
                    if (dstFork >= 0 && dstFork < rawData.length) {
                        novel = true;
                        rawData[dstFork][iteration] = itTime;
                    }
                }
                if (novel) {
                    param2res.put(params, results);
                    String type = oom ? "OOM"
                            : (timeout ? "Timeout"
                            : (exception ? "Exception" : "non-failure"));
                    log.info("Recorded {} ms={} for fork {}, iteration {} of {} from {}",
                             type, String.format("%.2fms", itTime),
                             fork, iteration, params, f.getPath());
                }
            }
        }
    }
}
