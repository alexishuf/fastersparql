package com.github.alexishuf.fastersparql.lrb;


import com.github.alexishuf.fastersparql.FlowModel;
import com.github.alexishuf.fastersparql.lrb.sources.SourceKind;
import com.google.gson.Gson;
import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Type;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME;

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
            boolean builtinPlans,
            boolean crossSourceDedup,
            QueryBench.BatchKind batchKind,
            FlowModel flowModel,
            boolean weakenDistinct,
            boolean thermalCooldown,
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
            out.println("originDatetime,method,jvm,jdkVersion,vmVersion,warmupTime,warmupIterations,measurementTime,measurementIterations,threads,forks,jvmArgs,queries,source,selector,builtinPlans,crossSourceDedup,batch,flow,weakenDistinct,thermalCooldown,unionSource,fork,iteration,ms\r\n");
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
                shared.append(r.params.queries).append(',');
                shared.append(r.params.srcKind).append(',');
                shared.append(r.params.selKind).append(',');
                shared.append(r.params.builtinPlans).append(',');
                shared.append(r.params.crossSourceDedup).append(',');
                shared.append(r.params.batchKind).append(',');
                shared.append(r.params.flowModel).append(',');
                shared.append(r.params.weakenDistinct).append(',');
                shared.append(r.params.thermalCooldown).append(',');
                shared.append(r.params.unionSource).append(',');
                if (!r.primaryMetric.scoreUnit.equals("ms/op"))
                    throw new IllegalArgumentException("Expected scoreUnit=ms/op");
                for (int fork = 0; fork < r.primaryMetric.rawData.length; fork++) {
                    double[] forkData = r.primaryMetric.rawData[fork];
                    for (int iteration = 0; iteration < forkData.length; iteration++) {
                        out.append(shared);
                        out.append(Integer.toString(fork)).append(',');
                        out.append(Integer.toString(iteration)).append(',');
                        out.append(Double.toString(forkData[iteration])).append("\r\n");
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
        return collector.param2res;
    }

    private static final class ResultsCollector {
        private static final Type LIST_OF_RESULTS = new TypeToken<List<JmhResults>>(){}.getType();
        private final Map<Params, JmhResults> param2res = new HashMap<>();
        private final Gson gson = new Gson();
        void visit(File dir) throws IOException {
            File[] files = dir.listFiles();
            if (files == null)
                throw new IOException("Could not list files in "+dir);
            for (File f : files) {
                if (f.getName().toLowerCase().endsWith(".json")) {
                    try (var reader = new FileReader(f, UTF_8)) {
                        List<JmhResults> list = gson.fromJson(reader, LIST_OF_RESULTS);
                        for (var results : list) {
                            results.originFile   = f;
                            results.lastModified = f.lastModified();
                            JmhResults old = param2res.get(results.params);
                            if (old == null || old.lastModified < results.lastModified)
                                param2res.put(results.params, results);
                        }
                    } catch (JsonSyntaxException e) {
                        log.warn("Ignoring invalid JSON at {}", f.getAbsolutePath(), e);
                    } catch (JsonIOException e) {
                        throw new IOException("Failed to read from "+f.getAbsolutePath(), e);
                    }
                } else if (f.isDirectory()) {
                    visit(f);
                }
            }
        }
    }
}
