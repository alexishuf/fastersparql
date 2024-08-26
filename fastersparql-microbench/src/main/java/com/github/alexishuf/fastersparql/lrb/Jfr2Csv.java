package com.github.alexishuf.fastersparql.lrb;

import com.github.alexishuf.fastersparql.FlowModel;
import com.github.alexishuf.fastersparql.lrb.cmd.MeasureOptions.BatchKind;
import com.github.alexishuf.fastersparql.lrb.sources.SourceKind;
import jdk.jfr.consumer.*;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.System.nanoTime;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNullElse;
import static java.util.stream.Collectors.joining;

@Command(name = "jfr2csv", showDefaultValues = true, mixinStandardHelpOptions = true,
         description = "Aggregate multiple jfr files that originate from the same QueryBench " +
                 "execution into one set of events and generate a csv that relates the relative " +
                 "CPU time of specific methods")
public class Jfr2Csv implements Callable<Void> {
    private static final Logger log = LoggerFactory.getLogger(Jfr2Csv.class);

    @Option(names = {"-o", "--output"}, required = true,
            description = "Destination csv file, which will be overwritten")
    private File destFile;

    @Parameters(paramLabel = "DIRS", arity = "1..*", description = "Directories that will be " +
            "recursively scanned for JFR files matching the naming scheme used by QueryBench")
    private List<File> roots;

    public static void main(String[] args) {
        new CommandLine(new Jfr2Csv()).execute(args);
    }

    @Override public Void call() throws Exception {
        checkOutputPaths();
        checkInputsPaths();
        long t0 = nanoTime();
        Map<Params, List<File>> params2jfrFiles = new HashMap<>();
        new JfrFinder(params2jfrFiles).visitAll(roots);
        int jfrFilesCount = params2jfrFiles.values().stream().mapToInt(List::size).sum();
        var duration = Duration.ofNanos(nanoTime() - t0);
        log.info("Found {} JFR files in {}s", jfrFilesCount, duration.toMillis()/1_000.0);

        t0 = nanoTime();
        List<TasksWeights> weightsList = Collections.synchronizedList(new ArrayList<>());
        params2jfrFiles.entrySet().parallelStream()
                .forEach(e -> makeTasksWeights(e.getKey(), e.getValue(), weightsList));
        duration = Duration.ofNanos(nanoTime() - t0);
        log.info("Processed {} JFR files in {}m {}s", jfrFilesCount, duration.toMinutes(),
                 duration.toSecondsPart() + duration.toMillisPart()/1_000.0);
        log.info("Writing to {}...", destFile);
        writeOutputCsv(weightsList);
        return null;
    }

    private void writeOutputCsv(List<TasksWeights> infos) throws IOException {
        try (var w = new FileWriter(destFile, UTF_8)) {
            w.append("queries,source,batch,flow,unionSource,origin");
            for (Task task : Task.ALL)
                w.append(',').append(task.headerName());
            w.append("\r\n");
            for (TasksWeights weights : infos) {
                var p = weights.params;
                w.append(p.queries).append(',')
                        .append(p.sourceKind.name()).append(',')
                        .append(p.batchKind.name()).append(',')
                        .append(p.flowModel.name()).append(',')
                        .append(String.valueOf(p.unionSource)).append(',')
                        .append(weights.origin);
                for (Task task : Task.ALL)
                    w.append(',').append(Double.toString(weights.get(task)));
                w.append("\r\n");
            }
        }
    }

    private void checkInputsPaths() throws IOException {
        String firstError = null;
        for (File root : roots) {
            if (!root.exists()) {
                log.error("{} does not exist", root);
                firstError = requireNonNullElse(firstError, root+" does not exist");
            }
            if (!root.isDirectory()) {
                log.error("{} is not a directory", root);
                firstError = requireNonNullElse(firstError, root+" is not a directory");
            }
        }
        if (firstError != null)
            throw new IOException(firstError);
    }

    private void checkOutputPaths() throws IOException {
        if (destFile.exists()) {
            if (!destFile.isFile())
                throw new IOException("--output "+destFile+" exists as non-file");
            File parent = destFile.getAbsoluteFile().getParentFile();
            if (!parent.isDirectory())
                throw new IOException(parent+" is not a directory");
            if (!parent.canWrite())
                throw new IOException("no write permissions on directory "+parent);
            log.info("Will overwrite {}", destFile);
        }
    }

    private static void makeTasksWeights(Params params, List<File> jfrFiles,
                                         List<TasksWeights> syncWeigtsList) {
        var allCounter = new SampleCounter(params, "All");
        var tmp = new StringBuilder();
        int nDone = 0;
        for (File file : jfrFiles) {
            log.info("Processing {}...", file  );
            try (var rec = new RecordingFile(file.toPath())) {
                var fileCounter = new SampleCounter(params, file.toString());
                while (rec.hasMoreEvents()) {
                    var event = rec.readEvent();
                    if (event.getEventType().getName().equals("jdk.ExecutionSample")) {
                        allCounter.sample();
                        fileCounter.sample();
                        for (var task : Task.EXCLUDE) {
                            if (task.matches(params, event, tmp)) {
                                allCounter.excluded(task);
                                fileCounter.excluded(task);
                            }
                        }
                        for (var task : Task.INCLUDE) {
                            if (task.matches(params, event, tmp)) {
                                allCounter.included(task);
                                fileCounter.included(task);
                            }
                        }
                    }
                }
                syncWeigtsList.add(fileCounter.makeInfo());
            } catch (IOException e) {
                log.error("Failed to read JFR data from {}", file);
                throw new RuntimeException(e);
            }
            log.info("Processed {}/{} .jfr files for {}", ++nDone, jfrFiles.size(), params);
        }
        syncWeigtsList.add(allCounter.makeInfo());
    }

    enum Task {
        GC,
        TICK,
        WATCHDOG,
        STRING2ID,
        ID2STRING,
        TP_REBIND,
        REBIND,
        CANCEL,
        TASK_TAKE,
        TASK_PUT,
        VTHREAD_SWITCH,
        PAGE_FAULT,
        LOCK,
        UNLOCK,
        ATOMICS,
        BIT_CONS_PARK,
        BIT_PROD_PARK,
        BATCH_CONVERSION,
        BATCH_QUICK_APPEND,
        BATCH_APPEND,
        BATCH_COPY,
        BATCH_CREATE,
        BATCH_RECYCLE,
        ROPE_INTERN,
        PUT_TERM,
        PUT_TERM_COPY,
        PUT_TERM_PAGE_FAULT,
        ALLOC_CREATE,
        ALLOC_OFFER,
        WEAK_DEDUP,
        DEDUP,
        FILTER_IN_PLACE,
        PROJECT_IN_PLACE;

        private static final Task[] ALL = Task.values();
        private static final Task[] EXCLUDE = Arrays.stream(ALL).filter(Task::shouldExclude).toArray(Task[]::new);
        private static final Task[] INCLUDE = Arrays.stream(ALL).filter(Task::shouldInclude).toArray(Task[]::new);

        public boolean shouldExclude() { return this == TICK || this == WATCHDOG; }
        public boolean shouldInclude() { return  !shouldExclude(); }

        private static final String[] HEADER_NAME;
        static {
            Task[] tasks = values();
            HEADER_NAME = new String[tasks.length];
            var sb = new StringBuilder();
            for (int taskIdx = 0; taskIdx < tasks.length; taskIdx++) {
                sb.setLength(0);
                String name = tasks[taskIdx].name();
                for (int i = 0; i < name.length(); i++) {
                    char c = name.charAt(i);
                    sb.append(c == '_' ? name.charAt(++i) : Character.toLowerCase(c));
                }
                HEADER_NAME[taskIdx] = sb.toString();
            }
        }
        public String headerName() { return HEADER_NAME[ordinal()]; }

        public boolean matches(Params params, RecordedEvent event, StringBuilder tmp) {
            return matches(params, event.getThread("sampledThread"),
                           event.getStackTrace().getFrames(), tmp);
        }

        private static final TaskPattern[][] PATTERNS;
        static {
            PATTERNS = new TaskPattern[HEADER_NAME.length][];
            PATTERNS[TICK.ordinal()] = new TaskPattern[] {
                    new TaskPattern("Timestamp\\.tick")
            };
            PATTERNS[WATCHDOG.ordinal()] = new TaskPattern[] {
                    new TaskPattern("QueryBench\\.watchdog")
            };
            PATTERNS[STRING2ID.ordinal()] = new TaskPattern[] {
                    new TaskPattern(SourceKind::isHdt,
                            "org\\.rdfhdt.*\\.stringToId|IdAccess\\.encode"),
                    new TaskPattern(SourceKind::isFsStore,
                            "LocalityCompositeDict\\.Lookup\\.find")
            };
            PATTERNS[ID2STRING.ordinal()] = new TaskPattern[] {
                    new TaskPattern(SourceKind::isHdt, "IdAccess\\.to(NT|Term|String)"),
                    new TaskPattern(SourceKind::isFsStore, "LocalityCompositeDict\\.Lookup.get")
            };
            PATTERNS[TP_REBIND.ordinal()] = new TaskPattern[] {
                    new TaskPattern("TPEmitter\\.rebind")
            };
            PATTERNS[REBIND.ordinal()] = new TaskPattern[] {
                    new TaskPattern("rebind")
            };
            PATTERNS[CANCEL.ordinal()] = new TaskPattern[] {
                    new TaskPattern("\\.(do|try)Cancel$|cancel$")
            };
            PATTERNS[TASK_TAKE.ordinal()] = new TaskPattern[] {
                    new TaskPattern(FlowModel.ITERATE,
                            "ThreadPoolExecutor\\.getTask|ForkJoinPool\\.awaitWork"),
                    new TaskPattern(FlowModel.EMIT, "TaskQueue\\.take"),
            };
            PATTERNS[TASK_PUT.ordinal()] = new TaskPattern[] {
                    new TaskPattern(FlowModel.ITERATE,
                            "(ThreadPoolExecutor|ForkJoinPool)\\.execute|VirtualThreads\\.unpark"),
                    new TaskPattern(FlowModel.EMIT,
                            "TaskQueue\\.put|Task\\.awake(SameWorker|Parallel)"),
            };
            PATTERNS[VTHREAD_SWITCH.ordinal()] = new TaskPattern[] {
                    new TaskPattern("jvmti_vthread|Continuation\\.|VirtualThread\\.(un)?mount")
            };
            PATTERNS[PAGE_FAULT.ordinal()] = new TaskPattern[] {
                    new TaskPattern("exc_page_fault")
            };
            PATTERNS[LOCK.ordinal()] = new TaskPattern[] {
                    new TaskPattern("\\.lock")
            };
            PATTERNS[UNLOCK.ordinal()] = new TaskPattern[] {
                    new TaskPattern("\\.unlock")
            };
            PATTERNS[ATOMICS.ordinal()] = new TaskPattern[] {
                    new TaskPattern("VarHandleGuards")
            };
            PATTERNS[BIT_CONS_PARK.ordinal()] = new TaskPattern[] {
                    new TaskPattern("LockSupport\\.park", "SPSCBIt.nextBatch", false)
            };
            PATTERNS[BIT_PROD_PARK.ordinal()] = new TaskPattern[] {
                    new TaskPattern("LockSupport\\.park", "SPSCBIt.offer", false)
            };
            PATTERNS[BATCH_CONVERSION.ordinal()] = new TaskPattern[] {
                    new TaskPattern("put(Row)?Converting|FromStoreConverter\\.onBatchByCopy|(Store|Hdt)?ConverterStage\\.onBatchByCopy")
            };
            PATTERNS[BATCH_QUICK_APPEND.ordinal()] = new TaskPattern[] {
                    new TaskPattern("Batch\\.quickAppend")
            };
            PATTERNS[BATCH_APPEND.ordinal()] = new TaskPattern[] {
                    new TaskPattern("Batch\\.append")
            };
            PATTERNS[BATCH_COPY.ordinal()] = new TaskPattern[] {
                    new TaskPattern("Batch\\.copy")
            };
            PATTERNS[ALLOC_CREATE.ordinal()] = new TaskPattern[] {
                    new TaskPattern("Alloc\\.create")
            };
            PATTERNS[ALLOC_OFFER.ordinal()] = new TaskPattern[] {
                    new TaskPattern("Alloc\\.offer")
            };
            PATTERNS[BATCH_CREATE.ordinal()] = new TaskPattern[] {
                    new TaskPattern("BatchType\\.(create$|createForThread|poll|empty)")
            };
            PATTERNS[BATCH_RECYCLE.ordinal()] = new TaskPattern[] {
                    new TaskPattern("Batch\\.recycle")
            };
            PATTERNS[ROPE_INTERN.ordinal()] = new TaskPattern[] {
                    new TaskPattern("SharedRopes\\.intern")
            };
            PATTERNS[PUT_TERM.ordinal()] = new TaskPattern[] {
                    new TaskPattern("\\.putTerm")
            };
            PATTERNS[PUT_TERM_COPY.ordinal()] = new TaskPattern[] {
                    new TaskPattern("(array)?copy", "\\.putTerm", true)
            };
            PATTERNS[PUT_TERM_PAGE_FAULT.ordinal()] = new TaskPattern[] {
                    new TaskPattern("page_fault", "\\.putTerm", true)
            };
            PATTERNS[WEAK_DEDUP.ordinal()] = new TaskPattern[] {
                    new TaskPattern("WeakDedup\\.isDuplicate")
            };
            PATTERNS[DEDUP.ordinal()] = new TaskPattern[] {
                    new TaskPattern("Dedup\\.isDuplicate")
            };
            PATTERNS[FILTER_IN_PLACE.ordinal()] = new TaskPattern[] {
                    new TaskPattern("filterInPlace")
            };
            PATTERNS[PROJECT_IN_PLACE.ordinal()] = new TaskPattern[] {
                    new TaskPattern("projectInPlace")
            };
            PATTERNS[GC.ordinal()] = new TaskPattern[0];
            String missing = Arrays.stream(ALL).filter(t -> PATTERNS[t.ordinal()] == null)
                    .map(Objects::toString).collect(joining(", "));
            if (!missing.isEmpty())
                throw new ExceptionInInitializerError("No patterns for "+missing);
        }

        public boolean matches(Params params, RecordedThread thread, List<RecordedFrame> frames,
                               StringBuilder tmp) {
            if (this == GC) {
                return thread != null && thread.getOSName().startsWith("GC Thread");
            }
            for (var taskPattern : PATTERNS[ordinal()]) {
                if (taskPattern.matches(params, frames, tmp))
                    return true;
            }
            return false;
        }
    }

    private record TaskPattern(@Nullable FlowModel flow,
                               @Nullable Predicate<SourceKind> sourceKindMatcher,
                               Pattern pattern, @Nullable Pattern callerPattern,
                               boolean allowIndirectCaller) {
        public TaskPattern(String regexp) {
            this(null, null, Pattern.compile(regexp), null, false);
        }
        public TaskPattern(@Nullable Predicate<SourceKind> sourceKindMatcher,
                           String regexp) {
            this(null, sourceKindMatcher, Pattern.compile(regexp), null, false);
        }
        public TaskPattern(@Nullable FlowModel flow, String regexp) {
            this(flow, null, Pattern.compile(regexp), null, false);
        }
        public TaskPattern(String regexp, String callerRegexp, boolean allowIndirectCaller) {
            this(null, null, Pattern.compile(regexp),
                    Pattern.compile(callerRegexp), allowIndirectCaller);
        }

        boolean matches(Params params, List<RecordedFrame> frames, StringBuilder tmp) {
            if (flow != null && params.flowModel!=flow)
                return false;
            if (sourceKindMatcher != null && !sourceKindMatcher.test(params.sourceKind))
                return false;
            for (int i = 0, framesCount = frames.size(); i < framesCount; i++) {
                if (match(pattern, frames.get(i), tmp)) {
                    if (callerPattern == null)
                        return true;
                    if (allowIndirectCaller) {
                        for (int j = i+1; j < framesCount; j++) {
                            if (match(callerPattern, frames.get(j), tmp))
                                return true;
                        }
                    } else {
                        if (i+1 < framesCount && match(callerPattern, frames.get(i+1), tmp))
                            return true;
                    }
                }
            }
            return false;
        }

        private boolean match(Pattern pattern, RecordedFrame frame, StringBuilder tmp) {
            RecordedMethod method = frame.getMethod();
            tmp.setLength(0);
            RecordedClass type = method.getType();
            if (type != null)
                tmp.append(type.getName()).append('.');
            tmp.append(method.getName());
            return pattern.matcher(tmp).find();
        }
    }

    private static final class SampleCounter {
        private final Params params;
        private final String origin;
        private long samples;
        private long excludedSamples;
        private final long[] task2samples = new long[Task.ALL.length];

        private SampleCounter(Params params, String origin) {
            this.params = params;
            this.origin = origin;
        }

        public void included(Task task) { ++task2samples[task.ordinal()]; }

        public void sample() { ++samples; }

        public void excluded(Task task) {
            ++excludedSamples;
            ++task2samples[task.ordinal()];
        }

        public TasksWeights makeInfo() {
            double[] weights  = new double[Task.ALL.length];
            double nonExcludedSamples = samples-excludedSamples;
            for (Task t : Task.EXCLUDE)
                weights[t.ordinal()] = task2samples[t.ordinal()]/(double)samples;
            for (Task t : Task.INCLUDE)
                weights[t.ordinal()] = task2samples[t.ordinal()]/nonExcludedSamples;
            return new TasksWeights(params, origin, weights);
        }
    }

    private record TasksWeights (Params params, String origin, double[] weights) {
        public double get(Task task) { return weights[task.ordinal()]; }
    }

    private record JfrFinder(Map<Params, List<File>> out) {
        private static final Pattern RX = Pattern.compile("fastersparql\\.QueryBench\\.termLen-AverageTime-batchKind-(.*)-flowModel-(.*)-queries-(.*)-srcKind-(.*)-unionSource-(.*)");
        private static final int BATCH_KIND_GRP   = 1;
        private static final int FLOW_GRP         = 2;
        private static final int QUERIES_GRP      = 3;
        private static final int SRC_KIND_GRP     = 4;
        private static final int UNION_SOURCE_GRP = 5;

        private void visitAll(Collection<File> dirs) throws IOException {
            for (File dir : dirs) visit(dir);
        }

        private void visit(File dir) throws IOException {
            var subDirs = dir.listFiles(File::isDirectory);
            if (subDirs == null)
                throw new IOException("Could list contents of "+dir);
            log.info("Scanning {} dirs in {}...", subDirs.length, dir);
            for (File subDir : subDirs) {
                Matcher matcher = RX.matcher(subDir.getName());
                if (matcher.matches()) {
                    File jfr = new File(subDir, "jfr-cpu.jfr");
                    if (jfr.isFile() && jfr.length() > 0) {
                        try (var rec = new RecordingFile(jfr.toPath())) {
                            rec.readEventTypes();
                            Params params = null;
                            try {
                                params = new Params(
                                        matcher.group(QUERIES_GRP),
                                        SourceKind.valueOf(matcher.group(SRC_KIND_GRP)),
                                        BatchKind.valueOf(matcher.group(BATCH_KIND_GRP)),
                                        FlowModel.valueOf(matcher.group(FLOW_GRP)),
                                        Boolean.parseBoolean(matcher.group(UNION_SOURCE_GRP)));
                            } catch (IllegalArgumentException e) {
                                log.error("JFR file naming does not match current names." +
                                          " file: {}. error: {}", jfr, e.getMessage());
                            }
                            if (params != null) {
                                //noinspection unused
                                var files = out.computeIfAbsent(params, k -> new ArrayList<>());
                                files.add(jfr);
                            }
                        } catch (Exception e) {
                            log.warn("Ignoring broken JFR at {}", jfr);
                        }
                    }
                } else if (subDir.isDirectory()) {
                    visit(subDir);
                }
            }
        }
    }

    record Params(
            String queries,
            SourceKind sourceKind,
            BatchKind batchKind,
            FlowModel flowModel,
            boolean unionSource
    ) {}
}

