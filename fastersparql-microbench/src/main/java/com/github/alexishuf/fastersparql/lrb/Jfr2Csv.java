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
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.String.format;
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
        long findStartNs = nanoTime();
        Map<Params, List<File>> params2jfrFiles = new HashMap<>();
        new JfrFinder(params2jfrFiles).visitAll(roots);
        int jfrFilesCount = params2jfrFiles.values().stream().mapToInt(List::size).sum();
        log.info("Found {} JFR files in {}s",
                 jfrFilesCount, duration2string(findStartNs, nanoTime()));
        int totalWeights = jfrFilesCount + params2jfrFiles.keySet().size();

        List<TasksWeights> weightsList = Collections.synchronizedList(new ArrayList<>());
        long procStartNs = nanoTime();
        var stopProgressReport = new Semaphore(0);
        Thread.startVirtualThread(() -> {
            Thread.currentThread().setName("Jfr2Csv.ProgressReporter");
            try {
                while (!stopProgressReport.tryAcquire(10, TimeUnit.SECONDS)) {
                    int done = weightsList.size();
                    long nsPerFile = (nanoTime()-procStartNs)/done;
                    log.info("Computed {}/{} rows, {}% ETA {}", done, totalWeights,
                            format("%.3f", 100.0/totalWeights*done),
                            duration2string(0, (totalWeights-done)*nsPerFile));
                }
            } catch (InterruptedException e) {
                log.error("Interrupted. Will stop reporting progress");
            }
        });

        try {
            params2jfrFiles.entrySet().parallelStream()
                    .forEach(e -> makeTasksWeights(e.getKey(), e.getValue(), weightsList));
        } finally {
            stopProgressReport.release();
        }
        log.info("Processed {} JFR files in {}",
                 jfrFilesCount, duration2string(procStartNs, nanoTime()));
        log.info("Writing to {}...", destFile);
        writeOutputCsv(weightsList);
        return null;
    }

    private static String duration2string(long startNs, long endNs) {
        var d = Duration.ofNanos(endNs - startNs);
        if (d.toHours() > 0)
            return format("%d:%d:%d", d.toHours(), d.toMinutesPart(), d.toSecondsPart());
        else if (d.toMinutes() > 0)
            return format("%d:%d", d.toMinutesPart(), d.toSecondsPart());
        else
            return format("%d seconds", d.toSeconds());
    }

    private void writeOutputCsv(List<TasksWeights> infos) throws IOException {
        try (var w = new FileWriter(destFile, UTF_8)) {
            w.append("queries,source,batch,flow,unionSource,origin,samples,excludedSamples,includedSamples");
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
                        .append(weights.origin).append(',')
                        .append(Long.toString(weights.samples)).append(',')
                        .append(Long.toString(weights.excludedSamples)).append(',')
                        .append(Long.toString(weights.includedSamples()));
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
        STRING2ID_BIND,
        ID2STRING,
        ID2STRING_BIND,
        TP_SEARCH,
        TP_REBIND,
        REBIND,
        EM_REBIND,
        IT_REBIND,
        PLAN_BIND,
        PARSE_SPARQL,
        FS_PARSE_SPARQL,
        JENA_PARSE_SPARQL,
        JENA_TXN,
        OPTIMIZER,
        FS_OPTIMIZER,
        JENA_OPTIMIZER,
        CANCEL,
        TASK_QUEUES,
        TASK_TAKE,
        TASK_PUT,
        EM_TASK_TAKE,
        EM_TASK_PUT,
        IT_TASK_TAKE,
        IT_TASK_PUT,
        VTHREAD_SWITCH,
        PAGE_FAULT,
        NEW,
        NEW_PAGE_FAULT,
        NEW_OR_PAGE_FAULT,
        NEW_OR_PAGE_FAULT_REBIND,
        MMAP_PAGE_FAULT,
        LOCK,
        LOCKBIND,
        UNLOCK,
        ATOMICS,
        MERGEBIT_OFFER_SYNC,
        GATHERING_ONBATCH_SYNC,
        BIT_CONS_PARK,
        BIT_PROD_PARK,
        BIT_INIT,
        EM_INIT,
        BIT_CLEANUP,
        EM_CLEANUP,
        BATCH_CONVERSION,
        BATCH_HASH,
        BATCH_QUICK_APPEND,
        BATCH_APPEND,
        BATCH_COPY,
        BATCH_CREATE,
        BATCH_RECYCLE,
        ROPE_INTERN,
        ROPE_INTERN_PAGE_FAULT,
        PUT_TERM,
        PUT_TERM_COPY,
        PUT_TERM_PAGE_FAULT,
        GC_PAGE_FAULT,
        JENA_TERM2NODE,
        JENA_PARSE_NODE,
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
                            "LocalityCompositeDict[.$]Lookup\\.find")
            };
            PATTERNS[STRING2ID_BIND.ordinal()] = new TaskPattern[] {
                    new TaskPattern(SourceKind::isHdt,
                            "org\\.rdfhdt.*\\.stringToId|IdAccess\\.encode",
                            "BIt\\.bind$|Plan\\.bound$|\\.rebind$",
                            true),
                    new TaskPattern(SourceKind::isFsStore,
                            "LocalityCompositeDict[.$]Lookup\\.find",
                            "BIt\\.bind$|Plan\\.bound$|\\.rebind$",
                            true)
            };
            PATTERNS[ID2STRING.ordinal()] = new TaskPattern[] {
                    new TaskPattern(SourceKind::isHdt, "IdAccess\\.to(NT|Term|String)"),
                    new TaskPattern(SourceKind::isFsStore, "LocalityCompositeDict[.$]Lookup.get")
            };
            PATTERNS[ID2STRING_BIND.ordinal()] = new TaskPattern[] {
                    new TaskPattern(SourceKind::isHdt,
                            "IdAccess\\.to(NT|Term|String)",
                            "BIt\\.bind$|Plan\\.bound$|\\.rebind$",
                            true),
                    new TaskPattern(SourceKind::isFsStore,
                            "LocalityCompositeDict[.$]Lookup.get",
                            "BIt\\.bind$|Plan\\.bound$|\\.rebind$",
                            true)
            };
            PATTERNS[TP_SEARCH.ordinal()] = new TaskPattern[] {
                    new TaskPattern(SourceKind::isHdt, "BitmapTriples\\.search"),
                    new TaskPattern(SourceKind::isFsStore, "Triples\\.(values|pairs|subKeys|contains)")
            };
            PATTERNS[TP_REBIND.ordinal()] = new TaskPattern[] {
                    new TaskPattern("TPEmitter\\.rebind")
            };
            PATTERNS[EM_REBIND.ordinal()] = new TaskPattern[] {
                    new TaskPattern("\\.rebind$")
            };
            PATTERNS[IT_REBIND.ordinal()] = new TaskPattern[] {
                    new TaskPattern("BindingBIt\\.(re)?bind")
            };
            PATTERNS[PLAN_BIND.ordinal()] = new TaskPattern[] {
                    new TaskPattern("Plan\\.bound")
            };
            PATTERNS[FS_PARSE_SPARQL.ordinal()] = new TaskPattern[] {
                    new TaskPattern("SparqlParser\\.parse$")
            };
            PATTERNS[JENA_PARSE_SPARQL.ordinal()] = new TaskPattern[] {
                    new TaskPattern("QueryFactory\\.create$")
            };
            PATTERNS[JENA_TXN.ordinal()] = new TaskPattern[] {
                    new TaskPattern("DatasetGraphTxnCtl")
            };
            PATTERNS[FS_OPTIMIZER.ordinal()] = new TaskPattern[] {
                    new TaskPattern("Optimizer\\.(optimize|shallowOptimize)")
            };
            PATTERNS[JENA_OPTIMIZER.ordinal()] = new TaskPattern[] {
                    new TaskPattern("Algebra\\.optimize", "QueryEngineMain\\.modifyOp", false)
            };
            PATTERNS[CANCEL.ordinal()] = new TaskPattern[] {
                    new TaskPattern("\\.(do|try)Cancel$|cancel$")
            };
            PATTERNS[IT_TASK_TAKE.ordinal()] = new TaskPattern[] {
                    new TaskPattern("ThreadPoolExecutor\\.getTask|ForkJoinPool\\.awaitWork"),
            };
            PATTERNS[EM_TASK_TAKE.ordinal()] = new TaskPattern[] {
                    new TaskPattern("TaskQueue\\.take"),
            };
            PATTERNS[IT_TASK_PUT.ordinal()] = new TaskPattern[] {
                    new TaskPattern("(ThreadPoolExecutor|ForkJoinPool)\\.execute|VirtualThreads\\.unpark"),
            };
            PATTERNS[EM_TASK_PUT.ordinal()] = new TaskPattern[] {
                    new TaskPattern("TaskQueue\\.put|Task\\.awake(SameWorker|Parallel)"),
            };
            PATTERNS[VTHREAD_SWITCH.ordinal()] = new TaskPattern[] {
                    new TaskPattern("jvmti_vthread|Continuation\\.(on|unpin|pin|mount|unmount)|VirtualThread\\.(un)?mount")
            };
            PATTERNS[PAGE_FAULT.ordinal()] = new TaskPattern[] {
                    new TaskPattern("exc_page_fault")
            };
            PATTERNS[NEW_PAGE_FAULT.ordinal()] = new TaskPattern[] {
                    new TaskPattern("exc_page_fault",
                            "arraycopy|\\.(create|make|copy|fill|dup|<init>|wrap)|WorkerThread::run",
                            true),
                    new TaskPattern("do_anonymous_page"),
            };
            PATTERNS[NEW.ordinal()] = new TaskPattern[] {
                    new TaskPattern("new_(instance|array(_nonzero)?)_C|allocate_(instance|common)")
            };
            PATTERNS[LOCK.ordinal()] = new TaskPattern[] {
                    new TaskPattern("\\.lock")
            };
            PATTERNS[UNLOCK.ordinal()] = new TaskPattern[] {
                    new TaskPattern("\\.unlock")
            };
            PATTERNS[ATOMICS.ordinal()] = new TaskPattern[] {
                    new TaskPattern("VarHandleGuards|\\.compareAndSet")
            };
            PATTERNS[MERGEBIT_OFFER_SYNC.ordinal()] = new TaskPattern[] {
                    new TaskPattern("\\.(awaitUninterruptibly|lock|unlock|park|unpark)$",
                                    "MergeBIt\\.offer", true)
            };
            PATTERNS[GATHERING_ONBATCH_SYNC.ordinal()] = new TaskPattern[] {
                    new TaskPattern("\\.lock$|\\.unlock|VarHandleGuards",
                            "GatheringEmitter[.$]Connector\\.onBatch",
                            true)
            };
            PATTERNS[BIT_CONS_PARK.ordinal()] = new TaskPattern[] {
                    new TaskPattern("LockSupport\\.park", "SPSCBIt.nextBatch", false)
            };
            PATTERNS[BIT_PROD_PARK.ordinal()] = new TaskPattern[] {
                    new TaskPattern("LockSupport\\.park", "SPSCBIt.offer", false)
            };
            PATTERNS[BIT_INIT.ordinal()] = new TaskPattern[] {
                    new TaskPattern("BIt.<init>")
            };
            PATTERNS[EM_INIT.ordinal()] = new TaskPattern[] {
                    new TaskPattern("(Emitter|BindingStage.*|ScatterStage.*).<init>")
            };
            PATTERNS[BIT_CLEANUP.ordinal()] = new TaskPattern[] {
                    new TaskPattern("BIt.cleanup$")
            };
            PATTERNS[EM_CLEANUP.ordinal()] = new TaskPattern[] {
                    new TaskPattern("(Emitter|Stage|Processor|Filter|Merger|Task).doRelease$")
            };
            PATTERNS[BATCH_CONVERSION.ordinal()] = new TaskPattern[] {
                    new TaskPattern("put(Row)?Converting|FromStoreConverter\\.onBatchByCopy|(Store|Hdt)?ConverterStage\\.onBatchByCopy")
            };
            PATTERNS[BATCH_HASH.ordinal()] = new TaskPattern[] {
                    new TaskPattern("Batch\\.hash")
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
            PATTERNS[ROPE_INTERN_PAGE_FAULT.ordinal()] = new TaskPattern[] {
                    new TaskPattern("exc_page_fault", "SharedRopes\\.intern", true)
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
            PATTERNS[JENA_TERM2NODE.ordinal()] = new TaskPattern[] {
                    new TaskPattern("JenaTermParser\\.parse")
            };
            PATTERNS[JENA_PARSE_NODE.ordinal()] = new TaskPattern[] {
                    new TaskPattern("JenaNodeParser\\.makeNode")
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
            PATTERNS[GC.ordinal()]                       = new TaskPattern[0];
            PATTERNS[GC_PAGE_FAULT.ordinal()]            = new TaskPattern[0];
            PATTERNS[NEW_OR_PAGE_FAULT.ordinal()]        = new TaskPattern[0];
            PATTERNS[NEW_OR_PAGE_FAULT_REBIND.ordinal()] = new TaskPattern[0];
            PATTERNS[MMAP_PAGE_FAULT.ordinal()]          = new TaskPattern[0];
            PATTERNS[PARSE_SPARQL.ordinal()]             = new TaskPattern[0];
            PATTERNS[REBIND.ordinal()]                   = new TaskPattern[0];
            PATTERNS[LOCKBIND.ordinal()]                 = new TaskPattern[0];
            PATTERNS[OPTIMIZER.ordinal()]                = new TaskPattern[0];
            PATTERNS[TASK_QUEUES.ordinal()]              = new TaskPattern[0];
            PATTERNS[TASK_TAKE.ordinal()]                = new TaskPattern[0];
            PATTERNS[TASK_PUT.ordinal()]                 = new TaskPattern[0];
            String missing = Arrays.stream(ALL).filter(t -> PATTERNS[t.ordinal()] == null)
                    .map(Objects::toString).collect(joining(", "));
            if (!missing.isEmpty())
                throw new ExceptionInInitializerError("No patterns for "+missing);
        }

        public boolean matches(Params params, RecordedThread thread, List<RecordedFrame> frames,
                               StringBuilder tmp) {
            return switch (this) {
                case GC -> thread != null && thread.getOSName().startsWith("GC Thread");
                case GC_PAGE_FAULT -> thread != null && thread.getOSName().startsWith("GC Thread")
                        && PAGE_FAULT.matches(params, thread, frames, tmp);
                case NEW_OR_PAGE_FAULT -> NEW.matches(params, thread, frames, tmp)
                        || NEW_PAGE_FAULT.matches(params, thread, frames, tmp);
                case NEW_OR_PAGE_FAULT_REBIND ->
                        NEW_OR_PAGE_FAULT.matches(params, thread, frames, tmp)
                                && REBIND.matches(params, thread, frames, tmp);
                case MMAP_PAGE_FAULT ->
                    PAGE_FAULT.matches(params, thread, frames, tmp)
                            && !NEW_PAGE_FAULT.matches(params, thread, frames, tmp);
                case OPTIMIZER -> JENA_OPTIMIZER.matches(params, thread, frames, tmp)
                        || FS_OPTIMIZER.matches(params, thread, frames, tmp);
                case PARSE_SPARQL -> FS_PARSE_SPARQL.matches(params, thread, frames, tmp)
                        || JENA_PARSE_SPARQL.matches(params, thread, frames, tmp);
                case REBIND -> EM_REBIND.matches(params, thread, frames, tmp)
                        || IT_REBIND.matches(params, thread, frames, tmp);
                case LOCKBIND -> LOCK.matches(params, thread, frames, tmp)
                        && REBIND.matches(params, thread, frames, tmp);
                case TASK_TAKE -> EM_TASK_TAKE.matches(params, thread, frames, tmp)
                        || IT_TASK_TAKE.matches(params, thread, frames, tmp);
                case TASK_PUT -> EM_TASK_PUT.matches(params, thread, frames, tmp)
                        || IT_TASK_PUT.matches(params, thread, frames, tmp);
                case TASK_QUEUES -> TASK_TAKE.matches(params, thread, frames, tmp)
                        || TASK_PUT.matches(params, thread, frames, tmp);
                default -> {
                    for (var taskPattern : PATTERNS[ordinal()]) {
                        if (taskPattern.matches(params, frames, tmp))
                            yield true;
                    }
                    yield false;
                }
            };
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
        public TaskPattern(String regexp, String callerRegexp, boolean allowIndirectCaller) {
            this(null, null, Pattern.compile(regexp),
                    Pattern.compile(callerRegexp), allowIndirectCaller);
        }
        public TaskPattern(Predicate<SourceKind> sourceKindMatcher,
                           String regexp, String callerRegexp, boolean allowIndirectCaller) {
            this(null, sourceKindMatcher, Pattern.compile(regexp),
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
            return new TasksWeights(params, origin, samples, excludedSamples, weights);
        }
    }

    private record TasksWeights(Params params, String origin, long samples, long excludedSamples,
                                double[] weights) {
        public double get(Task task) { return weights[task.ordinal()]; }
        public long includedSamples() { return samples-excludedSamples; }
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

