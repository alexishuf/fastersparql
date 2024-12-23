package com.github.alexishuf.fastersparql.lrb;

import com.github.alexishuf.fastersparql.FlowModel;
import com.github.alexishuf.fastersparql.lrb.cmd.MeasureOptions.BatchKind;
import com.github.alexishuf.fastersparql.lrb.sources.SourceKind;
import com.github.alexishuf.fastersparql.util.BS;
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
        var jfrFiles = new JfrFiles();
        roots.parallelStream().map(r -> new JfrFinder().visit(r)).forEachOrdered(jfrFiles::add);
        int jfrFilesCount = jfrFiles.filesCount();
        log.info("Found {} JFR files in {}s",
                 jfrFilesCount, duration2string(findStartNs, nanoTime()));
        int totalWeights = jfrFilesCount + jfrFiles.paramsCount();

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
            jfrFiles.param2files.entrySet().parallelStream()
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
        Matcher matcher = new Matcher(params);
        for (File file : jfrFiles) {
            log.info("Processing {}", file  );
            try (var rec = new RecordingFile(file.toPath())) {
                var fileCounter = new SampleCounter(params, file.toString());
                while (rec.hasMoreEvents()) {
                    var event = rec.readEvent();
                    if (event.getEventType().getName().equals("jdk.ExecutionSample")) {
                        matcher.reset(event);
                        allCounter.sample();
                        fileCounter.sample();
                        for (var task : Task.EXCLUDE) {
                            if (matcher.matches(task)) {
                                allCounter.excluded(task);
                                fileCounter.excluded(task);
                            }
                        }
                        for (var task : Task.INCLUDE) {
                            if (matcher.matches(task)) {
                                allCounter.included(task);
                                fileCounter.included(task);
                            }
                        }
                    }
                }
                syncWeigtsList.add(fileCounter.makeInfo());
            } catch (IOException e) {
                log.warn("Failed to read JFR data from {}: {}", file, e.toString());
            }
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
        BOUND_QUERY_GEN,
        PARSE_SPARQL,
        SERIALIZE_RESULTS,
        PARSE_RESULTS,
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
        TASK_WORKER,
        FEDX_WORKER,
        FEDX_JOIN_TASK,
        FEDX_UNION_TASK,
        DRAIN,
        NIO_WAKEUP,
        NIO_EVENT_LOOP,
        NIO_WAKEUP_IN_WORKER,
        SOCKET_READ,
        SOCKET_WRITE,
        FEDX_SOCKET_READ,
        FEDX_SOCKET_WRITE,
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
        TERM_WRAP,
        TERM_HASH,
        BATCH_HASH,
        BATCH_CONVERSION,
        BATCH_QUICK_APPEND,
        BATCH_APPEND,
        BATCH_COPY,
        BATCH_CREATE,
        BATCH_RECYCLE,
        TERMINFO_SET,
        ROPE_COMPARE,
        ROPE_COMPARE_PAGE_FAULT,
        ROPE_INTERN,
        ROPE_INTERN_PAGE_FAULT,
        PUT_TERM,
        PUT_TERM_COPY,
        PUT_TERM_PAGE_FAULT,
        GC_PAGE_FAULT,
        JENA_NODE2TERM,
        JENA_MAKE_NODE,
        ALLOC_CREATE,
        ALLOC_OFFER,
        WEAK_DEDUP,
        DEDUP,
        BTREE_DEDUP_COMPARE,
        BTREE_SORT_COMPARE,
        BTREE_SORT,
        BTREE_DESTRUCTIVE_FOREACH,
        BTREE_DESTRUCTIVE_FOREACH_CONSUMER,
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
            PATTERNS[BOUND_QUERY_GEN.ordinal()] = new TaskPattern[] {
                    new TaskPattern("(Plan|SparqlQuery)\\.bound"),
                    new TaskPattern("QueryStringUtil\\.selectQueryString")
            };
            PATTERNS[SERIALIZE_RESULTS.ordinal()] = new TaskPattern[] {
                    new TaskPattern("Handler\\.serialize")
            };
            PATTERNS[PARSE_RESULTS.ordinal()] = new TaskPattern[] {
                    new TaskPattern("\\.feedShared|\\.parseQueryResult")
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
            PATTERNS[DRAIN.ordinal()] = new TaskPattern[] {
                    new TaskPattern("QueryRunner.drain")
            };
            PATTERNS[TASK_WORKER.ordinal()] = new TaskPattern[] {
                    new TaskPattern("VirtualThread\\.run|EmitterService[$.]Worker\\.run|ControlledWorker|BackgroundResultExecutor")
            };
            PATTERNS[FEDX_WORKER.ordinal()] = new TaskPattern[] {
                    new TaskPattern("ControlledWorker|BackgroundResultExecutor")
            };
            PATTERNS[FEDX_JOIN_TASK.ordinal()] = new TaskPattern[] {
                    new TaskPattern("Parallel(Bound|Left|Check|Service)?JoinTask\\.performTaskInternal")
            };
            PATTERNS[FEDX_UNION_TASK.ordinal()] = new TaskPattern[] {
                    new TaskPattern("Parallel(Prepared(Algebra)?)?Union(Operator)?Task\\.performTaskInternal")
            };
            PATTERNS[NIO_WAKEUP.ordinal()] = new TaskPattern[] {
                    new TaskPattern("NioEventLoop\\.wakeup")
            };
            PATTERNS[NIO_EVENT_LOOP.ordinal()] = new TaskPattern[] {
                    new TaskPattern("NioEventLoop\\.run")
            };
            PATTERNS[NIO_WAKEUP_IN_WORKER.ordinal()] = new TaskPattern[]{
                    new TaskPattern("NioEventLoop\\.wakeup",
                            "VirtualThread\\.run|EmitterService[$.]Worker\\.run",
                            true)
            };
            PATTERNS[SOCKET_READ.ordinal()] = new TaskPattern[] {
                    new TaskPattern("Socket[^.]*\\..*[rR]ead")
            };
            PATTERNS[SOCKET_WRITE.ordinal()] = new TaskPattern[] {
                    new TaskPattern("Socket[^.]*\\..*[wW]rite")
            };
            PATTERNS[FEDX_SOCKET_READ.ordinal()] = new TaskPattern[] {
                    new TaskPattern("Socket[^.]*\\..*[rR]ead", "ControlledWorker|BackgroundResultExecutor", true)
            };
            PATTERNS[FEDX_SOCKET_WRITE.ordinal()] = new TaskPattern[] {
                    new TaskPattern("Socket[^.]*\\..*[wW]rite", "ControlledWorker|BackgroundResultExecutor", true)
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
            PATTERNS[TERM_WRAP.ordinal()] = new TaskPattern[] {
                    new TaskPattern("TermView\\.wrap")
            };
            PATTERNS[TERM_HASH.ordinal()] = new TaskPattern[] {
                    new TaskPattern("Term\\.hash")
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
            PATTERNS[TERMINFO_SET.ordinal()] = new TaskPattern[] {
                    new TaskPattern("TermInfo\\.set")
            };
            PATTERNS[ROPE_COMPARE.ordinal()] = new TaskPattern[] {
                    new TaskPattern("Rope\\.compare")
            };
            PATTERNS[ROPE_COMPARE_PAGE_FAULT.ordinal()] = new TaskPattern[] {
                    new TaskPattern("exc_page_fault", "Rope\\.compare", true)
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
            PATTERNS[JENA_NODE2TERM.ordinal()] = new TaskPattern[] {
                    new TaskPattern("JenaTermParser\\.parse")
            };
            PATTERNS[JENA_MAKE_NODE.ordinal()] = new TaskPattern[] {
                    new TaskPattern("JenaNodeParser\\.makeNode")
            };
            PATTERNS[WEAK_DEDUP.ordinal()] = new TaskPattern[] {
                    new TaskPattern("WeakDedup\\.isDuplicate")
            };
            PATTERNS[DEDUP.ordinal()] = new TaskPattern[] {
                    new TaskPattern("Dedup\\.isDuplicate")
            };
            PATTERNS[BTREE_DEDUP_COMPARE.ordinal()] = new TaskPattern[] {
                    new TaskPattern("Term\\.compareTo",
                                    "BTreeDedup\\.isDuplicate", true)
            };
            PATTERNS[BTREE_SORT_COMPARE.ordinal()] = new TaskPattern[] {
                    new TaskPattern("Term\\.compareTo",
                                    "BTreeDedup\\.sort", true)
            };
            PATTERNS[BTREE_SORT.ordinal()] = new TaskPattern[] {
                    new TaskPattern("BTreeDedup\\.sort")
            };
            PATTERNS[BTREE_DESTRUCTIVE_FOREACH.ordinal()] = new TaskPattern[] {
                    new TaskPattern("BTreeDedup\\.destructiveForEach")
            };
            PATTERNS[BTREE_DESTRUCTIVE_FOREACH_CONSUMER.ordinal()] = new TaskPattern[] {
                    new TaskPattern("(BatchQueue|SPSCBIt).(copy|offer)",
                                    "BTreeDedup\\.destructiveForEach",
                                    true), // ITERATE
                    new TaskPattern("BatchSorter\\.accept",
                                    "BTreeDedup\\.destructiveForEach",
                                    true) // EMIT
            };
            PATTERNS[FILTER_IN_PLACE.ordinal()] = new TaskPattern[] {
                    new TaskPattern("filterInPlace")
            };
            PATTERNS[PROJECT_IN_PLACE.ordinal()] = new TaskPattern[] {
                    new TaskPattern("projectInPlace")
            };
            var noPatternTasks = new Task[]{
                    GC,
                    GC_PAGE_FAULT,
                    NEW_OR_PAGE_FAULT,
                    NEW_OR_PAGE_FAULT_REBIND,
                    MMAP_PAGE_FAULT,
                    PARSE_SPARQL,
                    REBIND,
                    LOCKBIND,
                    OPTIMIZER,
                    TASK_QUEUES,
                    TASK_TAKE,
                    TASK_PUT
            };
            for (Task task : noPatternTasks) {
                if (PATTERNS[task.ordinal()] != null)
                    throw new ExceptionInInitializerError("PATTERNS["+task+"] already defined");
                PATTERNS[task.ordinal()] = new TaskPattern[0];
            }
            // check PATTERNS was intialized for all tasks
            String missing = Arrays.stream(ALL).filter(t -> PATTERNS[t.ordinal()] == null)
                    .map(Objects::toString).collect(joining(", "));
            if (!missing.isEmpty())
                throw new ExceptionInInitializerError("No patterns for "+missing);
        }
    }

    private static final class Matcher {
        private static final int TASKS_COUNT = Task.values().length;
        private static final int NEGATIVE_BEGIN = TASKS_COUNT;
        private final Params params;
        private RecordedEvent event;
        private final StringBuilder sb = new StringBuilder();
        private final long[] bitset = new long[BS.longsFor(TASKS_COUNT*2)];

        public Matcher(Params params) {this.params = params;}

        public void reset(RecordedEvent e) {
            Arrays.fill(this.bitset, 0L);
            this.sb.setLength(0);
            this.event = e;
        }

        public boolean matches(Task task) {
            int ordinal = task.ordinal();
            if (BS.get(bitset, ordinal))
                return true; // cached positive match
            if (BS.get(bitset, NEGATIVE_BEGIN+ordinal))
                return false; // cached negative match
            var frames = event.getStackTrace().getFrames();
            return matches0(params, task, event.getThread("sampledThread"), frames);
        }

        private boolean matches0(Params params, Task task, RecordedThread thread, List<RecordedFrame> frames) {
            boolean match = switch (task) {
                case GC -> thread != null && thread.getOSName().startsWith("GC Thread");
                case GC_PAGE_FAULT -> thread != null && thread.getOSName().startsWith("GC Thread")
                        && matches0(params, Task.PAGE_FAULT, thread, frames);
                case NEW_OR_PAGE_FAULT -> matches0(params, Task.NEW, thread, frames)
                        || matches0(params, Task.NEW_PAGE_FAULT, thread, frames);
                case NEW_OR_PAGE_FAULT_REBIND ->
                        matches0(params, Task.NEW_OR_PAGE_FAULT, thread, frames)
                                && matches0(params, Task.REBIND, thread, frames);
                case MMAP_PAGE_FAULT ->
                        matches0(params, Task.PAGE_FAULT, thread, frames)
                                && !matches0(params, Task.NEW_PAGE_FAULT, thread, frames);
                case OPTIMIZER -> matches0(params, Task.JENA_OPTIMIZER, thread, frames)
                        || matches0(params, Task.FS_OPTIMIZER, thread, frames);
                case PARSE_SPARQL -> matches0(params, Task.FS_PARSE_SPARQL, thread, frames)
                        || matches0(params, Task.JENA_PARSE_SPARQL, thread, frames);
                case REBIND -> matches0(params, Task.EM_REBIND, thread, frames)
                        || matches0(params, Task.IT_REBIND, thread, frames);
                case LOCKBIND -> matches0(params, Task.LOCK, thread, frames)
                        && matches0(params, Task.REBIND, thread, frames);
                case TASK_TAKE -> matches0(params, Task.EM_TASK_TAKE, thread, frames)
                        || matches0(params, Task.IT_TASK_TAKE, thread, frames);
                case TASK_PUT -> matches0(params, Task.EM_TASK_PUT, thread, frames)
                        || matches0(params, Task.IT_TASK_PUT, thread, frames);
                case TASK_QUEUES -> matches0(params, Task.TASK_TAKE, thread, frames)
                        || matches0(params, Task.TASK_PUT, thread, frames);
                default -> {
                    for (var taskPattern : Task.PATTERNS[task.ordinal()]) {
                        if (taskPattern.matches(params, frames, sb))
                            yield true;
                    }
                    yield false;
                }
            };
            BS.set(bitset, (match ? 0 : NEGATIVE_BEGIN) + task.ordinal());
            return match;
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

    record JfrFile(File file, long modifiedMs) {
        public JfrFile(File file) { this(file, file.lastModified()); }
        public static boolean isNewerThan(File candidate, @Nullable JfrFile older) {
            return older == null || candidate.lastModified() > older.modifiedMs;
        }
    }

    static final class JfrFiles {
        private final Map<Params, List<File>> param2files = new HashMap<>();

        public void add(Map<Params, JfrFile> param2file) {
            for (var e : param2file.entrySet()) {
                //noinspection unused
                var list = param2files.computeIfAbsent(e.getKey(), k -> new ArrayList<>());
                list.add(e.getValue().file);
            }
        }

        public int paramsCount() {return param2files.size();}
        public int  filesCount() {return param2files.values().stream().mapToInt(List::size).sum();}
    }

    private record JfrFinder() {
        private static final Pattern RX = Pattern.compile("fastersparql\\.QueryBench\\.termLen-AverageTime-batchKind-(.*)-flowModel-(.*)-queries-(.*)-srcKind-(.*)-unionSource-(.*)");
        private static final int BATCH_KIND_GRP   = 1;
        private static final int FLOW_GRP         = 2;
        private static final int QUERIES_GRP      = 3;
        private static final int SRC_KIND_GRP     = 4;
        private static final int UNION_SOURCE_GRP = 5;

        public Map<Params, JfrFile> visit(File dir) {
            Map<Params, JfrFile> param2jfr = new HashMap<>();
            visit0(dir, param2jfr);
            return param2jfr;
        }

        private void visit0(File dir, Map<Params, JfrFile> param2jfr) {
            var subDirs = dir.listFiles(File::isDirectory);
            if (subDirs == null) {
                log.error("Could not list contents of {}", dir);
                return;
            }
            log.info("Scanning {} dirs in {}...", subDirs.length, dir);
            for (File subDir : subDirs) {
                java.util.regex.Matcher matcher = RX.matcher(subDir.getName());
                if (matcher.matches()) {
                    File jfr = new File(subDir, "jfr-cpu.jfr");
                    if (jfr.isFile() && jfr.length() > 0) {
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
                            JfrFile old = param2jfr.getOrDefault(params, null);
                            if (JfrFile.isNewerThan(jfr, old))
                                param2jfr.put(params, new JfrFile(jfr));
                        }
                    }
                } else if (subDir.isDirectory()) {
                    visit0(subDir, param2jfr);
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

