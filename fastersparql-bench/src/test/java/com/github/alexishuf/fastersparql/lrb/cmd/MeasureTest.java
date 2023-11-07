package com.github.alexishuf.fastersparql.lrb.cmd;

import com.github.alexishuf.fastersparql.FSProperties;
import com.github.alexishuf.fastersparql.batch.dedup.StrongDedup;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.CompressedBatch;
import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import com.github.alexishuf.fastersparql.client.netty.util.NettyChannelDebugger;
import com.github.alexishuf.fastersparql.client.util.TestTaskSet;
import com.github.alexishuf.fastersparql.emit.Emitters;
import com.github.alexishuf.fastersparql.lrb.query.QueryName;
import com.github.alexishuf.fastersparql.lrb.sources.LrbSource;
import com.github.alexishuf.fastersparql.lrb.sources.SelectorKind;
import com.github.alexishuf.fastersparql.lrb.sources.SourceKind;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.ByteRope;
import com.github.alexishuf.fastersparql.sparql.OpaqueSparqlQuery;
import com.github.alexishuf.fastersparql.sparql.results.serializer.ResultsSerializer;
import com.github.alexishuf.fastersparql.store.StoreSparqlClient;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.*;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.batch.type.CompressedBatchType.COMPRESSED;
import static com.github.alexishuf.fastersparql.lrb.cmd.MeasureOptions.FlowModel.EMIT;
import static com.github.alexishuf.fastersparql.lrb.cmd.MeasureOptions.FlowModel.ITERATE;
import static com.github.alexishuf.fastersparql.lrb.cmd.MeasureOptions.ResultsConsumer.CHECK;
import static com.github.alexishuf.fastersparql.lrb.cmd.MeasureOptions.ResultsConsumer.COUNT;
import static com.github.alexishuf.fastersparql.lrb.sources.SourceKind.*;
import static com.github.alexishuf.fastersparql.model.SparqlResultFormat.TSV;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class MeasureTest {
    private static final Logger log = LoggerFactory.getLogger(MeasureTest.class);
    public static final String DATA_DIR_PROP = "fastersparql.bench.test.data-dir";
    private static File dataDir;
    private static boolean hasHDT, hasStore;
    private static boolean hasAllHDT, hasAllStore;

    @BeforeAll
    static void beforeAll() {
        Batch.makeValidationCheaper();
        String prop = System.getProperty(DATA_DIR_PROP);
        dataDir = prop != null && !prop.isEmpty() ? new File(prop) : new File("");
        hasHDT = Stream.of(LrbSource.DBPedia_Subset, LrbSource.NYT)
                .allMatch(s -> new File(dataDir, s.filename(HDT_FILE)).isFile());
        hasStore = Stream.of(LrbSource.DBPedia_Subset, LrbSource.NYT)
                .allMatch(s -> new File(dataDir, s.filename(FS_STORE)).isDirectory());
        hasAllHDT = LrbSource.all().stream()
                .allMatch(s -> new File(dataDir, s.filename(HDT_FILE)).isFile());
        hasAllStore = LrbSource.all().stream()
                .allMatch(s -> new File(dataDir, s.filename(FS_STORE)).isDirectory());
        log.info("Loading expected results...");
        if (hasAllStore || hasAllHDT)
            Arrays.stream(QueryName.values()).parallel().forEach(n -> n.expected(COMPRESSED));
    }

    @AfterAll static void afterAll() {
        Batch.restoreValidationCheaper();
        System.setProperty(FSProperties.OP_CROSS_DEDUP,
                           String.valueOf(FSProperties.DEF_OP_CROSS_DEDUP));
        FSProperties.refresh();
    }

    private final List<File> tempDirs = new ArrayList<>();

    @BeforeEach void setUp() {
        NettyChannelDebugger.flushActive();
        System.setProperty(FSProperties.OP_CROSS_DEDUP, "false");
        FSProperties.refresh();
    }

    @AfterEach void tearDown() {
        NettyChannelDebugger.flushActive();
        System.setProperty(FSProperties.OP_CROSS_DEDUP,
                           String.valueOf(FSProperties.DEF_OP_CROSS_DEDUP));
        FSProperties.refresh();
        for (File d : tempDirs) {
            if (d.isDirectory()) {
                File[] files = d.listFiles();
                if (files != null) {
                    for (File f : files)
                        if (!f.delete()) log.error("Could not delete {}", f);
                }
                if (!d.delete()) log.error("Could not delete dir {}", d);
            } else if (d.isFile() && !d.delete()) {
                log.error("Could not delete {}", d);
            }
        }
    }

    private File tempDir() throws IOException {
        File dir = Files.createTempDirectory("fastersparql-measure").toFile();
        dir.deleteOnExit();
        tempDirs.add(dir);
        return dir;
    }

    private void doTest(SourceKind sourceKind, boolean jsonPlans, String queries,
                        SelectorKind selectorKind,
                        MeasureOptions.ResultsConsumer consumer,
                        MeasureOptions.FlowModel flowModel) throws IOException {
        int nReps = 2;
        boolean isS2 = queries.equals("S2");
        if (sourceKind.isHdt() && (!hasHDT || (!isS2 && !hasAllHDT))) {
            log.warn("Skipping test: no HDT files in {}. Set Java property {} to change directory",
                     dataDir.getAbsolutePath(), DATA_DIR_PROP);
            return;
        }
        if (sourceKind.isFsStore() && (!hasStore || (!isS2 && !hasAllStore))) {
            log.warn("Skipping test: no Store dirs in {}. Set Java property {} to change directory",
                    dataDir.getAbsolutePath(), DATA_DIR_PROP);
            return;
        }
        int nQueries = new QueryOptions(List.of(queries)).queries().size();
        if (nQueries == 0)
            fail("No queries match "+queries);
        File destDir = tempDir();
        List<String> args = new ArrayList<>(List.of("measure",
                "--queries", queries,
                "--source", sourceKind.name(),
                "--selector", selectorKind.name(),
                "--data-dir", dataDir.getPath(),
                "--dest-dir", destDir.getPath(),
                "--warm-secs", "0",
                "--warm-cool-ms", "500",
                "--cool-ms", "1", // minimal amount, just to touch the code
                "--reps", Integer.toString(nReps),
                "--seed", "728305461",
                "--consumer", consumer.name(),
                "--flow", flowModel.name()
        ));
        if (isS2)
            args.addAll(List.of("--lrb-source", "nyt", "--lrb-source",  "dbpedia-subset"));
        if (jsonPlans)
            args.add("--builtin-plans-json");
        App.run(args.toArray(String[]::new));
        var measurements = MeasurementCsv.load(new File(destDir, "measurements.csv"));
        assertEquals(nReps*nQueries, measurements.size());
        Map<QueryName, Integer> expectedRows = new HashMap<>();
        for (Measurement m : measurements) {
            int rows = m.rows();
            String ctx = m.task().query() + ", rep="+m.rep()+", rows="+ rows;
            assertTrue(m.error() == null || m.error().isEmpty(),
                    ctx+", error="+m.error());
            assertTrue(m.firstRowNs() >= 0, "firstRowNs="+m.firstRowNs()+", "+ctx);
            assertTrue(m.allRowsNs() >= 0, "allRowsNs="+m.allRowsNs()+", "+ctx);
            assertTrue(rows >= 0, "negative row count for "+m.task()+ctx);
            int exRows = expectedRows.getOrDefault(m.task().query(), -1);
            if (exRows == -1)
                expectedRows.put(m.task().query(), rows);
            else
                assertEquals(exRows, rows, "unstable row count "+ctx);
            assertTrue(m.terminalNs() >= 0, "terminalNs="+m.terminalNs()+ctx);
            assertFalse(m.cancelled(), ctx);
            log.debug("{}, rep {} rows={}, allRows={}us", m.task().query(), m.rep(),
                      m.rows(),
                      String.format("%.3f", m.allRowsNs()/1_000.0));
        }
    }

    public static void main(String[] args) {
        beforeAll();
        String uri = "file://" + dataDir + "/DrugBank";
        try (var client = new StoreSparqlClient(SparqlEndpoint.parse(uri))) {
//            var emitter = client.emit(COMPRESSED, new OpaqueSparqlQuery("""
//                SELECT ?predicate ?object WHERE {
//                  ?drug <http://purl.org/dc/elements/1.1/title> "Adenine (JAN/USP)"
//                }"""), Vars.EMPTY);
            var query = new OpaqueSparqlQuery("""
                    SELECT ?s ?p ?o WHERE {
                      <http://bio2rdf.org/dr:D04682> <http://purl.org/dc/elements/1.1/title> ?o .
                    }""");
            var acc = Emitters.collect(client.emit(COMPRESSED, query, Vars.EMPTY));
            var serializer = ResultsSerializer.create(TSV);
            var tsv = new ByteRope();
            serializer.init(query.publicVars(), query.publicVars(), false, tsv);
            serializer.serializeAll(acc, tsv);
            serializer.serializeTrailer(tsv);
            System.out.println(tsv);
        }
    }

    static Stream<Arguments> test() {
        List<SourceKind> sources = List.of(FS_STORE, HDT_FILE, HDT_JSON, HDT_WS, FS_TSV, FS_WS);
        return Stream.of(true, false).flatMap(jsonPlans
                -> sources.stream().map(src -> arguments(jsonPlans, src)));
    }

    @ParameterizedTest @MethodSource("test")
    void testS2(boolean jsonPlans, SourceKind sourceKind) throws Exception {
        doTest(sourceKind, jsonPlans, "S2", SelectorKind.DICT, COUNT, ITERATE);
        doTest(sourceKind, jsonPlans, "S2", SelectorKind.DICT, COUNT, EMIT);
    }

    @ParameterizedTest @MethodSource("test")
    void testSQueries(boolean jsonPlans, SourceKind sourceKind) throws Exception {
        doTest(sourceKind, jsonPlans, "S.*", SelectorKind.ASK, CHECK, ITERATE);
        doTest(sourceKind, jsonPlans, "S.*", SelectorKind.ASK, CHECK, EMIT);
    }

    @ParameterizedTest @MethodSource("test")
    void testCQueries(boolean jsonPlans, SourceKind sourceKind) throws Exception {
        SelectorKind sel = sourceKind == FS_STORE ? SelectorKind.FS_STORE : SelectorKind.ASK;
        doTest(sourceKind, jsonPlans, "C.*", sel, CHECK, ITERATE);
        doTest(sourceKind, jsonPlans, "C.*", sel, CHECK, EMIT);
//        System.out.printf("""
//                BindingStage.repeatRebind: %5d
//                   TPEmitter.repeatRebind: %5d
//                """,
//                (int)BindingStage.REPEAT_REBIND.getOpaque(),
//                (int)StoreSparqlClient.REPEAT_REBIND.getOpaque());
    }

    @ParameterizedTest @MethodSource("test")
    void testBQueries(boolean jsonPlans, SourceKind sourceKind) throws Exception {
        SelectorKind sel = sourceKind == FS_STORE ? SelectorKind.FS_STORE : SelectorKind.ASK;
//        doTest(sourceKind, jsonPlans, "B2", sel, COUNT, ITERATE);
        doTest(sourceKind, jsonPlans, "B2", sel, COUNT, EMIT);
    }

    @RepeatedTest(10) void testS10Unexpected() throws Exception {
        try (var tasks = TestTaskSet.platformTaskSet("testS10Unexpected")) {
            tasks.repeat(Runtime.getRuntime().availableProcessors()*2, () -> {
                CompressedBatch s10 = QueryName.S10.expected(COMPRESSED);
                assertNotNull(s10);
                var dedup = StrongDedup.strongForever(COMPRESSED, s10.totalRows(), s10.cols);
                for (var node = s10; node != null; node = node.next) {
                    for (int r = 0; r < node.rows; r++) {
                        dedup.add(node, r);
                        assertTrue(dedup.contains(node, r));
                    }
                }
                for (var node = s10; node != null; node = node.next) {
                    for (int r = 0; r < node.rows; r++)
                        assertTrue(dedup.contains(node, r));
                    for (int r = node.rows-1; r >= 0; r--)
                        assertTrue(dedup.contains(node, r));
                }
            });
        }
    }
}