package com.github.alexishuf.fastersparql.lrb.cmd;

import com.github.alexishuf.fastersparql.FSProperties;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.CompressedBatch;
import com.github.alexishuf.fastersparql.client.netty.util.NettyChannelDebugger;
import com.github.alexishuf.fastersparql.lrb.query.QueryName;
import com.github.alexishuf.fastersparql.lrb.sources.LrbSource;
import com.github.alexishuf.fastersparql.lrb.sources.SelectorKind;
import com.github.alexishuf.fastersparql.lrb.sources.SourceKind;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
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

import static com.github.alexishuf.fastersparql.lrb.cmd.MeasureOptions.ResultsConsumer.CHECK;
import static com.github.alexishuf.fastersparql.lrb.cmd.MeasureOptions.ResultsConsumer.COUNT;
import static com.github.alexishuf.fastersparql.lrb.sources.SourceKind.*;
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
        CompressedBatch.DISABLE_VALIDATE = true;
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
        log.info("Loading expected results, this will take ~30s (due to CompressedBatch.validate())...");
        if (hasAllStore || hasAllHDT)
            Arrays.stream(QueryName.values()).parallel().forEach(n -> n.expected(Batch.COMPRESSED));
    }

    @AfterAll static void afterAll() {
        CompressedBatch.DISABLE_VALIDATE = false;
        System.setProperty(FSProperties.OP_CROSS_DEDUP_CAPACITY,
                           String.valueOf(FSProperties.DEF_OP_CROSS_DEDUP_CAPACITY));
        FSProperties.refresh();
    }

    private final List<File> tempDirs = new ArrayList<>();

    @BeforeEach void setUp() {
        NettyChannelDebugger.flushActive();
        System.setProperty(FSProperties.OP_CROSS_DEDUP_CAPACITY, "0");
        FSProperties.refresh();
    }

    @AfterEach void tearDown() {
        NettyChannelDebugger.flushActive();
        System.setProperty(FSProperties.OP_CROSS_DEDUP_CAPACITY,
                           String.valueOf(FSProperties.DEF_OP_CROSS_DEDUP_CAPACITY));
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
                        MeasureOptions.ResultsConsumer consumer) throws IOException {
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
                "--cool-ms", "600",
                "--reps", Integer.toString(nReps),
                "--seed", "728305461",
                "--consumer", consumer.name()
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
                assertEquals(exRows, rows, "unstable row count"+ctx);
            assertTrue(m.terminalNs() >= 0, "terminalNs="+m.terminalNs()+ctx);
            assertFalse(m.cancelled(), ctx);
            log.debug("{}, rep {} rows={}, allRows={}us", m.task().query(), m.rep(),
                      m.rows(),
                      String.format("%.3f", m.allRowsNs()/1_000.0));
        }
    }

    static Stream<Arguments> test() {
        List<SourceKind> sources = List.of(FS_STORE, HDT_FILE, HDT_JSON, HDT_WS, FS_TSV, FS_WS);
        return Stream.of(true, false).flatMap(jsonPlans
                -> sources.stream().map(src -> arguments(jsonPlans, src)));
    }

    @ParameterizedTest @MethodSource("test")
    void testS2(boolean jsonPlans, SourceKind sourceKind) throws Exception {
        doTest(sourceKind, jsonPlans, "S2", SelectorKind.DICT, COUNT);
    }

    @ParameterizedTest @MethodSource("test")
    void testSQueries(boolean jsonPlans, SourceKind sourceKind) throws Exception {
        doTest(sourceKind, jsonPlans, "S.*", SelectorKind.ASK, CHECK);
        NettyChannelDebugger.dumpAndFlushActive();
    }

    @ParameterizedTest @MethodSource("test")
    void testCQueries(boolean jsonPlans, SourceKind sourceKind) throws Exception {
        SelectorKind sel = sourceKind == FS_STORE ? SelectorKind.FS_STORE : SelectorKind.ASK;
        doTest(sourceKind, jsonPlans, "C.*", sel, CHECK);
    }

    @ParameterizedTest @MethodSource("test")
    void testBQueries(boolean jsonPlans, SourceKind sourceKind) throws Exception {
        SelectorKind sel = sourceKind == FS_STORE ? SelectorKind.FS_STORE : SelectorKind.ASK;
        doTest(sourceKind, jsonPlans, "B1", sel, COUNT);
    }

}