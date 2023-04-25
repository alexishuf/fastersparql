package com.github.alexishuf.fastersparql.lrb.cmd;

import com.github.alexishuf.fastersparql.lrb.sources.LrbSource;
import com.github.alexishuf.fastersparql.lrb.sources.SourceKind;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class MeasureTest {
    private static final Logger log = LoggerFactory.getLogger(MeasureTest.class);
    public static final String DATA_DIR_PROP = "fastersparql.bench.test.data-dir";
    private static File dataDir;
    private static boolean hasHDT;

    @BeforeAll
    static void beforeAll() {
        String prop = System.getProperty(DATA_DIR_PROP);
        dataDir = prop != null && !prop.isEmpty() ? new File(prop) : new File("");
        hasHDT = Stream.of(LrbSource.DBPedia_Subset, LrbSource.NYT)
                .allMatch(s -> new File(dataDir, s.hdtFilename()).isFile());
    }

    private void doTestS2(SourceKind sourceKind, boolean jsonPlans) throws IOException {
        if (!hasHDT) {
            log.warn("Skipping test: no HDT files in {}. Set Java property {} to change directory",
                     dataDir.getAbsolutePath(), DATA_DIR_PROP);
            return;
        }
        File destDir = Files.createTempDirectory("fastersparql-measure").toFile();
        List<String> args = new ArrayList<>(List.of("measure",
                "--queries", "S2",
                "--lrb-source", "nyt", "--lrb-source",  "dbpedia-subset",
                "--source", sourceKind.name(),
                "--data-dir", dataDir.getPath(),
                "--dest-dir", destDir.getPath(),
                "--warm-secs", "0",
                "--warm-cool-ms", "500",
                "--cool-ms", "600",
                "--reps", "2",
                "--seed", "728305461"
        ));
        if (jsonPlans)
            args.add("--builtin-plans-json");
        App.run(args.toArray(String[]::new));
        var measurements = MeasurementCsv.load(new File(destDir, "measurements.csv"));
        assertEquals(2, measurements.size());
        for (Measurement m : measurements) {
            assertTrue(m.firstRowNs() > 0, "firstRowNs="+m.firstRowNs());
            assertTrue(m.allRowsNs() > 0, "allRowsNs="+m.allRowsNs());
            assertEquals(1, m.rows());
            assertTrue(m.terminalNs() > 0, "terminalNs="+m.terminalNs());
            assertFalse(m.cancelled());
            assertTrue(m.error() == null || m.error().isEmpty());
        }
    }

    @ParameterizedTest @ValueSource(strings = {"HDT_FILE", "HDT_WS", "HDT_JSON"})
    void testJsonPlans(String sourceName) throws IOException {
        doTestS2(SourceKind.valueOf(sourceName), true);
    }

    @ParameterizedTest @ValueSource(strings = {"HDT_FILE", "HDT_WS", "HDT_JSON"})
    void testFederationPlans(String sourceName) throws IOException {
        doTestS2(SourceKind.valueOf(sourceName), false);
    }
}