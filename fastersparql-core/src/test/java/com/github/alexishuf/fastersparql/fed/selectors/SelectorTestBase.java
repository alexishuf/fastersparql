package com.github.alexishuf.fastersparql.fed.selectors;

import com.github.alexishuf.fastersparql.client.ResultsSparqlClient;
import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import com.github.alexishuf.fastersparql.fed.Selector;
import com.github.alexishuf.fastersparql.fed.Spec;
import com.github.alexishuf.fastersparql.operators.plan.Join;
import com.github.alexishuf.fastersparql.operators.plan.TriplePattern;
import com.github.alexishuf.fastersparql.util.BS;
import com.github.alexishuf.fastersparql.util.IOUtils;
import com.github.alexishuf.fastersparql.util.Results;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;

import static com.github.alexishuf.fastersparql.fed.Selector.InitOrigin.*;
import static com.github.alexishuf.fastersparql.util.concurrent.Async.waitStage;
import static java.lang.System.arraycopy;
import static org.junit.jupiter.api.Assertions.*;

public abstract class SelectorTestBase {
    protected static final SparqlEndpoint ENDPOINT = SparqlEndpoint.parse("http://example.org/sparql");

    protected File absStateFile;
    protected Spec spec;
    protected ResultsSparqlClient client;
    protected abstract Spec createSpec();

    @BeforeEach
    void setUp() throws IOException {
        absStateFile = Files.createTempFile("fastersparql", ".state").toFile().getAbsoluteFile();
        assertTrue(absStateFile.delete());
        absStateFile.deleteOnExit();
        File refDir = absStateFile.getParentFile();
        spec = new Spec(createSpec());
        spec.set(Spec.PATHS_RELATIVE_TO, refDir.toString());
        spec.set("state", Spec.of("file", absStateFile.getName(), "init-save", true));
        client = new ResultsSparqlClient(true, ENDPOINT);
    }

    @AfterEach
    void tearDown() { //noinspection ResultOfMethodCallIgnored
        absStateFile.delete();
        client.close();
    }

    protected Selector saveAndLoad(Selector selector) throws IOException {
        // wait and read auto-saved state
        var origin = waitStage(selector.initialization());
        if (origin == QUERY)
            assertTrue(absStateFile.exists());
        else if (origin != LOAD)
            assertFalse(absStateFile.exists());
        String autoSavedState = null;
        if (absStateFile.exists()) {
            assertTrue(absStateFile.isFile());
            autoSavedState = IOUtils.readAll(absStateFile);
        }

        // corrupt auto-saved state
        try (FileWriter w = new FileWriter(absStateFile, true)) {
            w.append("garbage\n");
        }

        //explicitly save state and validate
        File savedTo = waitStage(selector.saveIfEnabled(null));
        assertNotNull(savedTo);
        assertEquals(absStateFile.getAbsolutePath(), savedTo.getAbsolutePath());
        if (autoSavedState != null)
            assertEquals(autoSavedState, IOUtils.readAll(absStateFile));

        // load from state
        Selector loaded = Selector.load(client, spec);
        origin = waitStage(loaded.initialization());
        assertTrue(origin == LOAD || origin == SPEC, "origin="+origin+", expected LOAD or SPEC");
        return loaded;
    }

    protected void testTPs(Selector sel, List<String> positiveTTL, List<String> negativeTTL) {
        var positive = positiveTTL.stream().map(Results::parseTP).toArray(TriplePattern[]::new);
        var negative = negativeTTL.stream().map(Results::parseTP).toArray(TriplePattern[]::new);
        long[] bitset   = new long[BS.longsFor(positive.length+ negative.length)];
        long[] exBitset = new long[BS.longsFor(positive.length+ negative.length)];

        for (var tp : positive)
            assertTrue(sel.has(tp), "tp="+tp);
        for (var tp : negative)
            assertFalse(sel.has(tp), "tp="+tp);

        if (positive.length > 1) {
            assertEquals((1L << positive.length)-1, sel.subBitset(new Join(positive)));
            assertEquals(positive.length, sel.subBitset(bitset, new Join(positive)));
            BS.set(exBitset, 0, positive.length);
            assertArrayEquals(exBitset, bitset);
        }

        if (negative.length > 1) {
            assertEquals(0L, sel.subBitset(new Join(negative)));
            Arrays.fill(exBitset, 0);
            Arrays.fill(bitset, 0);
            assertEquals(0, sel.subBitset(bitset, new Join(negative)));
            assertArrayEquals(exBitset, bitset);
        }

        if (positive.length + negative.length > 1) {
            var all = Arrays.copyOf(negative, positive.length+negative.length);
            arraycopy(positive, 0, all, negative.length, positive.length);
            Join join = new Join(all);

            assertEquals((1L<<positive.length)-1 << negative.length, sel.subBitset(join));

            Arrays.fill(bitset, 0);
            Arrays.fill(exBitset, 0);
            BS.set(exBitset, negative.length, negative.length+positive.length);
            assertEquals(positive.length, sel.subBitset(bitset, join));
            assertArrayEquals(exBitset, bitset);
        }
    }
}
