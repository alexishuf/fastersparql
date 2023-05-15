package com.github.alexishuf.fastersparql.fed.selectors;

import com.github.alexishuf.fastersparql.client.ResultsSparqlClient;
import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import com.github.alexishuf.fastersparql.fed.Selector;
import com.github.alexishuf.fastersparql.fed.Spec;
import com.github.alexishuf.fastersparql.operators.plan.Join;
import com.github.alexishuf.fastersparql.operators.plan.TriplePattern;
import com.github.alexishuf.fastersparql.util.BS;
import com.github.alexishuf.fastersparql.util.Results;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;

import static com.github.alexishuf.fastersparql.fed.Selector.InitOrigin.LOAD;
import static com.github.alexishuf.fastersparql.fed.Selector.InitOrigin.SPEC;
import static com.github.alexishuf.fastersparql.util.concurrent.Async.waitStage;
import static java.lang.System.arraycopy;
import static org.junit.jupiter.api.Assertions.*;

public abstract class SelectorTestBase {
    protected static final SparqlEndpoint ENDPOINT = SparqlEndpoint.parse("http://example.org/sparql");

    protected File absStateFileOrDir;
    protected Spec spec;
    protected ResultsSparqlClient client;
    protected abstract Spec createSpec(File relTo);

    @BeforeEach
    void setUp() throws IOException {
        absStateFileOrDir = Files.createTempFile("fastersparql", ".state").toFile().getAbsoluteFile();
        assertTrue(absStateFileOrDir.delete());
        absStateFileOrDir.deleteOnExit();
        File refDir = absStateFileOrDir.getParentFile();
        assertNotNull(refDir);
        spec = new Spec(createSpec(refDir));
        client = new ResultsSparqlClient(true, ENDPOINT);
    }

    @AfterEach
    void tearDown() { //noinspection ResultOfMethodCallIgnored
        absStateFileOrDir.delete();
        client.close();
    }

    protected Selector checkSavedOnInit(Selector selector) throws IOException {
        Spec stateSpec = spec.get(Selector.STATE, Spec.class);
        assertNotNull(stateSpec, "no state spec");

        // wait and read auto-saved state
        var origin = waitStage(selector.initialization());
        switch (origin) {
            case QUERY,LOAD -> assertTrue(absStateFileOrDir.exists());
            case SPEC       -> assertFalse(absStateFileOrDir.exists());
        }

        // load from state
        Selector loaded = Selector.load(client, spec);
        origin = waitStage(loaded.initialization());
        assertTrue(origin == LOAD || origin == SPEC, "origin=" + origin + ", expected LOAD or SPEC");
        return loaded;
    }

    protected Selector saveAndReload(Selector selector) throws IOException {
        Spec stateSpec = spec.get(Selector.STATE, Spec.class);
        assertNotNull(stateSpec, "no state spec");

        // wait and read auto-saved state
        var origin = waitStage(selector.initialization());
        switch (origin) {
            case QUERY,LOAD -> assertTrue(absStateFileOrDir.exists());
            case SPEC       -> assertFalse(absStateFileOrDir.exists());
        }

        // delete auto-saved state
        if (absStateFileOrDir.isDirectory()) {
            File[] files = absStateFileOrDir.listFiles();
            assertNotNull(files);
            List<File> failed = Arrays.stream(files).filter(f -> !f.delete()).toList();
            assertEquals(List.of(), failed, "Some files could not be deleted");
        }
        if (absStateFileOrDir.exists())
            assertTrue(absStateFileOrDir.delete());

        //explicitly save state and load from saved state
        selector.saveIfEnabled();
        selector.close();

        // load from state
        Selector loaded = Selector.load(client, spec);
        origin = waitStage(loaded.initialization());
        assertTrue(origin == LOAD || origin == SPEC, "origin=" + origin + ", expected LOAD or SPEC");
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
