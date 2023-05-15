package com.github.alexishuf.fastersparql.store.batch;

import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.ByteRope;
import com.github.alexishuf.fastersparql.model.rope.TwoSegmentRope;
import com.github.alexishuf.fastersparql.store.index.dict.CompositeDictBuilder;
import com.github.alexishuf.fastersparql.store.index.dict.Dict;
import com.github.alexishuf.fastersparql.store.index.dict.LocalityCompositeDict;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.store.batch.IdTranslator.source;
import static com.github.alexishuf.fastersparql.store.batch.StoreBatch.TYPE;
import static com.github.alexishuf.fastersparql.store.index.dict.Splitter.Mode.LAST;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class StoreBatchTest {

    private static final int MAX_DIM = 32;
    private static Path dictPath;
    private static int dictId;
    private static LocalityCompositeDict dict;
    private static long[] sourcedIds;

    @BeforeAll static void beforeAll() throws IOException {
        Path tempDir = Files.createTempDirectory("fastersparql");
        tempDir.toFile().deleteOnExit();
        List<ByteRope> terms = new ArrayList<>(MAX_DIM*MAX_DIM);
        for (int r = 0; r < MAX_DIM; r++) {
            for (int c = 0; c < MAX_DIM; c++)
                terms.add(new ByteRope().append("<http://example.org/").append(r*c).append('>'));
        }
        try (var b = new CompositeDictBuilder(tempDir, tempDir, LAST, true)) {
            terms.forEach(b::visit);
            var secondPass = b.nextPass();
            terms.forEach(secondPass::visit);
            secondPass.write();
        }
        dictPath = tempDir.resolve("strings");
        dict = (LocalityCompositeDict) Dict.load(dictPath);
        dictId = IdTranslator.register(dict);
        sourcedIds = new long[MAX_DIM*MAX_DIM];
        var lookup = dict.lookup();
        int n = 0;
        for (ByteRope term : terms)
            sourcedIds[n++] = source(lookup.find(term), dictId);
    }

    @AfterAll static void afterAll() throws IOException {
        IdTranslator.deregister(dictId, dict);
        Path dir = dictPath.getParent();
        try (var paths = Files.newDirectoryStream(dir)) {
            for (Path p : paths)
                Files.deleteIfExists(p);
        }
        Files.deleteIfExists(dir);
    }

    static Stream<Arguments> testFill() {
        List<Integer> values = List.of(0, 1, 2, 3, 4, 7, 8, 15, 16, 17);
        return Stream.of(false, true).flatMap(grow ->
                values.stream().flatMap(rows ->
                        values.stream().map(cols -> arguments(grow, rows, cols))));
    }

    @ParameterizedTest @MethodSource
    void testFill(boolean grow, int rows, int cols) {
        StoreBatch b0 = grow ? TYPE.createSingleton(cols) : TYPE.create(rows, cols, 0);
        StoreBatch b1 = grow ? TYPE.createSingleton(cols) : TYPE.create(rows, cols, 0);
        StoreBatch b2 = grow ? TYPE.createSingleton(cols) : TYPE.create(rows, cols, 0);
        StoreBatch b3 = grow ? TYPE.createSingleton(cols) : TYPE.create(rows, cols, 0);
        StoreBatch b4 = grow ? TYPE.createSingleton(cols) : TYPE.create(rows, cols, 0);
        StoreBatch b5 = grow ? TYPE.createSingleton(cols) : TYPE.create(rows, cols, 0);
        b1.reserve(rows, cols);
        b3.reserve(rows, cols);
        b5.reserve(rows, cols);
        var lookup = IdTranslator.lookup(dictId);
        List<StoreBatch> batches = List.of(b0, b1, b2, b3, b4, b5);
        for (int r = 0; r < rows; r++) {
            b0.beginPut();
            b1.beginOffer();
            for (int c = 0; c < cols; c++) {
                b0.putTerm(c, sourcedIds[r*c]);
                assertTrue(b1.offerTerm(c, sourcedIds[r*c]));
            }
            b0.commitPut();
            b1.commitOffer();

            b2.beginPut();
            b3.beginOffer();
            for (int c = cols-1; c >= 0; c--) {
                b2.putTerm(c, sourcedIds[r*c]);
                assertTrue(b3.offerTerm(c, sourcedIds[r*c]));
            }
            b2.commitPut();
            b3.commitOffer();

            b4.putRow(b0, r);
            assertTrue(b5.offerRow(b0, r));

            for (int i = 0; i <= r; i++) {
                for (int c = 0; c < cols; c++) {
                    long id = b0.sourcedId(i, c);
                    TwoSegmentRope str = lookup.get(IdTranslator.unsource(id));
                    assertNotNull(str);
                    int hash = str.hashCode();
                    for (StoreBatch b : batches) {
                        assertEquals(id, b.sourcedId(i, c));
                        assertTrue(b0.equals(i, c, b, i, c));
                        assertTrue(b.equals(i, c, b0, i, c));
                        assertEquals(hash, b.hash(i, c));
                    }
                }
                int rowHash = b0.hash(i);
                for (StoreBatch b : batches) {
                    assertTrue(b0.equals(i, b, i));
                    assertTrue(b.equals(i, b0, i));
                    assertEquals(rowHash, b.hash(i));
                }
            }

            assertTrue(batches.stream().allMatch(b0::equals));
            assertTrue(batches.stream().allMatch(b -> b.equals(b0)));
        }
    }

    private StoreBatch mk(int cols, int... ids) {
        StoreBatch b = TYPE.create(ids.length / cols, cols, 0);
        for (int i = 0; i < ids.length; i += cols) {
            b.beginPut();
            for (int c = 0; c < cols; c++)
                b.putTerm(c, source(ids[i+c], dictId));
            b.commitPut();
        }
        return b;
    }

    private void check(StoreBatch b, int cols, int ... ids) {
        for (int i = 0, r = 0; i < ids.length; i += cols, ++r) {
            for (int c = 0; c < cols; c++)
                assertEquals(source(ids[i + c], dictId), b.sourcedId(r, c));
        }
    }

    @Test public void testRemoveRightCol() {
        var projector = TYPE.projector(Vars.of("x"), Vars.of("x", "y"));
        assertNotNull(projector);
        StoreBatch b0 = mk(2, 1, 2);
        StoreBatch b1 = mk(2, 1, 2, 3, 4, 5, 6);
        StoreBatch b2 = mk(2);
        assertSame(b0, projector.projectInPlace(b0));
        assertSame(b1, projector.projectInPlace(b1));
        assertSame(b2, projector.projectInPlace(b2));

        check(b0, 1, 1);
        check(b1, 1, 1, 3, 5);
        check(b2, 1);
    }

    @Test public void testRemoveLeftCol() {
        var projector = TYPE.projector(Vars.of("y"), Vars.of("x", "y"));
        assertNotNull(projector);
        StoreBatch b0 = mk(2, 1, 2);
        StoreBatch b1 = mk(2, 1, 2, 3, 4, 5, 6);
        assertSame(b0, projector.projectInPlace(b0));
        assertSame(b1, projector.projectInPlace(b1));

        check(b0, 1, 2);
        check(b1, 1, 2, 4, 6);
    }

    @Test public void testRemoveMidCol() {
        var projector = TYPE.projector(Vars.of("x", "z"), Vars.of("x", "y", "z"));
        assertNotNull(projector);
        StoreBatch b0 = mk(3, 1, 2, 3);
        StoreBatch b1 = mk(3, 1, 2, 3, 4, 5, 6);
        assertSame(b0, projector.projectInPlace(b0));
        assertSame(b1, projector.projectInPlace(b1));

        check(b0, 2, 1, 3);
        check(b1, 2, 1, 3, 4, 6);
    }

    @Test public void testRemoveOdd() {
        var filter = TYPE.filter((batch, row) -> (row & 1) == 1);
        assertNotNull(filter);
        StoreBatch b = mk(1, 0, 1, 2, 3);
        assertSame(b, filter.filterInPlace(b));
        check(b, 1, 0, 2);
    }


    @Test public void testRemoveEvenAndMidCol() {
        var filter = TYPE.filter(Vars.of("x", "z"), Vars.of("x", "y", "z"),
                                 (batch, row) -> (row & 1) == 0);
        assertNotNull(filter);
        StoreBatch b = mk(3,
                1, 2, 3,
                4, 5, 6,
                7, 8, 9);
        assertSame(b, filter.filterInPlace(b));
        check(b, 2, 4, 6);
    }
}