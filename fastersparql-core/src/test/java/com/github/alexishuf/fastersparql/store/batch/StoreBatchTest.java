package com.github.alexishuf.fastersparql.store.batch;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchFilter;
import com.github.alexishuf.fastersparql.batch.type.BatchMerger;
import com.github.alexishuf.fastersparql.batch.type.RowFilter;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.FinalSegmentRope;
import com.github.alexishuf.fastersparql.model.rope.RopeFactory;
import com.github.alexishuf.fastersparql.model.rope.TwoSegmentRope;
import com.github.alexishuf.fastersparql.sparql.binding.BatchBinding;
import com.github.alexishuf.fastersparql.store.index.dict.CompositeDictBuilder;
import com.github.alexishuf.fastersparql.store.index.dict.Dict;
import com.github.alexishuf.fastersparql.store.index.dict.LocalityCompositeDict;
import com.github.alexishuf.fastersparql.util.owned.AbstractOwned;
import com.github.alexishuf.fastersparql.util.owned.Guard;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import com.github.alexishuf.fastersparql.util.owned.Owned;
import org.checkerframework.checker.nullness.qual.Nullable;
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

import static com.github.alexishuf.fastersparql.batch.IntsBatch.X;
import static com.github.alexishuf.fastersparql.batch.type.RowFilter.Decision.DROP;
import static com.github.alexishuf.fastersparql.batch.type.RowFilter.Decision.KEEP;
import static com.github.alexishuf.fastersparql.store.batch.IdTranslator.source;
import static com.github.alexishuf.fastersparql.store.batch.StoreBatchType.STORE;
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
        List<FinalSegmentRope> terms = new ArrayList<>(MAX_DIM*MAX_DIM);
        String prefix = "<http://example.org/";
        for (int r = 0, reqBytes = prefix.length() + 12; r < MAX_DIM; r++) {
            for (int c = 0; c < MAX_DIM; c++)
                terms.add(RopeFactory.make(reqBytes).add(prefix).add((long)r*c).add('>').take());
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
        try (var lookupG = new Guard<LocalityCompositeDict.Lookup>(StoreBatchTest.class)) {
            var lookup = lookupG.set(dict.lookup());
            int n = 0;
            for (var term : terms)
                sourcedIds[n++] = source(lookup.find(term), dictId);
        }
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

    private static class G implements AutoCloseable {
        private final List<StoreBatch> batches = new ArrayList<>();
        private final List<Owned<?>> guarded = new ArrayList<>();

        @Override public void close() {
            for (int i = 0, n = batches.size(); i < n; i++) {
                StoreBatch b = batches.get(i);
                if (b != null)
                    batches.set(i, b.recycle(this));
            }
            for (int i = 0, n = guarded.size(); i < n; i++) {
                var o = guarded.get(i);
                if (o != null)
                    guarded.set(i, o.recycle(this));
            }
        }

        public StoreBatch create(int idx, int cols) {
            return set(idx, STORE.create(cols).takeOwnership(this));
        }
        public StoreBatch set(int idx, @Nullable Orphan<StoreBatch> orphan) {
            return set(idx, Orphan.takeOwnership(orphan, this));
        }
        public Orphan<StoreBatch> take(int idx) {
            Orphan<StoreBatch> b = batches.get(idx).releaseOwnership(this);
            batches.set(idx, null);
            return b;
        }
        public StoreBatch set(int idx, @Nullable StoreBatch b) {
            while (batches.size() <= idx) batches.add(null);
            StoreBatch old = batches.get(idx);
            if (old != null && old != b)
                old.recycle(this);
            batches.set(idx, b);
            return b;
        }
        public StoreBatch mk(int idx, int cols, int... ids) {
            StoreBatch b = create(idx, cols);
            for (int i = 0; i < ids.length; i += cols) {
                b.beginPut();
                for (int c = 0; c < cols; c++)
                    b.putTerm(c, source(ids[i+c], dictId));
                b.commitPut();
            }
            return b;
        }
        public LocalityCompositeDict.Lookup lookup(Orphan<LocalityCompositeDict.Lookup> orphan) {
            var l = orphan.takeOwnership(this);
            guarded.add(l);
            return l;
        }
        public BatchMerger<StoreBatch, ?> merger(@Nullable Orphan<? extends BatchMerger<StoreBatch, ?>> m) {
            if (m == null)
                return null;
            var o = (BatchMerger<StoreBatch, ?>)  m.takeOwnership(this);
            guarded.add(o);
            return o;
        }
        public BatchFilter<StoreBatch, ?> filter(@Nullable Orphan<? extends BatchFilter<StoreBatch, ?>> m) {
            if (m == null)
                return null;
            var o = (BatchFilter<StoreBatch, ?>)  m.takeOwnership(this);
            guarded.add(o);
            return o;
        }
    }

    @ParameterizedTest @MethodSource
    void testFill(boolean grow, int rows, int cols) {
        try (var g = new G()) {
            StoreBatch b0 = g.create(0, cols);
            StoreBatch b2 = g.create(1, cols);
            StoreBatch b4 = g.create(2, cols);
            var lookup = g.lookup(IdTranslator.dict(dictId).lookup());
            List<StoreBatch> batches = new ArrayList<>(List.of(b0, b2, b4));
            for (int r = 0; r < rows; r++) {
                b0.beginPut();
                for (int c = 0; c < cols; c++) b0.putTerm(c, sourcedIds[r*c]);
                b0.commitPut();

                b2.beginPut();
                for (int c = cols-1; c >= 0; c--) b2.putTerm(c, sourcedIds[r*c]);
                b2.commitPut();

                b4.linkedPutRow(b0, r);

                for (int i = 0; i <= r; i++) {
                    for (int c = 0; c < cols; c++) {
                        long id = b0.linkedId(i, c);
                        TwoSegmentRope str = lookup.get(IdTranslator.unsource(id));
                        assertNotNull(str);
                        int hash = str.hashCode();
                        for (StoreBatch b : batches) {
                            assertEquals(id, b.linkedId(i, c));
                            assertTrue(b0.linkedEquals(i, c, b, i, c));
                            assertTrue(b.linkedEquals(i, c, b0, i, c));
                            assertEquals(hash, b.linkedHash(i, c));
                        }
                    }
                    int rowHash = b0.linkedHash(i);
                    for (StoreBatch b : batches) {
                        assertTrue(b0.linkedEquals(i, b, i));
                        assertTrue(b.linkedEquals(i, b0, i));
                        assertEquals(rowHash, b.linkedHash(i));
                    }
                }

                assertTrue(batches.stream().allMatch(b0::equals));
                assertTrue(batches.stream().allMatch(b -> b.equals(b0)));
            }
        }
    }


    private void check(StoreBatch b, int cols, int ... ids) {
        for (int i = 0, r = 0; i < ids.length; i += cols, ++r) {
            for (int c = 0; c < cols; c++)
                assertEquals(source(ids[i + c], dictId), b.id(r, c));
        }
    }

    @Test public void testRemoveRightCol() {
        try (var g = new G()) {
            var projector = g.merger(STORE.projector(Vars.of("x"), Vars.of("x", "y")));
            assertNotNull(projector);
            StoreBatch b0 = g.mk(0, 2, 1, 2);
            StoreBatch b1 = g.mk(1, 2, 1, 2, 3, 4, 5, 6);
            StoreBatch b2 = g.mk(2, 2);
            var p0 = g.set(3, projector.projectInPlace(g.take(0)));
            var p1 = g.set(4, projector.projectInPlace(g.take(1)));
            var p2 = g.set(5, projector.projectInPlace(g.take(2)));
            assertSame(b0, p0);
            assertSame(b1, p1);
            assertSame(b2, p2);

            check(b0, 1, 1);
            check(b1, 1, 1, 3, 5);
            check(b2, 1);
        }
    }

    @Test public void testRemoveLeftCol() {
        try (var g = new G()) {
            var projector = g.merger(STORE.projector(Vars.of("y"), Vars.of("x", "y")));
            assertNotNull(projector);
            StoreBatch b0 = g.mk(0, 2, 1, 2);
            StoreBatch b1 = g.mk(1, 2, 1, 2, 3, 4, 5, 6);
            var p0 = g.set(2, projector.projectInPlace(g.take(0)));
            var p1 = g.set(3, projector.projectInPlace(g.take(1)));
            assertSame(b0, p0);
            assertSame(b1, p1);

            check(b0, 1, 2);
            check(b1, 1, 2, 4, 6);
        }
    }

    @Test public void testRemoveMidCol() {
        try (var g = new G()) {
            var projector = g.merger(STORE.projector(Vars.of("x", "z"), Vars.of("x", "y", "z")));
            assertNotNull(projector);
            StoreBatch b0 = g.mk(0, 3, 1, 2, 3);
            StoreBatch b1 = g.mk(1, 3, 1, 2, 3, 4, 5, 6);
            var p0 = g.set(2, projector.projectInPlace(g.take(0)));
            var p1 = g.set(3, projector.projectInPlace(g.take(1)));
            assertSame(b0, p0);
            assertSame(b1, p1);

            check(b0, 2, 1, 3);
            check(b1, 2, 1, 3, 4, 6);
        }
    }

    private abstract static class RF<B extends Batch<B>>
            extends AbstractOwned<RF<B>>
            implements RowFilter<B, RF<B>>, Orphan<RF<B>> {
        @Override public RF<B> takeOwnership(Object o) {return takeOwnership0(o);}
        @Override public @Nullable RF<B> recycle(Object currentOwner) {return null;}
    }

    @Test public void testRemoveOdd() {
        try (var g = new G()) {
            var filter = g.filter(STORE.filter(X,
                    new RF<>() {
                        @Override public Decision drop(StoreBatch batch, int row) {
                            return (row & 1) == 1 ? DROP : KEEP;
                        }
                        @Override public void rebind(BatchBinding binding) {}
                    }));
            assertNotNull(filter);
            StoreBatch b = g.mk(0, 1, 0, 1, 2, 3);
            StoreBatch p = g.set(1, filter.filterInPlace(g.take(0)));
            assertSame(b, p);
            check(b, 1, 0, 2);
        }
    }


    @Test public void testRemoveEvenAndMidCol() {
        try (var g = new G()) {
            var filter = g.filter(STORE.filter(Vars.of("x", "z"), Vars.of("x", "y", "z"),
                    new RF<>() {
                        @Override public Decision drop(StoreBatch batch, int row) {
                            return (row & 1) == 0 ? DROP : KEEP;
                        }
                        @Override public void rebind(BatchBinding binding) {}
                    }));
            assertNotNull(filter);
            StoreBatch b = g.mk(0, 3,
                    1, 2, 3,
                    4, 5, 6,
                    7, 8, 9);
            StoreBatch ac = g.set(1, filter.filterInPlace(g.take(0)));
            assertSame(b, ac);
            check(b, 2, 4, 6);
        }
    }
}