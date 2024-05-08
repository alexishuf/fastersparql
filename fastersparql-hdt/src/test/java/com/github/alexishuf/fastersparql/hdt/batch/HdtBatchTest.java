package com.github.alexishuf.fastersparql.hdt.batch;

import com.github.alexishuf.fastersparql.batch.type.BatchFilter;
import com.github.alexishuf.fastersparql.batch.type.BatchMerger;
import com.github.alexishuf.fastersparql.batch.type.RowFilter;
import com.github.alexishuf.fastersparql.emit.exceptions.RebindException;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.sparql.binding.BatchBinding;
import com.github.alexishuf.fastersparql.util.owned.AbstractOwned;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import com.github.alexishuf.fastersparql.util.owned.Owned;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.rdfhdt.hdt.dictionary.Dictionary;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.github.alexishuf.fastersparql.hdt.batch.HdtBatchType.HDT;
import static com.github.alexishuf.fastersparql.hdt.batch.IdAccessTest.*;
import static org.junit.jupiter.api.Assertions.*;

class HdtBatchTest {
    private static final Vars X = Vars.of("x");
    private static Dictionary dict;
    private static int dictId;
    private static long Alice, Bob, knows, charlie;

    @BeforeAll
    static void beforeAll() {
        dict    = dummyDict();
        dictId  = IdAccess.register(dict);
        Alice   = Alice(dictId);
        Bob     = Bob(dictId);
        knows   = knows(dictId);
        charlie = charlie(dictId);
    }

    @AfterAll
    static void afterAll() throws IOException {
        IdAccess.release(dictId);
        dictId = 0;
        dict.close();
        dict = null;
    }

    private static class G implements AutoCloseable {
        private final List<HdtBatch> batches = new ArrayList<>();
        private final List<Owned<?>> guarded = new ArrayList<>();

        @Override public void close() {
            for (int i = 0, n = batches.size(); i < n; i++) {
                HdtBatch b = batches.get(i);
                if (b != null && b.isOwner(this))
                    batches.set(i, b.recycle(this));
            }
            for (int i = 0, n = guarded.size(); i < n; i++) {
                var o = guarded.get(i);
                if (o != null && o.isOwner(this))
                    guarded.set(i, o.recycle(this));
            }
        }

        public HdtBatch create(int idx, int cols) {
            return set(idx, HDT.create(cols).takeOwnership(this));
        }
        public HdtBatch set(int idx, @Nullable Orphan<HdtBatch> orphan) {
            return set(idx, Orphan.takeOwnership(orphan, this));
        }
        public Orphan<HdtBatch> take(int idx) {
            Orphan<HdtBatch> b = batches.get(idx).releaseOwnership(this);
            batches.set(idx, null);
            return b;
        }
        public HdtBatch set(int idx, @Nullable HdtBatch b) {
            while (batches.size() <= idx) batches.add(null);
            HdtBatch old = batches.get(idx);
            if (old != null && old != b && old.isOwner(this))
                old.recycle(this);
            batches.set(idx, b);
            return b;
        }
        public BatchMerger<HdtBatch, ?> merger(@Nullable Orphan<? extends BatchMerger<HdtBatch, ?>> m) {
            if (m == null)
                return null;
            var o = (BatchMerger<HdtBatch, ?>)  m.takeOwnership(this);
            guarded.add(o);
            return o;
        }
        public BatchFilter<HdtBatch, ?> filter(@Nullable Orphan<? extends BatchFilter<HdtBatch, ?>> m) {
            if (m == null)
                return null;
            var o = (BatchFilter<HdtBatch, ?>)  m.takeOwnership(this);
            guarded.add(o);
            return o;
        }
    }

    @RepeatedTest(2)
    public void testSingletonOfferId() {
        try (var g = new G()) {
            HdtBatch b = g.create(0, 1);
            b.beginPut();
            b.putTerm(0, Alice);
            b.commitPut();
            assertEquals(1, b.totalRows());
            assertEquals(1, b.cols);
            assertEquals(ALICE_T, b.get(0, 0));

            assertThrows(IndexOutOfBoundsException.class, () -> b.get(-1, 0));
            assertThrows(IndexOutOfBoundsException.class, () -> b.get(0, -1));
            assertThrows(IndexOutOfBoundsException.class, () -> b.get(0, 1));
            assertThrows(IndexOutOfBoundsException.class, () -> b.get(1, 0));

            HdtBatch copy0 = g.set(1, b.dup()), copy1 = g.set(2, b.dup());
            assertEquals(b, copy0);
            assertEquals(b, copy1);
            assertNotSame(b, copy0);
            assertNotSame(b, copy1);
        }
    }

    @RepeatedTest(2)
    public void testOfferThenPut() {
        try (var g = new G()) {
            HdtBatch two = g.create(0, 2);
            two.beginPut();
            two.putTerm(0, Alice);
            two.putTerm(1, charlie);
            two.commitPut();
            assertEquals(ALICE_T, two.get(0, 0));
            assertEquals(CHARLIE_T, two.get(0, 1));

            HdtBatch b = g.create(1, 1);
            b.beginPut();
            b.putTerm(0, two, 0, 0);
            b.commitPut();
            assertEquals(1, b.rows);
            assertEquals(ALICE_T, b.get(0, 0));

            b.beginPut();
            b.putTerm(0, two, 0, 1);
            b.commitPut();
            assertEquals(2, b.totalRows());
            assertEquals(ALICE_T, b.get(0, 0));
            assertEquals(CHARLIE_T, b.get(1, 0));

            HdtBatch bobBatch = g.set(2, HdtBatch.of(1, 1, Bob));
            b.putRow(bobBatch, 0);
            assertEquals(3, b.totalRows());
            assertEquals(ALICE_T, b.get(0, 0));
            assertEquals(CHARLIE_T, b.get(1, 0));
            assertEquals(BOB_T, b.get(2, 0));

            HdtBatch knowsBatch = g.set(3, HdtBatch.of(1, 1, knows));
            b.putConverting(knowsBatch);
            assertEquals(4, b.totalRows());
            assertEquals(ALICE_T, b.get(0, 0));
            assertEquals(CHARLIE_T, b.get(1, 0));
            assertEquals(BOB_T, b.get(2, 0));
            assertEquals(KNOWS_T, b.get(3, 0));
        }
    }

    @Test
    public void testProjectInPlace() {
        try (var g = new G()) {
            var x = g.merger(HDT.projector(X, Vars.of("x", "y")));
            var y = g.merger(HDT.projector(Vars.of("y"), Vars.of("x", "y")));
            assertNotNull(x);
            assertNotNull(y);
            HdtBatch in, ac;

            in = g.set(0, HdtBatch.of(1, 2, Alice, Bob));
            assertSame(in, ac = g.set(1, x.projectInPlace(g.take(0))));
            assertEquals(g.set(2, HdtBatch.of(1, 1, Alice)), ac);

            in = g.set(0, HdtBatch.of(2, 2, Alice, Bob, charlie, knows));
            assertSame(in, ac = g.set(1, x.projectInPlace(g.take(0))));
            assertEquals(g.set(2, HdtBatch.of(2, 1, Alice, charlie)), ac);

            in = g.set(0, HdtBatch.of(1, 2, Alice, Bob));
            assertSame(in, ac = g.set(1, y.projectInPlace(g.take(0))));
            assertEquals(g.set(2, HdtBatch.of(1, 1, Bob)), ac);
        }

    }

    @Test public void testMerge() {
        try (var g = new G()) {
            var merger = g.merger(HDT.merger(Vars.of("x", "u", "y", "z"), Vars.of("x", "y"),
                                             Vars.of("z")));
            var l = g.set(0, HdtBatch.of(2, 2, Alice, Bob, charlie, knows));
            var r = g.set(1, HdtBatch.of(3, 1, knows, charlie, Bob));
            HdtBatch ac = g.set(2, merger.merge(null, l, 0, r));
            assertEquals(g.set(3, HdtBatch.of(3, 4,
                    Alice, 0, knows,   Bob,
                    Alice, 0, charlie, Bob,
                    Alice, 0, Bob,     Bob)), ac);
            ac.clear();
            ac = g.set(2, merger.merge(g.take(2), l, 1, r));
            assertEquals(g.set(3, HdtBatch.of(3, 4,
                    charlie, 0, knows,   knows,
                    charlie, 0, charlie, knows,
                    charlie, 0, Bob,     knows)), ac);
        }
    }

    private static abstract class RF extends AbstractOwned<RF>
            implements RowFilter<HdtBatch, RF>, Orphan<RF>  {
        @Override public RF takeOwnership(Object newOwner) {return takeOwnership0(newOwner);}
        @Override public @Nullable RF recycle(Object currentOwner) {return null;}
        @Override public void rebind(BatchBinding binding) throws RebindException {
            throw new UnsupportedOperationException();
        }
    }

    @Test public void testFilter() {
        try (var g = new G()) {
            var filter = g.filter(HDT.filter(X, new RF() {
                int calls = 0;
                @Override public Decision drop(HdtBatch batch, int row) { return calls++ == 0 ? Decision.DROP : Decision.KEEP; }
            }));
            var b = g.set(0, HdtBatch.of(3, 1, Alice, Bob, charlie));
            assertSame(b, g.set(0, filter.filterInPlace(g.take(0))));
            assertEquals(g.set(1, HdtBatch.of(2, 1, Bob, charlie)), b);
        }
    }

    @Test public void testFilterProjecting() {
        try (var g = new G()) {
            var filter = g.filter(HDT.filter(X, Vars.of("x", "y"), new RF() {
                int calls = 0;
                @Override public Decision drop(HdtBatch batch, int row) {
                    return calls++ == 0 ? Decision.DROP : Decision.KEEP;
                }
            }));

            var b = g.set(0, HdtBatch.of(2, 2, Alice, Bob, charlie, knows));
            assertSame(b, g.set(0, filter.filterInPlace(g.take(0))));
            assertEquals(g.set(1, HdtBatch.of(1, 1, charlie)), b);
        }
    }
}