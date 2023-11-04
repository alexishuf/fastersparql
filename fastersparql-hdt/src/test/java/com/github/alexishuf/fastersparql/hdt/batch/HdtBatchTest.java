package com.github.alexishuf.fastersparql.hdt.batch;

import com.github.alexishuf.fastersparql.batch.type.IdBatch;
import com.github.alexishuf.fastersparql.batch.type.RowFilter;
import com.github.alexishuf.fastersparql.emit.exceptions.RebindException;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.sparql.binding.BatchBinding;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.rdfhdt.hdt.dictionary.Dictionary;

import java.io.IOException;

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

    @RepeatedTest(2)
    public void testSingletonOfferId() {
        HdtBatch b = HdtBatchType.HDT.create(1);
        assertEquals(1, b.rowsCapacity());
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

        HdtBatch copy0 = b.dup(), copy1 = b.dup();
        assertEquals(b, copy0);
        assertEquals(b, copy1);
        assertNotSame(b, copy0);
        assertNotSame(b, copy1);
    }

    @RepeatedTest(2)
    public void testOfferThenPut() {
        HdtBatch two = HdtBatchType.HDT.create(2);
        two.beginPut();
        two.putTerm(0, Alice);
        two.putTerm(1, charlie);
        two.commitPut();
        assertEquals(ALICE_T, two.get(0, 0));
        assertEquals(CHARLIE_T, two.get(0, 1));

        HdtBatch b = HdtBatchType.HDT.create(1);
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

        b.putRow(HdtBatch.of(1, 1, Bob), 0);
        assertEquals(3, b.totalRows());
        assertEquals(ALICE_T, b.get(0, 0));
        assertEquals(CHARLIE_T, b.get(1, 0));
        assertEquals(BOB_T, b.get(2, 0));

        b.putConverting(HdtBatch.of(1, 1, knows));
        assertEquals(4, b.totalRows());
        assertEquals(ALICE_T, b.get(0, 0));
        assertEquals(CHARLIE_T, b.get(1, 0));
        assertEquals(BOB_T, b.get(2, 0));
        assertEquals(KNOWS_T, b.get(3, 0));
    }

    @Test
    public void testProjectInPlace() {
        var x = new IdBatch.Merger<>(HdtBatchType.HDT, Vars.of("x"), new short[]{1});
        var y = new IdBatch.Merger<>(HdtBatchType.HDT, Vars.of("y"), new short[]{2});
        HdtBatch in, ac;

        in = HdtBatch.of(1, 2, Alice, Bob);
        assertSame(in, ac = x.projectInPlace(in));
        assertEquals(HdtBatch.of(1, 1, Alice), ac);

        in = HdtBatch.of(2, 2, Alice, Bob, charlie, knows);
        assertSame(in, ac = x.projectInPlace(in));
        assertEquals(HdtBatch.of(2, 1, Alice, charlie), ac);

        in = HdtBatch.of(1, 2, Alice, Bob);
        assertSame(in, ac = y.projectInPlace(in));
        assertEquals(HdtBatch.of(1, 1, Bob), ac);
    }

    @Test public void testMerge() {
        var merger = new IdBatch.Merger<>(HdtBatchType.HDT, Vars.of("x", "u", "y", "z"),
                                          new short[]{1, 0, -1, 2});
        var l = HdtBatch.of(2, 2, Alice, Bob, charlie, knows);
        var r = HdtBatch.of(3, 1, knows, charlie, Bob);
        HdtBatch ac = merger.merge(null, l, 0, r);
        assertEquals(HdtBatch.of(3, 4,
                        Alice, 0, knows,   Bob,
                             Alice, 0, charlie, Bob,
                             Alice, 0, Bob,     Bob), ac);
        ac = merger.merge(ac, l, 1, r);
        assertEquals(HdtBatch.of(3, 4,
                        charlie, 0, knows,   knows,
                             charlie, 0, charlie, knows,
                             charlie, 0, Bob,     knows), ac);
        ac.recycle();
    }


    @Test public void testFiler() {
        var filter = new IdBatch.Filter<>(HdtBatchType.HDT, X, null, new RowFilter<>() {
            int calls = 0;
            @Override public Decision drop(HdtBatch batch, int row) { return calls++ == 0 ? Decision.DROP : Decision.KEEP; }
            @Override public void rebind(BatchBinding binding) throws RebindException {}
        }, null);

        var b = HdtBatch.of(3, 1, Alice, Bob, charlie);
        assertSame(b, filter.filterInPlace(b));
        assertEquals(HdtBatch.of(2, 1, Bob, charlie), b);
    }

    @Test public void testFilterProjecting() {
        var projector = HdtBatchType.HDT.projector(Vars.of("x"), Vars.of("x", "y"));
        var filter = new IdBatch.Filter<>(HdtBatchType.HDT, X, projector, new RowFilter<>() {
            int calls = 0;
            @Override public Decision drop(HdtBatch batch, int row) { return calls++ == 0 ? Decision.DROP : Decision.KEEP; }
            @Override public void rebind(BatchBinding binding) throws RebindException {}
        }, null);

        var b = HdtBatch.of(2, 2, Alice, Bob, charlie, knows);
        assertSame(b, filter.filterInPlace(b));
        assertEquals(HdtBatch.of(1, 1, charlie), b);
    }
}