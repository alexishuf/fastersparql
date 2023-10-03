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
        HdtBatch b = HdtBatch.TYPE.create(1, 1, 0);
        assertEquals(1, b.rowsCapacity());
        b.beginPut();
        b.putTerm(0, Alice);
        b.commitPut();
        assertEquals(1, b.rows);
        assertEquals(1, b.cols);
        assertEquals(ALICE_T, b.get(0, 0));

        assertThrows(IndexOutOfBoundsException.class, () -> b.get(-1, 0));
        assertThrows(IndexOutOfBoundsException.class, () -> b.get(0, -1));
        assertThrows(IndexOutOfBoundsException.class, () -> b.get(0, 1));
        assertThrows(IndexOutOfBoundsException.class, () -> b.get(1, 0));

        HdtBatch copy0 = b.copy(null), copy1 = b.copy(null);
        assertEquals(b, copy0);
        assertEquals(b, copy1);
        assertNotSame(b, copy0);
        assertNotSame(b, copy1);
    }

    @RepeatedTest(2)
    public void testOfferThenPut() {
        HdtBatch two = HdtBatch.TYPE.createSingleton(2);
        two.beginPut();
        two.putTerm(0, Alice);
        two.putTerm(1, charlie);
        two.commitPut();
        assertEquals(ALICE_T, two.get(0, 0));
        assertEquals(CHARLIE_T, two.get(0, 1));

        HdtBatch b = HdtBatch.TYPE.create(1, 1, 0);
        b.beginPut();
        b.putTerm(0, two, 0, 0);
        b.commitPut();
        assertEquals(1, b.rows);
        assertEquals(ALICE_T, b.get(0, 0));

        b.beginPut();
        b.putTerm(0, two, 0, 1);
        b.commitPut();
        assertEquals(2, b.rows);
        assertEquals(ALICE_T, b.get(0, 0));
        assertEquals(CHARLIE_T, b.get(1, 0));

        b.putRow(new HdtBatch(new long[]{Bob}, 1, 1), 0);
        assertEquals(3, b.rows);
        assertEquals(ALICE_T, b.get(0, 0));
        assertEquals(CHARLIE_T, b.get(1, 0));
        assertEquals(BOB_T, b.get(2, 0));

        b = b.putConverting(new HdtBatch(new long[]{knows}, 1, 1));
        assertEquals(4, b.rows);
        assertEquals(ALICE_T, b.get(0, 0));
        assertEquals(CHARLIE_T, b.get(1, 0));
        assertEquals(BOB_T, b.get(2, 0));
        assertEquals(KNOWS_T, b.get(3, 0));
    }

    @Test
    public void testProjectInPlace() {
        var x = new IdBatch.Merger<>(HdtBatch.TYPE, Vars.of("x"), new int[]{1});
        var y = new IdBatch.Merger<>(HdtBatch.TYPE, Vars.of("y"), new int[]{2});
        HdtBatch in, ac;

        in = new HdtBatch(new long[]{Alice, Bob}, 1, 2);
        assertSame(in, ac = x.projectInPlace(in));
        assertEquals(new HdtBatch(new long[]{Alice}, 1, 1), ac);

        in = new HdtBatch(new long[]{Alice, Bob, charlie, knows}, 2, 2);
        assertSame(in, ac = x.projectInPlace(in));
        assertEquals(new HdtBatch(new long[]{Alice, charlie}, 2, 1), ac);

        in = new HdtBatch(new long[]{Alice, Bob}, 1, 2);
        assertSame(in, ac = y.projectInPlace(in));
        assertEquals(new HdtBatch(new long[]{Bob}, 1, 1), ac);
    }

    @Test public void testMerge() {
        var merger = new HdtBatch.Merger<>(HdtBatch.TYPE, Vars.of("x", "u", "y", "z"),
                                                        new int[]{1, 0, -1, 2});
        var l = new HdtBatch(new long[]{Alice, Bob, charlie, knows}, 2, 2);
        var r = new HdtBatch(new long[]{knows, charlie, Bob}, 3, 1);
        assertEquals(new HdtBatch(new long[]{Alice, 0, knows,   Bob,
                                             Alice, 0, charlie, Bob,
                                             Alice, 0, Bob,     Bob}, 3, 4),
                     merger.merge(null, l, 0, r));
        assertEquals(new HdtBatch(new long[]{charlie, 0, knows,   knows,
                                             charlie, 0, charlie, knows,
                                             charlie, 0, Bob,     knows}, 3, 4),
                merger.merge(null, l, 1, r));
    }


    @Test public void testFiler() {
        var filter = new HdtBatch.Filter<>(HdtBatch.TYPE, X, null, new RowFilter<>() {
            int calls = 0;
            @Override public Decision drop(HdtBatch batch, int row) { return calls++ == 0 ? Decision.DROP : Decision.KEEP; }
            @Override public void rebind(BatchBinding binding) throws RebindException {}
        }, null);

        var b = new HdtBatch(new long[]{Alice, Bob, charlie}, 3, 1);
        assertSame(b, filter.filterInPlace(b));
        assertEquals(new HdtBatch(new long[]{Bob, charlie}, 2, 1), b);
    }

    @Test public void testFilterProjecting() {
        var projector = HdtBatch.TYPE.projector(Vars.of("x"), Vars.of("x", "y"));
        var filter = new HdtBatch.Filter<>(HdtBatch.TYPE, X, projector, new RowFilter<>() {
            int calls = 0;
            @Override public Decision drop(HdtBatch batch, int row) { return calls++ == 0 ? Decision.DROP : Decision.KEEP; }
            @Override public void rebind(BatchBinding binding) throws RebindException {}
        }, null);

        var b = new HdtBatch(new long[]{Alice, Bob, charlie, knows}, 2, 2);
        assertSame(b, filter.filterInPlace(b));
        assertEquals(new HdtBatch(new long[]{charlie}, 1, 1), b);
    }
}