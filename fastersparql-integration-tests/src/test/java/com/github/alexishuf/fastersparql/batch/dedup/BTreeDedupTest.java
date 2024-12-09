package com.github.alexishuf.fastersparql.batch.dedup;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.batch.type.ScopedIdBatchType;
import com.github.alexishuf.fastersparql.hdt.batch.IdAccess;
import com.github.alexishuf.fastersparql.model.rope.FinalSegmentRope;
import com.github.alexishuf.fastersparql.model.rope.MutableRope;
import com.github.alexishuf.fastersparql.model.rope.TwoSegmentRope;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import com.github.alexishuf.fastersparql.util.owned.Owned;
import com.github.alexishuf.fastersparql.util.owned.StaticMethodOwner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.rdfhdt.hdt.dictionary.impl.FourSectionDictionary;
import org.rdfhdt.hdt.dictionary.impl.section.PFCDictionarySection;
import org.rdfhdt.hdt.options.HDTSpecification;

import java.util.*;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.batch.type.CABatchType.CA;
import static com.github.alexishuf.fastersparql.batch.type.CompressedBatchType.COMPRESSED;
import static com.github.alexishuf.fastersparql.batch.type.SharedKind.*;
import static com.github.alexishuf.fastersparql.batch.type.TermBatchType.TERM;
import static com.github.alexishuf.fastersparql.hdt.batch.HdtBatchType.HDT;
import static com.github.alexishuf.fastersparql.model.rope.FinalSegmentRope.EMPTY;
import static com.github.alexishuf.fastersparql.model.rope.SharedRopes.DT_integer;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class BTreeDedupTest {
    private static final int MAX_ROWS = 8192;
    private static ScopedIdBatchType.WithScope SCOPED;
    private static List<BatchType<?>> BATCH_TYPES;
    private static List<BatchType<?>> CRITICAL_BATCH_TYPES;
    private static int hdtDictId;

    @BeforeAll static void beforeAll() {
        SCOPED = ScopedIdBatchType.beginScope().takeOwnership(BTreeDedupTest.class);
        BATCH_TYPES = List.of(COMPRESSED, TERM, CA, SCOPED, HDT);
        CRITICAL_BATCH_TYPES = List.of(COMPRESSED, HDT);

        TreeSet<String> iris = new TreeSet<>(), lits = new TreeSet<>();
        for (RowGenerator gen : RowGenerator.values()) {
            var b = gen.generate(COMPRESSED, Order.INCREASING, MAX_ROWS)
                       .takeOwnership(BTreeDedupTest.class);
            var view = new TwoSegmentRope();
            for (var node = b; node != null; node = node.next) {
                for (int r = 0, rows = node.rows, cols = node.cols; r < rows; r++) {
                    for (int c = 0; c < cols; c++) {
                        if (node.getRopeView(r, c, view)) {
                            switch (node.termType(r, c)) {
                                case IRI -> iris.add(view.toString(1, view.len-1));
                                case LIT -> lits.add(view.toString());
                                case null, default -> throw new UnsupportedOperationException("Expected only IRIs and literals");
                            }
                        }
                    }
                }
            }
            Batch.safeRecycle(b, BTreeDedupTest.class);
        }
        var dict = new FourSectionDictionary(new HDTSpecification());
        var predicates = (PFCDictionarySection) dict.getPredicates();
        var subjects   = (PFCDictionarySection) dict.getSubjects();
        var objects    = (PFCDictionarySection) dict.getObjects();
        predicates.load(List.of(Term.RDF_TYPE.toString()).iterator(), 1, null);
        subjects  .load(iris.iterator(), iris.size(), null);
        objects   .load(lits.iterator(), lits.size(), null);
        hdtDictId = IdAccess.register(dict);
    }

    @AfterAll static void afterAll() {
        BATCH_TYPES = BATCH_TYPES.stream().filter(t -> t != SCOPED).toList();
        SCOPED = Owned.safeRecycle(SCOPED, BTreeDedupTest.class);
        if (hdtDictId != 0) {
            IdAccess.release(hdtDictId);
            hdtDictId = 0;
        }
    }

    enum Order {
        INCREASING,
        DECREASING,
        INWARD,
        OUTWARD,
        RANDOM;

        public static final Order[] VALUES = values();

        public int[] order(int rows) {
            int[] values = new int[rows];
            switch (this) {
                case INCREASING -> {
                    for (int i = 0; i < rows; i++) values[i] = i;
                }
                case DECREASING -> {
                    for (int i = 0; i < rows; i++) values[i] = rows-1-i;
                }
                case INWARD -> {
                    for (int i = 0, o = 0, half = rows>>1; i < half; i++) {
                        values[o++] = i;
                        values[o++] = rows-1-i;
                    }
                    if ((rows&1) == 1) values[rows-1] = rows>>1;
                }
                case OUTWARD -> {
                    for (int i = 0, o = 0, half = rows>>1; i < half; i++) {
                        values[o++] = half+i;
                        values[o++] = half-1-i;
                    }
                    if ((rows&1) == 1) values[rows-1] = rows-1;
                }
                case RANDOM -> {
                    var r = new Random(604387366);
                    for (int i = 0; i < rows; i++)
                        values[i] = i;
                    for (int i = 0; i < rows; i++) {
                        int otherIdx     = r.nextInt(rows);
                        int otherValue   = values[otherIdx];
                        values[otherIdx] = values[i];
                        values[i]        = otherValue;
                    }
                }
            }
            BitSet seen = new BitSet(rows);
            for (int v : values) {
                assertFalse(seen.get(v), "non unique value in order");
                seen.set(v);
            }
            return values;
        }
    }

    @ParameterizedTest @EnumSource(Order.class)
    void selfTestSequence(Order s) {
        for (int n = 0; n <= 8; n++) {
            int[] values = s.order(n), expected = new int[n];
            for (int i = 0; i < n; i++)
                expected[i] = i;
            Arrays.sort(values);
            assertArrayEquals(expected, values);
        }
    }

    @Test void selfTestIncreasingSequence() {
        assertArrayEquals(new int[]{0},         Order.INCREASING.order(1));
        assertArrayEquals(new int[]{0,1},       Order.INCREASING.order(2));
        assertArrayEquals(new int[]{0,1,2},     Order.INCREASING.order(3));
        assertArrayEquals(new int[]{0,1,2,3},   Order.INCREASING.order(4));
        assertArrayEquals(new int[]{0,1,2,3,4}, Order.INCREASING.order(5));
    }
    @Test void selfTestDecreasingSequence() {
        assertArrayEquals(new int[]{0},         Order.DECREASING.order(1));
        assertArrayEquals(new int[]{1,0},       Order.DECREASING.order(2));
        assertArrayEquals(new int[]{2,1,0},     Order.DECREASING.order(3));
        assertArrayEquals(new int[]{3,2,1,0},   Order.DECREASING.order(4));
        assertArrayEquals(new int[]{4,3,2,1,0}, Order.DECREASING.order(5));
    }
    @Test void selfTestInwardSequence() {
        assertArrayEquals(new int[]{0},         Order.INWARD.order(1));
        assertArrayEquals(new int[]{0,1},       Order.INWARD.order(2));
        assertArrayEquals(new int[]{0,2,1},     Order.INWARD.order(3));
        assertArrayEquals(new int[]{0,3,1,2},   Order.INWARD.order(4));
        assertArrayEquals(new int[]{0,4,1,3,2}, Order.INWARD.order(5));
    }
    @Test void selfTestOutwardSequence() {
        assertArrayEquals(new int[]{0},         Order.OUTWARD.order(1));
        assertArrayEquals(new int[]{1,0},       Order.OUTWARD.order(2));
        assertArrayEquals(new int[]{1,0,2},     Order.OUTWARD.order(3));
        assertArrayEquals(new int[]{2,1,3,0},   Order.OUTWARD.order(4));
        assertArrayEquals(new int[]{2,1,3,0,4}, Order.OUTWARD.order(5));
    }

    enum RowGenerator {
        STR,
        NUM,
        STR_ZERO,
        EQ_STR_STR,
        EQ_NUM_IRI_ZERO,
        EQ_STR_NUM,
        EQ_NUM_BIG_STR;

        private short cols() {
            return (short)switch (this) {
                case STR, NUM -> 1;
                case STR_ZERO, EQ_STR_STR, EQ_STR_NUM,EQ_NUM_BIG_STR -> 2;
                case EQ_NUM_IRI_ZERO -> 3;
            };
        }

        private Orphan<SortProjection.OfByte> projection(boolean descending) {
            var b = SortProjection.ofByte(cols());
            if (descending) {
                return switch (this) {
                    case STR,NUM                    -> b.des(0).build();
                    case STR_ZERO, EQ_STR_NUM       -> b.des(0).des(1).build();
                    case EQ_NUM_IRI_ZERO            -> b.des(1).des(0).build();
                    case EQ_NUM_BIG_STR,EQ_STR_STR  -> b.des(1).build();
                };
            } else {
                return switch (this) {
                    case STR,NUM                    -> b.asc(0).build();
                    case STR_ZERO, EQ_STR_NUM       -> b.asc(0).asc(1).build();
                    case EQ_NUM_IRI_ZERO            -> b.asc(1).asc(0).build();
                    case EQ_NUM_BIG_STR,EQ_STR_STR  -> b.asc(1).build();
                };
            }
        }

        private MutableRope writeZeroPadded(int value, MutableRope t) {
            int logFloor = (int)Math.floor(value == 0 ? 0 : Math.log10(value));
            for (int i = 0, n = 4-1-logFloor; i < n; i++)
                t.append('0');
            return t.append(value);
        }
        private void addRow(Batch<?> b, int value, MutableRope t) {
            t.clear();
            switch (this) {
                case STR -> {
                    writeZeroPadded(value, t.append('"')).append('"');
                    b.putTerm(0, EMPTY, t.segment, t.utf8, 0, t.len, WHOLE_LIT);
                }
                case NUM -> {
                    t.append('"').append(value);
                    b.putTerm(0, DT_integer, t.segment, t.utf8, 0, t.len, SUFF_LIT);
                }
                case STR_ZERO -> {
                    writeZeroPadded(value, t.append('"')).append('"');
                    b.putTerm(0, EMPTY, t.segment, t.utf8, 0, t.len, WHOLE_LIT);
                    b.putTerm(1, DT_integer, ZERO_LOCAL.segment, ZERO_LOCAL.utf8,
                              ZERO_LOCAL.offset, ZERO_LOCAL.len, SUFF_LIT);
                }
                case EQ_STR_STR -> {
                    writeZeroPadded(value, t.append('"')).append('"');
                    b.putTerm(0, EMPTY, FIVE.segment, FIVE.utf8,
                                     FIVE.offset, FIVE.len, WHOLE_LIT);
                    b.putTerm(1, EMPTY, t.segment, t.utf8, 0, t.len, WHOLE_LIT);
                }
                case EQ_STR_NUM -> {
                    t.append('"').append(value);
                    b.putTerm(0, EMPTY, NINE.segment, NINE.utf8,
                                     NINE.offset, NINE.len, WHOLE_LIT);
                    b.putTerm(1, DT_integer, t.segment, t.utf8, 0, t.len, SUFF_LIT);
                }
                case EQ_NUM_IRI_ZERO -> {
                    writeZeroPadded(value, t).append('>');
                    b.putTerm(0, DT_integer, SEVEN_LOCAL.segment, SEVEN_LOCAL.utf8,
                                     SEVEN_LOCAL.offset, SEVEN_LOCAL.len, SUFF_LIT);
                    b.putTerm(1, PREFIX, t.segment, t.utf8,
                                     0, t.len, PREF_IRI_OR_BLANK);
                    b.putTerm(2, DT_integer, ZERO_LOCAL.segment, ZERO_LOCAL.utf8,
                                     ZERO_LOCAL.offset, ZERO_LOCAL.len, SUFF_LIT);
                }
                case EQ_NUM_BIG_STR -> {
                    t.append('"');
                    for (int i = 0; i < 1024; i++) t.append('a');
                    writeZeroPadded(value, t).append('"');
                    b.putTerm(0, DT_integer, ZERO_LOCAL.segment, ZERO_LOCAL.utf8,
                                     ZERO_LOCAL.offset, ZERO_LOCAL.len, SUFF_LIT);
                    b.putTerm(1, EMPTY, t.segment, t.utf8, 0, t.len, WHOLE_LIT);
                }
                default -> throw new UnsupportedOperationException("missing branch");
            }
        }
        private static final FinalSegmentRope PREFIX = FinalSegmentRope.asFinal("<http://example.org/");
        private static final FinalSegmentRope ZERO_LOCAL  = FinalSegmentRope.asFinal("\"0");
        private static final FinalSegmentRope SEVEN_LOCAL = FinalSegmentRope.asFinal("\"7");
        private static final FinalSegmentRope FIVE = FinalSegmentRope.asFinal("\"5\"");
        private static final FinalSegmentRope NINE = FinalSegmentRope.asFinal("\"9\"");

        @SuppressWarnings("unchecked")
        public <B extends Batch<B>> Orphan<B> generate(BatchType<B> bt, Order order, int rows) {
            var tmp = new MutableRope(16);
            BatchType<?> fillBT = HDT.equals(bt) ? COMPRESSED : bt;
            Batch<?> fill = fillBT.create(cols()).takeOwnership(GENERATOR);
            for (int value : order.order(rows)) {
                fill.beginPut();
                addRow(fill, value, tmp);
                fill.commitPut();
            }
            B b;
            if (fillBT == bt) {
                b = (B)fill;
            } else {
                var hdt = HDT.create(fill.cols).takeOwnership(GENERATOR);
                hdt.putConverting(fill, hdtDictId);
                fill.recycle(GENERATOR);
                b = (B)hdt;
            }
            return b.releaseOwnership(GENERATOR);
        }
        private static final StaticMethodOwner GENERATOR = new StaticMethodOwner("RowGenerator.generate()");
    }

    static Stream<Arguments> selfTestGenerator() {
        record D(RowGenerator gen, BatchType<?> bt) {}
        List<D> list = new ArrayList<>();
        for (var gen : RowGenerator.values()) {
            for (var bt : BATCH_TYPES)
                list.add(new D(gen, bt));
        }
        return list.stream().map(d -> arguments(d.gen, d.bt));
    }

    @ParameterizedTest @MethodSource
    <B extends Batch<B>> void selfTestGenerator(RowGenerator gen, BatchType<B> bt) {
        var ex = gen.generate(TERM, Order.INCREASING, 1).takeOwnership(this);
        B ac   = gen.generate(bt, Order.INCREASING, 1).takeOwnership(this);
        assertEquals(1, ac.rows);
        assertEquals(ex.cols, ac.cols);
        for (int c = 0; c < ex.cols; c++)
            assertEquals(ex.get(0, c), ac.get(0, c));
    }

    public static Stream<Arguments> test() {
        record D(BatchType<?> bt, RowGenerator gen, Order order, int rows) {}
        List<D> list = new ArrayList<>();
        for (RowGenerator gen : RowGenerator.values()) {
            for (BatchType<?> bt : BATCH_TYPES) {
                list.add(new D(bt, gen, Order.INCREASING, 1));
                list.add(new D(bt, gen, Order.INCREASING, 2));
                list.add(new D(bt, gen, Order.DECREASING, 2));
                for (Order o : Order.VALUES)
                    list.add(new D(bt, gen, o, 17));

            }
        }
        for (BatchType<?> bt : CRITICAL_BATCH_TYPES) {
            for (Order order : Order.VALUES)
                list.add(new D(bt, RowGenerator.STR, order, MAX_ROWS));
        }
        return list.stream().map(d -> arguments(d.bt, d.gen, d.order, d.rows));
    }

    @ParameterizedTest @MethodSource
    <B extends Batch<B>> void test(BatchType<B> bt, RowGenerator gen, Order order, int uniqueRows) {
        var values = gen.generate(bt, order, uniqueRows).takeOwnership(this);
        var tmp    = bt.create(gen.cols()).takeOwnership(this);
        var dedup  = BTreeDedup.create(bt, gen.cols()).takeOwnership(this);
        try {
            for (B node = values; node != null; node = node.next) {
                for (int r = 0, rows = node.rows; r < rows; r++) {
                    if (dedup.contains(node, r))
                        fail("dedup.contains(node, "+r+") == true, expected false");
                    if (r == 0 || r == 3 || r == 23) {
                        tmp.clear();
                        tmp.putRow(node, r);
                        assertFalse(dedup.isDuplicate(tmp, 0, 0));
                    } else {
                        if (dedup.isDuplicate(node, r, 0))
                            fail("dedup.isDuplicate(node, "+r+", 0) == true, expected false");
                    }
                    if (!dedup.contains(node, r))
                        fail("dedup.contains(node, "+r+") == false, expected true");
                    if (!dedup.isDuplicate(node, r, 0))
                        fail("dedup.isDuplicate(node, "+r+", 0) == false, expected true");
                    for (B prev = values; prev != null && prev != node; prev = prev.next) {
                        for (int prevR = 0, prevRows = prev.rows; prevR < prevRows; prevR++)
                            assertTrue(dedup.isDuplicate(prev, prevR, 0));
                    }
                    for (int prev = 0; prev < r; prev++) {
                        if (!dedup.isDuplicate(node, prev, 0))
                            fail("dedup.isDuplicate(node, "+prev+", 0) == false, expected true");
                    }
                }
            }
        } finally {
            Batch.safeRecycle(values, this);
            Batch.safeRecycle(tmp, this);
            Owned.safeRecycle(dedup, this);
        }
    }

    @ParameterizedTest @MethodSource("test")
    <B extends Batch<B>> void testSort(BatchType<B> bt, RowGenerator gen,
                                       Order order, int uniqueRows) {
        var sortDesc  = order == Order.INCREASING;
        var sortOrder = sortDesc ? Order.DECREASING : Order.INCREASING;
        B values       = gen.generate(bt, order,            uniqueRows).takeOwnership(this);
        B uniqueSorted = gen.generate(bt, sortOrder, uniqueRows).takeOwnership(this);
        B observed     = bt.create(values.cols).takeOwnership(this);
        var dedup      = BTreeDedup.create(bt, values.cols,
                                           gen.projection(sortDesc)).takeOwnership(this);
        try {
            for (int instances = 1; instances <= 2; instances++) {
                dedup.sort(values);
                observed.clear();
                dedup.forEach(observed::copy);
                if (instances == 1)
                    assertEquals(uniqueSorted, observed);
                int uniqueRow = 0;
                for (var ex = uniqueSorted; ex != null; ex = ex.next) {
                    for (short r = 0, rows = ex.rows; r < rows; r++, uniqueRow++) {
                        for (int i = 0; i < instances; i++) {
                            if (!ex.linkedEquals(r, observed, uniqueRow*instances + i))
                                fail("Mismatch at uniqueRow="+uniqueRow+", instances="+instances);
                        }
                    }
                }
            }
        } finally {
            Batch.safeRecycle(values,       this);
            Batch.safeRecycle(uniqueSorted, this);
            Batch.safeRecycle(observed,     this);
            Owned.safeRecycle(dedup,        this);
        }
    }
}