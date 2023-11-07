package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.client.util.TestTaskSet;
import com.github.alexishuf.fastersparql.model.RopeArrayMap;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.ByteRope;
import com.github.alexishuf.fastersparql.model.rope.Rope;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import com.github.alexishuf.fastersparql.model.rope.TwoSegmentRope;
import com.github.alexishuf.fastersparql.sparql.PrefixAssigner;
import com.github.alexishuf.fastersparql.sparql.binding.BatchBinding;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import com.github.alexishuf.fastersparql.sparql.expr.TermParser;
import com.github.alexishuf.fastersparql.store.batch.IdTranslator;
import com.github.alexishuf.fastersparql.store.batch.StoreBatch;
import com.github.alexishuf.fastersparql.store.batch.StoreBatchType;
import com.github.alexishuf.fastersparql.store.index.dict.*;
import com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal;
import jdk.incubator.vector.ByteVector;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.model.rope.ByteRope.EMPTY;
import static com.github.alexishuf.fastersparql.model.rope.SharedRopes.DT_integer;
import static com.github.alexishuf.fastersparql.model.rope.SharedRopes.SHARED_ROPES;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.stream.IntStream.range;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class BatchTest {
    private static final List<BatchType<?>> TYPES = List.of(
            TermBatchType.TERM,
            CompressedBatchType.COMPRESSED,
            StoreBatchType.STORE
    );
    private static LocalityCompositeDict storeDict;
    private static int storeDictId;

    static final class Size {
        private static final int ALIGNMENT = ByteVector.SPECIES_PREFERRED.length();
        private static final SegmentRope P_EX = SHARED_ROPES.internPrefix("<http://www.example.org/ns#");
        public final int rows;
        public final int cols;
        private final int requiredBytesUnaligned;
        private final int requiredBytesAligned;
        private final Term[][] terms;

        Size(int rows, int cols) {
            this.rows = rows;
            this.cols = cols;
            this.terms = new Term[rows][cols];
            int unaligned = 0, aligned = 0;
            for (int r = 0; r < rows; r++) {
                for (int c = 0; c < cols; c++) {
                    this.terms[r][c] = switch (c % 4) {
                        case 0 -> Term.valueOf("\"R"+r+"C"+c+"\"");
                        case 1 -> Term.prefixed(P_EX, ("R"+r+"C"+c+">"));
                        case 2 -> Term.typed("\""+(r*cols + c), DT_integer);
                        case 3 -> null;
                        default -> throw new IllegalArgumentException();
                    };
                    if (terms[r][c] != null) {
                        unaligned += terms[r][c].local().len;
                        aligned   += terms[r][c].local().len;
                    }
                }
                int floor = aligned & -ALIGNMENT;
                if (floor != aligned)
                    aligned = floor + ALIGNMENT;
            }
            this.requiredBytesUnaligned = unaligned;
            this.requiredBytesAligned   = aligned;
        }

        public int requiredBytes(BatchType<?> type) {
            return CompressedBatchType.COMPRESSED.equals(type) ? requiredBytesAligned
                                                             : requiredBytesUnaligned;
        }

        public <B extends Batch<B>> B fill(B batch) {
            for (int r = 0; r < rows; r++) {
                batch.beginPut();
                for (int c = 0; c < cols; c++) batch.putTerm(c, terms[r][c]);
                batch.commitPut();
            }
            return batch;
        }

        public <B extends Batch<B>> B reverseFill(B batch) {
            for (int r = 0; r < rows; r++) {
                batch.beginPut();
                for (int c = 0; c < cols; c++) batch.putTerm(c, terms[rows-1-r][cols-1-c]);
                batch.commitPut();
            }
            return batch;
        }

        @Override public boolean equals(Object obj) { return obj instanceof Size s && s.rows == rows && s.cols == cols; }
        @Override public int hashCode() { return Objects.hash(rows, cols); }
        @Override public String toString() { return "("+rows+", "+cols+")"; }
    }

    public static final List<Size> SIZES = List.of(
            new Size(0, 0),

            // 1 column,
            new Size(1, 1),
            new Size(2, 1),
            new Size(7, 1),
            new Size(23, 1),

            // 1 row
            new Size(1, 1),
            new Size(1, 2),
            new Size(1, 3),
            new Size(1, 4),
            new Size(1, 5),

            // >1 row & >1 col
            new Size(2, 3),
            new Size(3, 4),
            new Size(17, 4),
            new Size(7, 4),

            // big batches
            new Size(23, 7),
            new Size(256, 1),
            new Size(256, 8),
            new Size(128, 12)
    );

    @BeforeAll static void beforeAll() throws IOException  {
        Path tmp = Files.createTempDirectory("fastersparql-BatchTest");
        try (var b = new CompositeDictBuilder(tmp, tmp, Splitter.Mode.LAST, true)) {
            visitStrings(b);
            var second = b.nextPass();
            visitStrings(second);
            second.write();
        }
        storeDict = (LocalityCompositeDict)Dict.load(tmp.resolve("strings"));
        StoreBatch.TEST_DICT = storeDictId = IdTranslator.register(storeDict);
    }

    @AfterAll static void afterAll() {
        IdTranslator.deregister(storeDictId, storeDict);
        StoreBatch.TEST_DICT = storeDictId = 0;
        storeDict = null;
    }

    private static void visitStrings(NTVisitor visitor) {
        for (Size s : SIZES) {
            for (int r = 0; r < s.rows; r++) {
                for (int c = 0; c < s.cols; c++) {
                    Term term = s.terms[r][c];
                    if (term != null)
                        visitor.visit(SegmentRope.of(term));
                }
            }
        }
        visitor.visit(SegmentRope.of("\"23\"^^<http://www.w3.org/2001/XMLSchema#integer>"));
        visitor.visit(SegmentRope.of("\"bob\"@en"));
        visitor.visit(SegmentRope.of("\"bob\"@en-US"));
        visitor.visit(SegmentRope.of("\"bob\""));
        visitor.visit(SegmentRope.of("<http://www.w3.org/2001/XMLSchema#string>"));
        visitor.visit(SegmentRope.of("\"alice\""));
        visitor.visit(SegmentRope.of("\"\""));
        visitor.visit(SegmentRope.of("\"\"@en"));
        visitor.visit(SegmentRope.of("\"\"@en-US"));
    }

    private interface ForEachSizeTest {
        <B extends Batch<B>> void run(BatchType<B> type, Size size, String ctx);
    }

    static Stream<Arguments> types() { return TYPES.stream().map(Arguments::arguments); }

    private void forEachSize(ForEachSizeTest test) {
        for (BatchType<?> type : TYPES) {
            for (Size size : SIZES) {
                String ctx = "type="+type+", size="+size;
                test.run(type, size, ctx);
            }
        }
    }

    @Test void testUnPooledCreate() {
        for (Size s : SIZES) {
            Consumer<Batch<?>> check = b -> {
                String ctx = b.getClass().getSimpleName()+s;
//                assertTrue(b.hasCapacity(s.rows, s.cols), ctx);
                if (s.rows == 0) return;
                assertTrue(b.rowsCapacity() >= s.rows, ctx);
            };
            check.accept(new TermBatch(new Term[s.rows*s.cols], s.rows, s.cols, true));
        }
    }

    @Test void testCreate() {
        forEachSize(new ForEachSizeTest() {
            @Override
            public <B extends Batch<B>> void run(BatchType<B> type, Size size, String ctx) {
                var b = type.create(size.cols);
                assertEquals(size.cols, b.cols, ctx);
                assertEquals(0, b.rows(), ctx);
                assertEquals(size.cols, b.hashCode(), ctx);
                b.reserveAddLocals(size.requiredBytes(type));
                assertEquals(size.cols, b.cols, ctx);
                assertEquals(0, b.rows(), ctx);
                assertEquals(size.cols, b.hashCode(), ctx);
                b.recycle();
            }
        });
    }

    @Test void testBatchesEquals() {
        forEachSize(new ForEachSizeTest() {
            @Override
            public <B extends Batch<B>> void run(BatchType<B> type, Size size, String ctx) {
                int reqBytes = size.requiredBytes(type);
                B b1 = size.fill(type.create(size.cols));
                B b2 = size.fill(type.create(size.cols));
                B b3 = size.fill(type.create(size.cols));
                b1.reserveAddLocals(reqBytes);
                b2.reserveAddLocals(reqBytes);
                assertBatchesEquals(b1, b2, ctx);
                assertBatchesEquals(b1, b3, ctx);
                assertBatchesEquals(b2, b3, ctx);
                for (B b : List.of(b1, b2, b3))
                    b.recycle();
            }
        });
    }

    static <B extends Batch<B>> void
    checkTerm(String ctx, B batch, int r, int c, Term t, Term tmpTerm, TwoSegmentRope expectedTSR,
              TwoSegmentRope tmpTSR, SegmentRope tmpSR, PrefixAssigner assigner) {
        if (t == null) {
            expectedTSR = null;
        } else {
            expectedTSR.wrapFirst(t.first());
            expectedTSR.wrapSecond(t.second());
        }

        assertEquals(t, batch.linkedGet(r, c), ctx);
        assertEquals(t != null, batch.linkedGetView(r, c, tmpTerm));

        assertEquals(expectedTSR, batch.linkedGetRope(r, c), ctx);
        assertEquals(expectedTSR != null, batch.linkedGetRopeView(r, c, tmpTSR), ctx);

        assertEquals(t != null, batch.linkedLocalView(r, c, tmpSR));

        if (t != null) {
            assertEquals(t, tmpTerm, ctx);
            assertEquals(expectedTSR, tmpTSR, ctx);
            assertEquals(t.local(), tmpSR);
        }

        assertEquals(t == null ? EMPTY : t.shared(), batch.linkedShared(r, c), ctx);
        if (t == null) {
            assertFalse(batch.linkedSharedSuffixed(r, c), ctx);
        } else if (t.sharedSuffixed() && t.shared().len <= 7 && batch.linkedShared(r, c).len == 0) {
            // for short lit suffixes (i.e., no suffix or lang tags), if th batch reports
            // no shared, tolerate both true and false for sharedSuffix()
            batch.linkedSharedSuffixed(r, c); // simply check if it throws
        } else {
            assertEquals(t.sharedSuffixed(), batch.linkedSharedSuffixed(r, c), ctx);
        }
        assertEquals(t == null ? 0 : t.len, batch.linkedLen(r, c), ctx);
        assertEquals(t == null ? null : t.type(), batch.linkedTermType(r, c), ctx);
        assertEquals(t == null ? 0 : Math.max(0, t.endLex()), batch.linkedLexEnd(r, c), ctx);
        assertEquals(t == null ? 0 : t.local().len, batch.linkedLocalLen(r, c), ctx);
        assertEquals(t == null ? null : t.asDatatypeSuff(), batch.linkedAsDatatypeSuff(r, c), ctx);
        assertEquals(t == null ? null : t.datatypeTerm(), batch.linkedDatatypeTerm(r, c), ctx);
        assertEquals(t == null ? Rope.FNV_BASIS : t.hashCode(), batch.linkedHash(r, c), ctx);

        ByteRope ex = new ByteRope(), ac = new ByteRope();
        if (t != null) t.toSparql(ex, assigner);
        int sparqlBytes = batch.linkedWriteSparql(ac, r, c, assigner);
        assertEquals(ex, ac, ctx);
        assertEquals(ac.len, sparqlBytes);

        ex.clear().append(t == null ? EMPTY : t);
        batch.linkedWriteNT(ac.clear(), r, c);
        assertEquals(ex, ac, ctx);
    }

    @SuppressWarnings("SimplifiableAssertion")
    static <B extends Batch<B>> void assertBatchesEquals(B expected, B batch, String outerCtx) {
        expected.requireUnpooled();
        batch.requireUnpooled();
        expected.validate();
        batch.validate();
        Term tmpTerm = Term.pooledMutable();
        TwoSegmentRope expectedTSR = new TwoSegmentRope(), tmpTSR = TwoSegmentRope.pooled();
        SegmentRope tmpSR = SegmentRope.pooled();
        var assigner = new PrefixAssigner(new RopeArrayMap());
        assertEquals(expected.totalRows(), batch.totalRows(), outerCtx);
        assertEquals(expected.cols, batch.cols, outerCtx);
        for (int r = 0, rows = expected.rows, cols = expected.cols; r < rows; r++) {
            for (int c = 0; c < cols; c++) {
                String ctx = ", r=" + r + ", c=" + c+", "+outerCtx;
                assertTrue(batch.linkedEquals(r, c, expected, r, c), ctx);
                assertEquals(expected.linkedHash(r, c), batch.linkedHash(r, c), ctx);
                checkTerm(ctx, batch, r, c, expected.get(r, c),
                          tmpTerm, expectedTSR, tmpTSR, tmpSR, assigner);
            }
            assertTrue(batch.linkedEquals(r, expected, r), "r="+r+", "+outerCtx);
            assertEquals(expected.linkedHash(r), batch.linkedHash(r), "r="+r+", "+outerCtx);
        }
        assertTrue(expected.equals(batch), outerCtx);
        assertTrue(batch.equals(expected), outerCtx);
        assertEquals(expected.hashCode(), batch.hashCode());
        tmpTerm.recycle();
        tmpSR.recycle();
    }

    static <B extends Batch<B>> void assertBatchesEquals(Size size,
                                                         B batch, String outerCtx) {
        batch.requireUnpooled();
        batch.validate();
        Term tmpTerm = Term.pooledMutable();
        TwoSegmentRope expectedTSR = new TwoSegmentRope(), tmpTSR = new TwoSegmentRope();
        SegmentRope tmpSR = SegmentRope.pooled();
        PrefixAssigner assigner = new PrefixAssigner(new RopeArrayMap());
        for (int r = 0, rows = size.rows, cols = size.cols; r < rows; r++) {
            for (int c = 0; c < cols; c++) {
                Term t = size.terms[r][c];
                String ctx = "r=" + r + ", c=" + c+",t="+t+", "+outerCtx;
                assertTrue(batch.linkedEquals(r, c, t), ctx);

                checkTerm(ctx, batch, r, c, t, tmpTerm, expectedTSR, tmpTSR, tmpSR, assigner);
            }
            assertTrue(batch.linkedEquals(r, size.terms[r]), "r="+r+", "+outerCtx);
        }
        tmpTerm.recycle();
        tmpSR.recycle();
    }

    @Test void testPut() {
        forEachSize(new ForEachSizeTest() {
            @Override
            public <B extends Batch<B>> void run(BatchType<B> type, Size size, String ctx) {
                ThreadJournal.resetJournals();
                int reqBytes = size.requiredBytes(type);
                B b1  = type.create(size.cols);
                B b2  = type.create(size.cols);
                B b3  = type.create(size.cols);
                B b6  = type.create(size.cols);
                B b6_ = type.create(size.cols);
                B b4 = size.reverseFill(type.create(size.cols));
                B b5 = size.reverseFill(type.create(size.cols));
                B b7 = size.reverseFill(type.create(size.cols));
                B b8 = type.create(size.cols);
                B b9 = type.create(size.cols);
                B bA = type.create(size.cols);
                b4 = b4.clear(size.cols*2);
                b5 = b5.clear(size.cols*2);
                b4 = b4.clear(size.cols);
                b5 = b5.clear(size.cols);
                b7 = b7.clear(size.cols);
                TermBatch bT = TermBatchType.TERM.create(size.cols);
                bT.reserveAddLocals(reqBytes);
                size.fill(bT);
                TermParser termParser = new TermParser();
                for (int r = 0; r < size.rows; r++) {
                    b1.beginPut();
                    b2.beginPut();
                    b4.beginPut();
                    b5.beginPut();
                    b7.beginPut();
                    b8.beginPut();
                    b9.beginPut();
                    for (int c = 0; c < size.cols; c++) {
                        Term t = size.terms[r][c];
                        b1.putTerm(c, t);
                        b4.putTerm(c, t);
                        byte[] u8 = ("(" + (t == null ? "" : t.local()) + ")").getBytes(UTF_8);
                        SegmentRope sh = t == null ? null : t.shared();
                        boolean sharedSuffixed = t != null && t.sharedSuffixed();
                        MemorySegment u8Seg = MemorySegment.ofArray(u8);
                        b7.putTerm(c, sh, u8, 1, u8.length-2, sharedSuffixed);
                        b8.putTerm(c, sh, u8Seg, 1, u8.length-2, sharedSuffixed);
                    }
                    b1.commitPut();
                    for (int c = size.cols-1; c >= 0; c--) {
                        Term t = size.terms[r][c];
                        b2.linkedPutTerm(c, b1, r, c);
                        b5.putTerm(c, t);
                        if (t != null) {
                            ByteRope in = new ByteRope(t.len + 2).append('(').append(t).append(')');
                            assertTrue(termParser.parse(in, 1, in.len - 1).isValid());
                            b9.putTerm(c, termParser);
                        }
                    }
                    b2.commitPut();
                    b4.commitPut();
                    b5.commitPut();
                    b7.commitPut();
                    b8.commitPut();
                    b9.commitPut();
                }
                b3.copy(b1);
                if (size.rows > 0)
                    b6.putRow(b1, 0);
                for (int r = 1; r < size.rows; r++)
                    b6_.putRow(size.terms[r]);
                b6.copy(b6_);
                bA.putConverting(bT);
                assertBatchesEquals(size, b1, ctx);
                assertBatchesEquals(size, b2, ctx);
                assertBatchesEquals(size, b3, ctx);
                assertBatchesEquals(size, b4, ctx);
                assertBatchesEquals(size, b5, ctx);
                assertBatchesEquals(size, b6, ctx);
                assertBatchesEquals(size, b7, ctx);
                assertBatchesEquals(size, b8, ctx);
                assertBatchesEquals(size, b9, ctx);
                assertBatchesEquals(size, bA, ctx);
                assertBatchesEquals(b1, b2, ctx);
                assertBatchesEquals(b2, b3, ctx);
                assertBatchesEquals(b3, b4, ctx);
                assertBatchesEquals(b4, b5, ctx);
                assertBatchesEquals(b5, b6, ctx);
                assertBatchesEquals(b6, b7, ctx);
                assertBatchesEquals(b7, b8, ctx);
                assertBatchesEquals(b8, b9, ctx);
                assertBatchesEquals(b9, bA, ctx);

                for (B b : List.of(b1, b2, b3, b4, b5, b6, b7, b8, b9))
                    b.recycle();
            }
        });
    }

    @Test void testAppend() { doTestAppend(false); }

    private static void checkPooledOrAppended(boolean concurrent, Batch<?> left, Batch<?> right) {
        if (concurrent) return;
        for (var rn = right; rn != null; rn = rn.next) {
            for (var ln = left; ln != null; ln = ln.next) {
                if (ln == rn) return;
            }
            rn.requirePooled();
        }
    }


    @Test void testMergeThinLeft() {
        forEachSize(new ForEachSizeTest() {
            @Override public <B extends Batch<B>> void run(BatchType<B> type, Size size, String ctx) {
                B full = size.fill(type.create(size.cols));
                B left = type.create(0);
                left.rows = 2;
                var merger = type.merger(mkVars(size.cols), Vars.EMPTY, mkVars(size.cols));
                B merged = merger.merge(null, left, 1, full);
                assertBatchesEquals(size, full, ctx);
                assertBatchesEquals(size, merged, ctx);
                for (B b : List.of(requireNonNull(full), left, merged))
                    type.recycle(b);
            }
        });
    }

    @Test void testMergeThinRight() {
        forEachSize(new ForEachSizeTest() {
            @Override public <B extends Batch<B>> void run(BatchType<B> type, Size size, String ctx) {
                B full = size.fill(type.create(size.cols));
                B tmp = type.create(size.cols);
                B expected = type.create(size.cols);
                B right = type.create(0);
                right.rows = 2;
                var merger = type.merger(mkVars(size.cols), mkVars(size.cols), Vars.EMPTY);
                for (var node = full; node != null; node = node.next) {
                    for (int r = 0; r < node.rows; r++) {
                        expected.clear();
                        expected.putRow(node, r);
                        expected.putRow(node, r);
                        B merged = merger.merge(tmp.clear(size.cols), node, r, right);
                        assertSame(tmp, merged);
                        assertBatchesEquals(expected, merged, ctx);
                    }
                }
                for (B b : List.of(requireNonNull(full), tmp, expected, right))
                    type.recycle(b);
            }
        });
    }

    @Test void testMerge() {
        forEachSize(new ForEachSizeTest() {
            @Override public <B extends Batch<B>> void run(BatchType<B> type, Size size, String ctx) {
                B left = new Size(2, 3).fill(type.create(3));
                B right = size.fill(type.create(size.cols));
                B expected = type.create(3+1+size.cols);
                Vars rightVars = mkVars(size.cols);
                Vars leftVars = Vars.of("a", "b", "c");
                Vars exVars = rightVars.union(Vars.of("c", "z", "a", "b"));
                if (right.rows == 0) {
                    expected.beginPut();
                    expected.putTerm(size.cols, left, 1, 2);
                    expected.putTerm(size.cols+2, left, 1, 0);
                    expected.putTerm(size.cols+3, left, 1, 1);
                    expected.commitPut();
                }
                for (var node = right; node != null; node = node.next) {
                    for (int r = 0; r < node.rows; r++) {
                        expected.beginPut();
                        for (int c = 0; c < size.cols; c++)
                            expected.putTerm(c, node, r, c);
                        expected.putTerm(size.cols, left, 1, 2);
                        expected.putTerm(size.cols+2, left, 1, 0);
                        expected.putTerm(size.cols+3, left, 1, 1);
                        expected.commitPut();
                    }
                }
                BatchMerger<B> merger = type.merger(exVars, leftVars, rightVars);
                assertNotNull(merger);
                B actual = merger.merge(null, left, 1, right);
                assertBatchesEquals(expected, actual, ctx);

                var expected2 = expected.dup();
                expected2.copy(expected);
                B actual2 = merger.merge(actual, left, 1, right);
                assertSame(actual, actual2);
                assertBatchesEquals(expected2, actual2, ctx);

                for (B b : List.of(expected, left, requireNonNull(right), actual, expected2))
                    type.recycle(b);
            }
        });
    }

    <B extends Batch<B>> void testMergeRow0(BatchType<B> type, int cols) {
        String ctx = "type="+type+", cols="+cols;
        Vars leftVars = Vars.of("l0", "l1", "l2");
        B left = new Size(2, 3).fill(type.create(3));
        B rightRoot = new Size(4, cols).fill(type.create(cols));
        int rightRow = 2;
        B right = rightRoot;
        for (; right != null && rightRow >= right.rows; right = right.next)
            rightRow -= right.rows;
        assertNotNull(right);

        var rightVars = mkVars(cols);
        Vars outVars = Vars.of("l0", "l2").union(rightVars);
        var merger = type.merger(outVars, leftVars, rightVars);

        B dst = type.create(2+cols);
        while ((dst.rows+1)*dst.cols <= dst.termsCapacity()) {
            dst.beginPut();
            dst.putTerm(0, left, 0, 0);
            dst.putTerm(1, left, 0, 2);
            for (int c = 0; c < cols; c++)
                dst.putTerm(2+c, rightRoot, 0, c);
            dst.commitPut();
        }

        B expected = dst.dup(), actual = null;
        expected.beginPut();
        expected.putTerm(0, left, 1, 0);
        expected.putTerm(1, left, 1, 2);
        for (int c = 0; c < cols; c++)
            expected.putTerm(2+c, right, rightRow, c);
        expected.commitPut();

        try {
            actual = merger.mergeRow(dst, left, 1, right, rightRow);
        } catch (Throwable t) {
            fail(t.getClass().getSimpleName()+"ctx="+ctx, t);
        }
        assertSame(dst, actual, ctx);
        assertBatchesEquals(expected, actual, ctx);

        assertNull(type.recycle(actual));
        assertNull(type.recycle(expected));
        assertNull(type.recycle(left));
        assertNull(type.recycle(right));
    }

    @Test void testMergeRow() {
        for (BatchType<?> type : TYPES) {
            for (int cols : List.of(0, 1, 2, 3, 128))
                testMergeRow0(type, cols);
        }
    }

    @Test void testCopy() {
        forEachSize(new ForEachSizeTest() {
            @Override public <B extends Batch<B>> void run(BatchType<B> type, Size size, String ctx) {
                B b = size.fill(type.create(size.cols));
                B byRow = type.create(size.cols);
                B byRow2 = type.create(size.cols);
                for (var node = b; node != null; node = node.next) {
                    for (int r = 0; r < node.rows; ++r) {
                        byRow .putRow(node, r);
                        byRow2.putRow(node, r);
                    }
                }
                for (var node = b; node != null; node = node.next) {
                    for (int r = 0; r < node.rows; ++r)
                        byRow2.putRow(node, r);
                }
                B dupAndCopy = byRow.dup();
                dupAndCopy.copy(byRow);

                assertEquals(size.rows, byRow.totalRows());
                assertEquals(size.rows*2, byRow2.totalRows());
                assertEquals(size.rows*2, dupAndCopy.totalRows());
                assertBatchesEquals(size, byRow, ctx);
                assertBatchesEquals(b, byRow, ctx);
                assertBatchesEquals(byRow2, dupAndCopy, ctx);
                for (B batch : List.of(b, byRow, byRow2, dupAndCopy))
                    batch.recycle();
            }
        });
    }

    private void doTestAppend(boolean concurrent) {
        forEachSize(new ForEachSizeTest() {
            @Override
            public <B extends Batch<B>> void run(BatchType<B> type, Size size, String ctx) {
                int reqBytes = size.requiredBytes(type);
                int halfRows = size.rows/2, halfBytes = reqBytes/2;
                B origFull   = size.fill(type.create(size.cols));
                B origFstHalf  = type.create(size.cols);
                B origSndHalf  = type.create(size.cols);
                origFstHalf.reserveAddLocals(halfBytes);
                origSndHalf.reserveAddLocals(reqBytes);
                for (int r =        0; r <  halfRows; r++) origFstHalf.linkedPutRow(origFull, r);
                for (int r = halfRows; r < size.rows; r++) origSndHalf.linkedPutRow(origFull, r);

                List<B> batches = new ArrayList<>();
                {
                    B b = type.create(size.cols), tmp = origFull.dup();
                    assertNotSame(tmp, origFull);
                    assertBatchesEquals(origFull, tmp, ctx);
                    b.append(tmp);
                    checkPooledOrAppended(concurrent, b, tmp);
                    batches.add(b);
                }
                {
                    B b = type.create(size.cols), t1 = origFstHalf.dup(), t2 = origSndHalf.dup();
                    assertNotSame(origFstHalf, t1);
                    assertNotSame(origSndHalf, t2);
                    assertBatchesEquals(origFstHalf, t1, ctx);
                    assertBatchesEquals(origSndHalf, t2, ctx);
                    batches.add(b);
                    b.append(t1);
                    checkPooledOrAppended(concurrent, b, t1);
                    b.append(t2);
                    checkPooledOrAppended(concurrent, b, t2);
                }
                {
                    B b  = type.create(size.cols), t1 = origFstHalf.dup(), t2 = origSndHalf.dup();
                    assertNotSame(t1, origFstHalf);
                    assertNotSame(t2, origSndHalf);
                    assertBatchesEquals(origFstHalf, t1, ctx);
                    assertBatchesEquals(origSndHalf, t2, ctx);
                    batches.add(b);
                    b.quickAppend(t1);
                    checkPooledOrAppended(concurrent, b, t1);
                    b.quickAppend(t2);
                    checkPooledOrAppended(concurrent, b, t2);
                }
                {
                    B b = type.create(size.cols), tmp = origSndHalf.dup();
                    batches.add(b);
                    for (int r = 0; r < halfRows; r++) b.linkedPutRow(origFull, r);
                    assertBatchesEquals(origFstHalf, b,   ctx);
                    assertBatchesEquals(origSndHalf, tmp, ctx);
                    b.append(tmp);
                    checkPooledOrAppended(concurrent, b, tmp);
                }
                {
                    B b = type.create(size.cols), tmp = origSndHalf.dup();
                    batches.add(b);
                    for (int r = 0; r < halfRows; r++) b.linkedPutRow(origFull, r);
                    assertBatchesEquals(origFstHalf, b,   ctx);
                    assertBatchesEquals(origSndHalf, tmp, ctx);
                    b.quickAppend(tmp);
                    checkPooledOrAppended(concurrent, b, tmp);
                }
                {
                    B b = type.create(size.cols);
                    batches.add(b);
                    for (int r = 0; r < size.rows; r++) {
                        B tmp = origFull.linkedDupRow(r);
                        assertTrue(origFull.linkedEquals(r, tmp, 0), ctx);
                        b.append(tmp);
                        checkPooledOrAppended(concurrent, b, tmp);
                    }
                }
                {
                    B b = type.create(size.cols);
                    batches.add(b);
                    for (int r = 0; r < size.rows; r++) {
                        B tmp = origFull.linkedDupRow(r);
                        b.quickAppend(tmp);
                        checkPooledOrAppended(concurrent, b, tmp);
                        if (r > 0)
                            assertSame(tmp, b.tail());
                    }
                }

                IdentityHashMap<B, Boolean> allNodes = new IdentityHashMap<>();
                for (B b : batches) {
                    for (var node = b; node != null; node = node.next)
                       assertNull(allNodes.put(node, Boolean.TRUE), "node is shared");
                }

                for (int i = 0, n = batches.size(); i < n; i++) {
                    B b = batches.get(i);
                    String iCtx = ctx + ", i=" + i;
                    assertBatchesEquals(origFull, b, iCtx);
                    assertBatchesEquals(size,     b, iCtx);
                    assertNull(b.recycle());
                    if (!concurrent) b.requirePooled();
                }
            }
        });
    }

    @Test
    void testConcurrentAppended() throws Exception {
        try (var tasks = TestTaskSet.platformTaskSet(getClass().getSimpleName())) {
            tasks.repeat(28, () -> doTestAppend(true));
        }
    }

    @Test void testBucket() {
        forEachSize(new ForEachSizeTest() {
            @Override
            public <B extends Batch<B>> void run(BatchType<B> type, Size size, String ctx) {
                int halfRows = size.rows/2, offRows = size.rows+1;
                var bucket0 =           type.createBucket(offRows  , size.cols);
                var bucket1 =           type.createBucket(halfRows , size.cols);
                var batch   = size.fill(type.create(size.cols));

                for (int r = 0; r < size.rows; r++)
                    bucket0.setLinked(r+1, batch, r);
                for (int r = 0; r < halfRows; r++)
                    bucket1.setLinked(r, batch, r);
                bucket1.grow(size.rows-halfRows);
                for (int r = halfRows; r < size.rows; r++)
                    bucket1.setLinked(r, batch, r);

                for (int r = 0; r < size.rows; r++) {
                    String rCtx = "r=" + r + ", ctx=" + ctx;
                    assertTrue(bucket0.equalsLinked(r+1, batch, r), rCtx);
                    assertTrue(bucket1.equalsLinked(r, batch, r), rCtx);
                }

                for (int r = 0; r < halfRows; r++)
                    bucket0.set(r, r+1);
                for (int r = halfRows; r < size.rows; r++)
                    bucket0.set(r, bucket1, r);

                for (int r = 0; r < size.rows; r++) {
                    String rCtx = "r=" + r + ", ctx=" + ctx;
                    assertTrue(bucket0.equalsLinked(r, batch, r), rCtx);
                    assertTrue(bucket1.equalsLinked(r, batch, r), rCtx);
                }

                assertBatchesEquals(size, batch, ctx); // batch was not changed
            }
        });
    }

    private static Vars mkVars(int n) {
        var vars = new Vars.Mutable(n);
        for (int i = 0; i < n; i++)
            vars.add(Rope.of("x", i));
        return vars;
    }

    static Stream<Arguments> testProject() {
        return Stream.of(
                arguments("prepend null column",
                        (BiConsumer<Term[], Batch<?>>)(row, ex) -> {
                            int c = 1;
                            for (Term term : row) ex.putTerm(c++, term);
                        },
                        (Function<Vars, Vars>)in -> {
                            var set = new Vars.Mutable(in.size() + 1);
                            set.add(SegmentRope.of("empty"));
                            set.addAll(in);
                            return set;
                        }
                ),
                arguments("interleave null column",
                        (BiConsumer<Term[], Batch<?>>)(row, ex) -> {
                            int c = 0;
                            for (Term t : row) {
                                ex.putTerm(c, t);
                                c += 2;
                            }
                        },
                        (Function<Vars, Vars>)in -> {
                            var out = new Vars.Mutable(in.size() * 2);
                            for (int i = 0; i < in.size(); i++) {
                                out.add(in.get(i));
                                out.add(Rope.of("empty", i));
                            }
                            return out;
                        }),
                arguments("leftHalf",
                        (BiConsumer<Term[], Batch<?>>)(row, ex) -> {
                            for (int i = 0; i < row.length/2; i++) ex.putTerm(i, row[i]);
                        },
                        (Function<Vars, Vars>)in -> {
                            var out = new Vars.Mutable(10);
                            for (int i = 0; i < in.size()/2; i++)
                                out.add(in.get(i));
                            return out;
                        }),
                arguments("rightHalf",
                        (BiConsumer<Term[], Batch<?>>)(row, ex) -> {
                            int c = 0;
                            for (int i = row.length/2; i < row.length; ++i) ex.putTerm(c++, row[i]);
                        },
                        (Function<Vars, Vars>)in -> {
                            var out = new Vars.Mutable(10);
                            for (int i = in.size()/2; i < in.size(); i++) out.add(in.get(i));
                            return out;
                        })
        );
    }

    @ParameterizedTest @MethodSource
    void testProject(@SuppressWarnings("unused") String name,
                     BiConsumer<Term[], Batch<?>> projectExpected,
                     Function<Vars, Vars> generateOutVars) {
        forEachSize(new ForEachSizeTest() {
            @Override
            public <B extends Batch<B>> void run(BatchType<B> type, Size size, String ctx) {
                int reqBytes = size.requiredBytes(type);
                Vars in = mkVars(size.cols), out = generateOutVars.apply(in);
                B expected = type.create(out.size());
                for (int r = 0; r < size.rows; r++) {
                    expected.beginPut();
                    projectExpected.accept(size.terms[r], expected);
                    expected.commitPut();
                }
                B full = size.fill(type.create(size.cols));
                full.reserveAddLocals(reqBytes);
                BatchMerger<B> projector = type.projector(out, in);
                if (out.equals(in)) {
                    assertNull(projector, ctx);
                    full.recycle();
                } else {
                    assertNotNull(projector, ctx);
                    B copyProjected = projector.project(null, full);
                    assertBatchesEquals(expected, copyProjected, ctx);
                    copyProjected.recycle();
                    assertBatchesEquals(size, full, ctx); // copy-projection does not change input

                    B inPlace = projector.projectInPlace(full);
                    assertBatchesEquals(expected, inPlace, ctx);
                    inPlace.recycle();
                }
                expected.recycle();
            }
        });
    }

    static Stream<Arguments> testFilter() {
        return Stream.of(
                arguments("drop-even",
                        (Function<Size, List<Integer>>)s -> range(0, s.rows).filter(i -> (i%2)==1)
                                                                            .boxed().toList(),
                        (BiConsumer<Term[], Batch<?>>)(row, ex) -> {
                            for (int c = 0; c < row.length; c++)
                                ex.putTerm(c, row[c]);
                        },
                        (Function<Vars, Vars>)in -> in
                ),
                arguments("drop-odd",
                        (Function<Size, List<Integer>>)s -> range(0, s.rows).filter(i -> (i%2)==0)
                                .boxed().toList(),
                        (BiConsumer<Term[], Batch<?>>)(row, ex) -> {
                            for (int c = 0; c < row.length; c++)
                                ex.putTerm(c, row[c]);
                        },
                        (Function<Vars, Vars>)in -> in
                ),
                arguments("drop-first-half",
                        (Function<Size, List<Integer>>)s -> range(s.rows/2, s.rows).boxed().toList(),
                        (BiConsumer<Term[], Batch<?>>)(row, ex) -> {
                            for (int c = 0; c < row.length; c++) ex.putTerm(c, row[c]);
                        },
                        (Function<Vars, Vars>)in -> in
                ),
                arguments("drop-second-half",
                        (Function<Size, List<Integer>>)s -> range(0, s.rows/2).boxed().toList(),
                        (BiConsumer<Term[], Batch<?>>)(row, ex) -> {
                            for (int c = 0; c < row.length; c++)
                                ex.putTerm(c, row[c]);
                        },
                        (Function<Vars, Vars>)in -> in
                ),

                arguments("top-left",
                        (Function<Size, List<Integer>>)s -> range(0, s.rows/2).boxed().toList(),
                        (BiConsumer<Term[], Batch<?>>)(row, ex) -> {
                            for (int i = 0; i < row.length/2; i++) ex.putTerm(i, row[i]);
                        },
                        (Function<Vars, Vars>)in -> {
                            Vars.Mutable out = new Vars.Mutable(10);
                            for (int i = 0; i < in.size() / 2; i++) out.add(in.get(i));
                            return out;
                        }
                ),
                arguments("top-right",
                        (Function<Size, List<Integer>>)s -> range(0, s.rows/2).boxed().toList(),
                        (BiConsumer<Term[], Batch<?>>)(row, ex) -> {
                            for (int i = row.length/2, c = 0; i < row.length; i++)
                                ex.putTerm(c++, row[i]);
                        },
                        (Function<Vars, Vars>)in -> {
                            Vars.Mutable out = new Vars.Mutable(10);
                            for (int i = in.size()/2; i < in.size(); i++) out.add(in.get(i));
                            return out;
                        }
                ),
                arguments("bottom-left",
                        (Function<Size, List<Integer>>)s -> range(s.rows/2, s.rows).boxed().toList(),
                        (BiConsumer<Term[], Batch<?>>)(row, ex) -> {
                            for (int i = 0; i < row.length/2; i++) ex.putTerm(i, row[i]);
                        },
                        (Function<Vars, Vars>)in -> {
                            Vars.Mutable out = new Vars.Mutable(10);
                            for (int i = 0; i < in.size() / 2; i++) out.add(in.get(i));
                            return out;
                        }
                ),
                arguments("bottom-right",
                        (Function<Size, List<Integer>>)s -> range(s.rows, s.rows/2).boxed().toList(),
                        (BiConsumer<Term[], Batch<?>>)(row, ex) -> {
                            for (int i = row.length/2, c = 0; i < row.length; i++)
                                ex.putTerm(c++, row[i]);
                        },
                        (Function<Vars, Vars>)in -> {
                            Vars.Mutable out = new Vars.Mutable(10);
                            for (int i = in.size()/2; i < in.size(); i++) out.add(in.get(i));
                            return out;
                        }
                ),

                arguments("odd-left",
                        (Function<Size, List<Integer>>)s -> range(0, s.rows).filter(i -> i%2 ==1)
                                                                            .boxed().toList(),
                        (BiConsumer<Term[], Batch<?>>)(row, ex) -> {
                            for (int i = 0; i < row.length / 2; i++) ex.putTerm(i, row[i]);
                        },
                        (Function<Vars, Vars>)in -> {
                            Vars.Mutable out = new Vars.Mutable(10);
                            for (int i = 0; i < in.size() / 2; i++)  out.add(in.get(i));
                            return out;
                        }
                ),

                arguments("even-right",
                        (Function<Size, List<Integer>>)s -> range(0, s.rows).filter(i -> i%2==0)
                                                                            .boxed().toList(),
                        (BiConsumer<Term[], Batch<?>>)(row, ex) -> {
                            for (int i = row.length/2, c = 0; i < row.length; i++)
                                ex.putTerm(c++, row[i]);
                        },
                        (Function<Vars, Vars>)in -> {
                            Vars.Mutable out = new Vars.Mutable(10);
                            for (int i = in.size()/2; i < in.size(); i++)  out.add(in.get(i));
                            return out;
                        }
                ),

                arguments("even-right-dummy",
                        (Function<Size, List<Integer>>)s -> range(0, s.rows).filter(i -> i%2==0)
                                .boxed().toList(),
                        (BiConsumer<Term[], Batch<?>>)(row, ex) -> {
                            int c = 0;
                            for (int i = row.length / 2; i < row.length; i++)
                                ex.putTerm(c++, row[i]);
                        },
                        (Function<Vars, Vars>)in -> {
                            Vars.Mutable out = new Vars.Mutable(10);
                            for (int i = in.size()/2; i < in.size(); i++)  out.add(in.get(i));
                            out.add(SegmentRope.of("dummy"));
                            return out;
                        }
                )
        );
    }

    @ParameterizedTest @MethodSource
    void testFilter(@SuppressWarnings("unused") String name,
                    Function<Size, List<Integer>> survivorsGetter,
                    BiConsumer<Term[], Batch<?>> projectExpected,
                    Function<Vars, Vars> genOutVars) {
        forEachSize(new ForEachSizeTest() {
            @Override
            public <B extends Batch<B>> void run(BatchType<B> type, Size size, String ctx) {
                int reqBytes = size.requiredBytes(type);
                Vars in = mkVars(size.cols), out = genOutVars.apply(in);
                B full = size.fill(type.create(size.cols));
                full      .reserveAddLocals(reqBytes);
                B expected = type.create(out.size());
                var survivors = survivorsGetter.apply(size);
                for (int r : survivors) {
                    expected.beginPut();
                    projectExpected.accept(size.terms[r], expected);
                    expected.commitPut();
                }
                RowFilter<B> rowFilter = new RowFilter<>() {
                    private int absRow;
                    private B currentBatch;
                    private final BitSet currentVisitedRows = new BitSet();
                    @Override public Decision drop(B batch, int row) {
                        if (batch == currentBatch) {
                            assertFalse(currentVisitedRows.get(row), "row visited twice");
                        } else {
                            currentBatch = batch;
                            currentVisitedRows.clear();
                        }
                        currentVisitedRows.set(row);
                        return survivors.contains(absRow++) ? Decision.KEEP : Decision.DROP;
                    }
                    @Override public void rebind(BatchBinding binding) {}
                };
                var filter = out.equals(in) ? type.filter(out, rowFilter)
                                            : type.filter(out, in, rowFilter);
                B inPlace = filter.filterInPlace(full);
                if (inPlace != full)
                    full.requirePooled();
                assertBatchesEquals(expected, inPlace, ctx);
                inPlace.recycle();
                expected.recycle();
            }
        });
    }


    static Stream<Arguments> testWrite() {
        Term i23 = Term.valueOf("\"23\"^^<http://www.w3.org/2001/XMLSchema#integer>");
        return Stream.of(
                arguments(Term.valueOf("\"bob\"@en"), 0, 1),
                arguments(Term.valueOf("\"bob\"@en"), 0, 4),
                arguments(Term.valueOf("\"bob\"@en"), 0, 8),
                arguments(Term.valueOf("\"bob\""), 0, 5),
                arguments(Term.valueOf("\"bob\""), 1, 4),
                // <http://www.w3.org/2001/XMLSchema#string>
                arguments(Term.XSD_STRING, 0, 1),
                arguments(Term.XSD_STRING, 1, 33),
                arguments(Term.XSD_STRING, 1, 34),
                arguments(Term.XSD_STRING, 1, 35),
                arguments(Term.XSD_STRING, 1, 40),
                arguments(Term.XSD_STRING, 1, 41),
                arguments(Term.XSD_STRING, 0, 41),
                // "23"^^<http://www.w3.org/2001/XMLSchema#integer>
                arguments(i23, 0, 48),
                arguments(i23, 0, 3),
                arguments(i23, 0, 4),
                arguments(i23, 1, 3),
                arguments(i23, 1, 2),
                arguments(i23, 0, 7),
                arguments(i23, 1, 7),
                arguments(i23, 2, 7),
                arguments(i23, 3, 48),
                arguments(i23, 7, 47)
        );
    }

    private static final Term[] DUMMY_ROW = Term.array("xsd:string", "\"bob\"@en", 23);

    @ParameterizedTest @MethodSource void testWrite(Term term, int begin, int end) {
        for (BatchType<?> type : TYPES) {
            var b = type.create(3);
            b.putRow(DUMMY_ROW);
            b.putRow(new Term[]{DUMMY_ROW[2], term, DUMMY_ROW[2]});

            ByteRope dest = new ByteRope().append("@");
            b.linkedWrite(dest, 1, 1, begin, end);
            assertEquals("@"+term.toString(begin, end), dest.toString(), "type="+type);
            b.recycle();
        }
    }

    @ParameterizedTest @ValueSource(strings = {
        "\"alice\"",
        "\"\"",
        "\"bob\"@en",
        "\"\"@en",
        "\"23\"^^<http://www.w3.org/2001/XMLSchema#integer>",
        "<http://www.w3.org/2001/XMLSchema#string>",
    })
    void testLen(String termString) {
        Term term = termString.equals("null") ? null : Term.array(termString)[0];
        for (BatchType<?> type : TYPES) {
            var b = type.create( 3);
            b.putRow(DUMMY_ROW);
            b.putRow(new Term[]{DUMMY_ROW[2], term, DUMMY_ROW[2]});
            assertEquals(term == null ? 0 : term.len, b.len(1, 1));
        }
    }

    static Stream<Arguments> testLexEnd() {

        return Stream.of(
                arguments(Term.valueOf("\"bob\""), 4),
                arguments(Term.valueOf("\"bob\"@en"), 4),
                arguments(Term.valueOf("\"bob\"@en-US"), 4),
                arguments(Term.valueOf("\"\""), 1),
                arguments(Term.valueOf("\"\"@en"), 1),
                arguments(Term.valueOf("\"\"@en-US"), 1),
                arguments(Term.valueOf(null), 0),
                arguments(Term.valueOf("_:b"), 0),
                arguments(Term.valueOf("?x"), 0),
                arguments(Term.valueOf("<rel>"), 0),
                arguments(Term.valueOf("<http://www.w3.org/2001/XMLSchema#string>"), 0)
        );
    }

    @ParameterizedTest @MethodSource
    void testLexEnd(Term term, int expected) {
        for (BatchType<?> type : TYPES) {
            var b = type.create(3);
            b.putRow(DUMMY_ROW);
            b.putRow(new Term[]{DUMMY_ROW[2], term, DUMMY_ROW[2]});
            assertEquals(expected, b.lexEnd(1, 1));
            b.recycle();
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"}) @ParameterizedTest @MethodSource("types")
    void testOutOfOrderOffer(BatchType<?> type) {
        Batch<?> ex1 = type.create(3);
        Batch<?> ex2 = type.create(3);
        Batch<?> ex3 = type.create(3);
        ex1.putRow(DUMMY_ROW);
        for (int i = 0; i < 2; i++) ex2.putRow(DUMMY_ROW);
        for (int i = 0; i < 4; i++) ex3.putRow(DUMMY_ROW);

        for (var permutation : List.of(List.of(2, 1, 0), List.of(1, 2, 0), List.of(0, 2, 1))) {
            Batch<?> b1 = type.create(3);
            Batch<?> b2 = type.create(3);
            Batch<?> b3 = type.create(3);
            b1.beginPut();
            for (int c : permutation)
                b1.putTerm(c, DUMMY_ROW[c]);
            b1.commitPut();

            ((Batch)b2).putRow(b1, 0);
            ((Batch)b2).putRow(b1, 0);

            // put by term
            b3.beginPut();
            for (int c = 0; c < 3; c++)
                b3.putTerm(c, b1.get(0, c));
            b3.commitPut();
            b3.beginPut();
            // offer by term
            for (int c = 0; c < 3; c++)
                b3.putTerm(c, b1.get(0, c));
            b3.commitPut();
            // offer by term from b1
            b3.beginPut();
            for (int c = 0; c < 3; c++)
                ((Batch)b3).putTerm(c, b1, 0, c);
            b3.commitPut();
            // put by term from b1
            b3.beginPut();
            for (int c = 0; c < 3; c++)
                ((Batch)b3).putTerm(c, b1, 0, c);
            b3.commitPut();

            String ctx = "permutation="+permutation;
            assertBatchesEquals((Batch)ex1, (Batch)b1, ctx);
            assertBatchesEquals((Batch)ex2, (Batch)b2, ctx);
            assertBatchesEquals((Batch)ex3, (Batch)b3, ctx);

            for (var b : List.of(b1, b2, b3))
                b.recycle();
        }
        for (var b : List.of(ex1, ex2, ex3))
            b.recycle();
    }

    @ParameterizedTest @MethodSource("types")
    <B extends Batch<B>> void testNullRow(BatchType<B> type) {
        Size sz = new Size(4, 2);
        B n = type.create(2);
        n.putRow(new Term[]{null, null});

        B uo0 = type.create(2);
        uo0.beginPut();
        uo0.putTerm(1, sz.terms[0][1]);
        uo0.putTerm(0, sz.terms[0][0]);
        uo0.commitPut();
        uo0.putRow(n, 0);
        if (type == CompressedBatchType.COMPRESSED) assertTrue(uo0.validate());

        B uo1 = type.create(2);
        uo1.beginPut();
        uo1.putTerm(1, sz.terms[0][1]);
        uo1.putTerm(0, sz.terms[0][0]);
        uo1.commitPut();
        uo1.beginPut();
        uo1.commitPut();
        if (type == CompressedBatchType.COMPRESSED) assertTrue(uo1.validate());

        B o0 = type.create(2);
        o0.putRow(sz.terms[0]);
        o0.putRow(n, 0);
        if (type == CompressedBatchType.COMPRESSED) assertTrue(o0.validate());

        B o1 = type.create(2);
        o1.putRow(sz.terms[0]);
        o1.beginPut();
        o1.commitPut();
        if (type == CompressedBatchType.COMPRESSED) assertTrue(o1.validate());

        B expected = type.create(2);
        expected.putRow(sz.terms[0]);
        expected.beginPut();
        expected.commitPut();
        assertEquals(2, expected.totalRows());

        assertBatchesEquals(expected, uo0, "uo0");
        assertBatchesEquals(expected, uo1, "uo1");
        assertBatchesEquals(expected, o0, "o0");
        assertBatchesEquals(expected, o1, "o1");

        for (B b : List.of(n, uo0, uo1, o0, o1, expected))
            b.recycle();
    }

//    @Test void regressionHashC7() {
//        var b0 = CompressedBatchType.COMPRESSED.create(1, 8, 0);
//        var b1 = CompressedBatchType.COMPRESSED.create(1, 8, 0);
//        for (var b : List.of(b0, b1)) {
//            b.beginPut();
//            var terms = List.of(
//             /* 0 */"<http://data.semanticweb.org/person/martin-szomszor>",
//             /* 1 */"<http://data.semanticweb.org/conference/eswc/2010/main/chair/semanticwebtechnologieschair>",
//             /* 2 */"<http://data.semanticweb.org/conference/eswc/2010/paper/social_web/5>",
//             /* 3 */"<http://dbpedia.org/resource/United_Kingdom>",
//             /* 4 */"<http://dbpedia.org/resource/London>",
//             /* 5 */"\"51.5\"^^<http://www.w3.org/2001/XMLSchema#double>"
//            );
//            for (int i = 0; i < terms.size(); i++)
//                b.putTerm(i, Term.valueOf(terms.get(i)));
//        }
//        b0.putTerm(6, Term.valueOf("\"-0.116667\"^^<http://www.w3.org/2001/XMLSchema#double>"));
//        b1.putTerm(6, Term.valueOf("\"-0.11666666666666667\"^^<http://www.w3.org/2001/XMLSchema#double>"));
//        for (var b : List.of(b0, b1)) {
//            b.putTerm(7, Term.valueOf("<http://data.semanticweb.org/conference/eswc/2010/proceedings>"));
//            b.commitPut();
//        }
//        assertTrue(b0.equals(0, b1, 0));
//        assertTrue(b1.equals(0, b0, 0));
//        assertEquals(b0.hash(0), b1.hash(0));
//        for (int c = 0; c < b0.cols; c++) {
//            assertTrue(b0.equals(0, c, b1, 0, c));
//            assertTrue(b1.equals(0, c, b0, 0, c));
//            assertEquals(b0.hash(0, c), b1.hash(0, c));
//        }
//    }

    @Test void  regressionHashS6() {
        String name = "\"Michael Bartels\"";
        Term place = Term.valueOf("<http://sws.geonames.org/2911297/>");
        var ex = CompressedBatchType.COMPRESSED.create(2);
        ex.beginPut();
        ex.putTerm(0, EMPTY, name.getBytes(UTF_8), 0, name.length(), false);
        ex.putTerm(1, place);
        ex.commitPut();

        var ac = CompressedBatchType.COMPRESSED.create(2);
        ac.beginPut();
        ac.putTerm(0, EMPTY, (".."+name).getBytes(UTF_8), 2, name.length(), true);
        ac.putTerm(1, SegmentRope.of("<http://sws.geonames.org/"),
                    "2911297/>".getBytes(UTF_8), 0, 9, false);
        ac.commitPut();

        for (int c = 0; c < 2; c++) {
            assertTrue(ac.equals(0, c, ex, 0, c), "c="+c);
            assertTrue(ex.equals(0, c, ac, 0, c), "c="+c);
            assertEquals(ex.hash(0, c), ac.hash(0, c), "c="+c);
        }
        assertTrue(ac.equals(0, ex, 0));
        assertTrue(ex.equals(0, ac, 0));
        assertEquals(ex.hash(0), ac.hash(0));
        ac.recycle();
        ex.recycle();
    }

}