package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.batch.type.RowFilter.Decision;
import com.github.alexishuf.fastersparql.model.RopeArrayMap;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.ByteRope;
import com.github.alexishuf.fastersparql.model.rope.Rope;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import com.github.alexishuf.fastersparql.model.rope.TwoSegmentRope;
import com.github.alexishuf.fastersparql.sparql.PrefixAssigner;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import com.github.alexishuf.fastersparql.sparql.expr.TermParser;
import com.github.alexishuf.fastersparql.store.batch.IdTranslator;
import com.github.alexishuf.fastersparql.store.batch.StoreBatch;
import com.github.alexishuf.fastersparql.store.batch.StoreBatchType;
import com.github.alexishuf.fastersparql.store.index.dict.*;
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
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.model.rope.ByteRope.EMPTY;
import static com.github.alexishuf.fastersparql.model.rope.SharedRopes.DT_integer;
import static com.github.alexishuf.fastersparql.model.rope.SharedRopes.SHARED_ROPES;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.IntStream.range;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class BatchTest {
    private static final List<BatchType<?>> TYPES = List.of(
            TermBatchType.INSTANCE,
            CompressedBatchType.INSTANCE,
            StoreBatchType.INSTANCE
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
            return CompressedBatchType.INSTANCE.equals(type) ? requiredBytesAligned
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
            new Size(256, 8)
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
            check.accept(new TermBatch(s.rows, s.cols));
            check.accept(new CompressedBatch(s.rows, s.cols, s.requiredBytesAligned));
        }
    }

    @Test void testCreate() {
        forEachSize(new ForEachSizeTest() {
            @Override
            public <B extends Batch<B>> void run(BatchType<B> type, Size size, String ctx) {
                int reqBytes = size.requiredBytes(type);
                var b = type.create(size.rows, size.cols, reqBytes);
//                assertTrue(b.hasCapacity(size.rows, reqBytes), ctx);
                assertFalse(b.hasMoreCapacity(b), ctx);
                assertEquals(size.cols, b.cols, ctx);
                assertEquals(0, b.rows(), ctx);
                assertEquals(size.cols, b.hashCode(), ctx);
            }
        });
    }

    @Test void testBatchesEquals() {
        forEachSize(new ForEachSizeTest() {
            @Override
            public <B extends Batch<B>> void run(BatchType<B> type, Size size, String ctx) {
                int reqBytes = size.requiredBytes(type);
                B b1 = size.fill(type.create(size.rows, size.cols, reqBytes));
                B b2 = size.fill(type.create(size.rows, size.cols, reqBytes));
                B b3 = size.fill(type.create(1, size.cols, 0));
                assertBatchesEquals(b1, b2, ctx);
                assertBatchesEquals(b1, b3, ctx);
                assertBatchesEquals(b2, b3, ctx);
            }
        });
    }

    static <B extends Batch<B>> void
    checkTerm(String ctx, B batch, int r, int c, Term t, Term tmpTerm,
              TwoSegmentRope expectedTSR, TwoSegmentRope tmpTSR, PrefixAssigner assigner) {
        if (t == null) {
            expectedTSR = null;
        } else {
            expectedTSR.wrapFirst(t.first());
            expectedTSR.wrapSecond(t.second());
        }

        assertEquals(t, batch.get(r, c), ctx);
        assertEquals(t != null, batch.getView(r, c, tmpTerm));
        if (t != null) assertEquals(t, tmpTerm, ctx);

        assertEquals(expectedTSR, batch.getRope(r, c), ctx);
        assertEquals(expectedTSR != null, batch.getRopeView(r, c, tmpTSR), ctx);
        if (t != null) assertEquals(expectedTSR, tmpTSR, ctx);

        assertEquals(t == null ? EMPTY : t.shared(), batch.shared(r, c), ctx);
        if (t == null) {
            assertFalse(batch.sharedSuffixed(r, c), ctx);
        } else if (t.sharedSuffixed() && t.shared().len <= 7 && batch.shared(r, c).len == 0) {
            // for short lit suffixes (i.e., no suffix or lang tags), if th batch reports
            // no shared, tolerate both true and false for sharedSuffix()
            batch.sharedSuffixed(r, c); // simply check if it throws
        } else {
            assertEquals(t.sharedSuffixed(), batch.sharedSuffixed(r, c), ctx);
        }
        assertEquals(t == null ? 0 : t.len, batch.len(r, c), ctx);
        assertEquals(t == null ? null : t.type(), batch.termType(r, c), ctx);
        assertEquals(t == null ? 0 : Math.max(0, t.endLex()), batch.lexEnd(r, c), ctx);
        assertEquals(t == null ? 0 : t.local().len, batch.localLen(r, c), ctx);
        assertEquals(t == null ? null : t.asDatatypeSuff(), batch.asDatatypeSuff(r, c), ctx);
        assertEquals(t == null ? null : t.datatypeTerm(), batch.datatypeTerm(r, c), ctx);
        assertEquals(t == null ? Rope.FNV_BASIS : t.hashCode(), batch.hash(r, c), ctx);

        ByteRope ex = new ByteRope(), ac = new ByteRope();
        if (t != null) t.toSparql(ex, assigner);
        batch.writeSparql(ac, r, c, assigner);
        assertEquals(ex, ac, ctx);

        ex.clear().append(t == null ? EMPTY : t);
        batch.writeNT(ac.clear(), r, c);
        assertEquals(ex, ac, ctx);
    }

    @SuppressWarnings("SimplifiableAssertion")
    static <B extends Batch<B>> void assertBatchesEquals(B expected, B batch, String outerCtx) {
        Term tmpTerm = new Term();
        TwoSegmentRope expectedTSR = new TwoSegmentRope(), tmpTSR = new TwoSegmentRope();
        var assigner = new PrefixAssigner(new RopeArrayMap());
        assertEquals(expected.rows, batch.rows, outerCtx);
        assertEquals(expected.cols, batch.cols, outerCtx);
        for (int r = 0, rows = expected.rows, cols = expected.cols; r < rows; r++) {
            for (int c = 0; c < cols; c++) {
                String ctx = ", r=" + r + ", c=" + c+", "+outerCtx;
                assertTrue(batch.equals(r, c, expected, r, c), ctx);
                assertEquals(expected.hash(r, c), batch.hash(r, c), ctx);
                checkTerm(ctx, batch, r, c, expected.get(r, c),
                          tmpTerm, expectedTSR, tmpTSR, assigner);
            }
            assertTrue(batch.equals(r, expected, r), "r="+r+", "+outerCtx);
            assertEquals(expected.hash(r), batch.hash(r), "r="+r+", "+outerCtx);
        }
        assertTrue(expected.equals(batch), outerCtx);
        assertTrue(batch.equals(expected), outerCtx);
        assertEquals(expected.hashCode(), batch.hashCode());
    }

    static <B extends Batch<B>> void assertBatchesEquals(Size size,
                                                         B batch, String outerCtx) {
        Term tmpTerm = new Term();
        TwoSegmentRope expectedTSR = new TwoSegmentRope(), tmpTSR = new TwoSegmentRope();
        PrefixAssigner assigner = new PrefixAssigner(new RopeArrayMap());
        for (int r = 0, rows = size.rows, cols = size.cols; r < rows; r++) {
            for (int c = 0; c < cols; c++) {
                Term t = size.terms[r][c];
                String ctx = "r=" + r + ", c=" + c+",t="+t+", "+outerCtx;
                assertTrue(batch.equals(r, c, t), ctx);

                checkTerm(ctx, batch, r, c, t, tmpTerm, expectedTSR, tmpTSR, assigner);
            }
            assertTrue(batch.equals(r, size.terms[r]), "r="+r+", "+outerCtx);
        }
    }

    @Test void testPut() {
        forEachSize(new ForEachSizeTest() {
            @Override
            public <B extends Batch<B>> void run(BatchType<B> type, Size size, String ctx) {
                int reqBytes = size.requiredBytes(type);
                B b1  = type.create(1, size.cols, 1);
                B b2  = type.create(1, size.cols, 0);
                B b3  = type.create(0, size.cols, 0);
                B b6  = type.create(0, size.cols, 0);
                B b6_ = type.create(0, size.cols, 0);
                B b4 = size.reverseFill(type.create(size.rows, size.cols, reqBytes));
                B b5 = size.reverseFill(type.create(size.rows, size.cols, reqBytes));
                B b7 = size.reverseFill(type.create(size.rows, size.cols, reqBytes));
                B b8 = type.create(1, size.cols, 1);
                B b9 = type.create(1, size.cols, 0);
                B bA = type.create(1, size.cols, 4);
                b4.clear(size.cols*2);
                b5.clear(size.cols*2);
                b4.clear(size.cols);
                b5.clear(size.cols);
                b7.clear(size.cols);
                TermBatch bT = Batch.TERM.create(size.rows, size.cols, reqBytes);
                size.fill(bT);
                TermParser termParser = new TermParser();
                List<B> putBatches = List.of(b2, b4, b5, b7, b8, b9);
                for (int r = 0; r < size.rows; r++) {
                    b1.beginPut();
                    for (B b : putBatches) b.beginPut();
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
                        b2.putTerm(c, b1, r, c);
                        b5.putTerm(c, t);
                        if (t != null) {
                            ByteRope in = new ByteRope(t.len + 2).append('(').append(t).append(')');
                            assertTrue(termParser.parse(in, 1, in.len - 1).isValid());
                            b9.putTerm(c, termParser);
                        }
                    }
                    for (B b : putBatches) b.commitPut();
                }
                b3.put(b1);
                if (size.rows > 0)
                    b6.putRow(b1, 0);
                for (int r = 1; r < size.rows; r++)
                    b6_.putRow(size.terms[r]);
                b6.put(b6_);
                assertSame(bA, bA.putConverting(bT));
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
            }
        });
    }

    @Test void testOffer() {
        forEachSize(new ForEachSizeTest() {
            @Override
            public <B extends Batch<B>> void run(BatchType<B> type, Size size, String ctx) {
                int reqBytes = size.requiredBytes(type);
                B b1 = type.create(size.rows, size.cols, reqBytes);
                B b2 = type.create(size.rows, size.cols, reqBytes);
                B b3 = type.create(size.rows, size.cols, reqBytes);
                B b4 = size.reverseFill(type.create(size.rows, size.cols, reqBytes));
                B b5 = size.reverseFill(type.create(size.rows, size.cols, reqBytes));
                B b6 = type.create(size.rows, size.cols, reqBytes);
                B b7 = type.create(size.rows, size.cols, reqBytes);
                B b8 = type.create(size.rows, size.cols, reqBytes);
                b4.clear(size.cols*2);
                b5.clear(size.cols*2);
                b4.clear(size.cols);
                b5.clear(size.cols);

                b1.reserve(size.rows, reqBytes);
                b2.reserve(size.rows, reqBytes);
                b3.reserve(size.rows, reqBytes);
                b6.reserve(size.rows, reqBytes);
                b7.reserve(size.rows, reqBytes);
                b8.reserve(size.rows, reqBytes);

                assertTrue(b1.hasCapacity(size.rows, reqBytes));
                assertTrue(b2.hasCapacity(size.rows, reqBytes));
                assertTrue(b3.hasCapacity(size.rows, reqBytes));
                assertTrue(b4.hasCapacity(size.rows, reqBytes));
                assertTrue(b5.hasCapacity(size.rows, reqBytes));
                assertTrue(b6.hasCapacity(size.rows, reqBytes));
                assertTrue(b7.hasCapacity(size.rows, reqBytes));
                assertTrue(b8.hasCapacity(size.rows, reqBytes));
                TermParser termParser = new TermParser();
                for (int r = 0; r < size.rows; r++) {
                    assertTrue(b1.beginOffer());
                    for (int c = 0; c < size.cols; c++)
                        assertTrue(b1.offerTerm(c, size.terms[r][c]), "r=, "+r+"c="+c);
                    assertTrue(b1.commitOffer());
                    assertTrue(b2.beginOffer());
                    for (int c = 0; c < size.cols; c++)
                        assertTrue(b2.offerTerm(c, b1, r, c), "r= "+r+",c="+c);
                    assertTrue(b2.commitOffer());

                    assertTrue(b4.beginOffer());
                    for (int c = 0; c < size.cols; c++)
                        assertTrue(b4.offerTerm(c, size.terms[r][c]), "r=, "+r+"c="+c);
                    assertTrue(b4.commitOffer());
                    assertTrue(b5.beginOffer());
                    for (int c = 0; c < size.cols; c++)
                        assertTrue(b5.offerTerm(c, b1, r, c), "r= "+r+",c="+c);
                    assertTrue(b5.commitOffer());

                    b6.beginOffer();
                    b7.beginOffer();
                    b8.beginOffer();
                    for (int c = 0; c < size.cols; c++) {
                        Term t = size.terms[r][c];
                        if (t == null) continue;
                        byte[] u8 = ("("+t.local()+")").getBytes(UTF_8);
                        MemorySegment u8Seg = MemorySegment.ofArray(u8);
                        ByteRope rope = new ByteRope(t);
                        assertTrue(termParser.parse(rope, 0, rope.len).isValid());
                        assertTrue(b6.offerTerm(c,  t.shared(), u8, 1, u8.length-2, t.sharedSuffixed()));
                        assertTrue(b7.offerTerm(c, t.shared(), u8Seg, 1, u8.length-2, t.sharedSuffixed()));
                        assertTrue(b8.offerTerm(c, termParser));
                        Arrays.fill(u8, (byte)'#');
                    }
                    b6.commitOffer();
                    b7.commitOffer();
                    b8.commitOffer();
                }
                assertTrue(b3.offer(b1), ctx);
                assertBatchesEquals(size, b1, ctx);
                assertBatchesEquals(size, b2, ctx);
                assertBatchesEquals(size, b3, ctx);
                assertBatchesEquals(size, b4, ctx);
                assertBatchesEquals(size, b5, ctx);
                assertBatchesEquals(b1, b2, ctx);
                assertBatchesEquals(b2, b3, ctx);
                assertBatchesEquals(b3, b4, ctx);
                assertBatchesEquals(b4, b5, ctx);
                assertBatchesEquals(b5, b6, ctx);
                assertBatchesEquals(b6, b7, ctx);
                assertBatchesEquals(b7, b8, ctx);
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
                            set.add(Rope.of("empty"));
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
                B expected = type.create(0, out.size(), 0);
                for (int r = 0; r < size.rows; r++) {
                    expected.beginPut();
                    projectExpected.accept(size.terms[r], expected);
                    expected.commitPut();
                }
                B full = size.fill(type.create(size.rows, size.cols, reqBytes));
                BatchMerger<B> projector = type.projector(out, in);
                if (out.equals(in)) {
                    assertNull(projector, ctx);
                } else {
                    assertNotNull(projector, ctx);
                    B copyProjected = projector.project(null, full);
                    assertBatchesEquals(expected, copyProjected, ctx);
                    assertBatchesEquals(size, full, ctx);
                    B inPlace = projector.projectInPlace(full);
                    assertBatchesEquals(expected, inPlace, ctx);
                    if (inPlace != full)
                        assertBatchesEquals(size, full, ctx);
                }
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
                            out.add(Rope.of("dummy"));
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
                B full = size.fill(type.create(size.rows, size.cols, reqBytes));
                B fullBackup = size.fill(type.create(size.rows, size.cols, reqBytes));
                B expected = type.create(0, out.size(), 0);
                var survivors = survivorsGetter.apply(size);
                for (int r : survivors) {
                    expected.beginPut();
                    projectExpected.accept(size.terms[r], expected);
                    expected.commitPut();
                }
                RowFilter<B> rowFilter = (batch, row)
                        -> survivors.contains(row) ? Decision.KEEP : Decision.DROP;
                var filter = out.equals(in) ? type.filter(out, rowFilter)
                                            : type.filter(out, in, rowFilter);
                B copyFilter = filter.filter(null, full);
                assertBatchesEquals(expected, copyFilter, ctx);
                assertBatchesEquals(fullBackup, full, ctx);
                B inPlace = filter.filterInPlace(full);
                if (inPlace != full)
                    assertBatchesEquals(fullBackup, full, ctx);
                assertBatchesEquals(inPlace, expected, ctx);
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
            var b = type.create(2, 3, 0);
            b.putRow(DUMMY_ROW);
            b.putRow(new Term[]{DUMMY_ROW[2], term, DUMMY_ROW[2]});

            ByteRope dest = new ByteRope().append("@");
            b.write(dest, 1, 1, begin, end);
            assertEquals("@"+term.toString(begin, end), dest.toString(), "type="+type);
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
            var b = type.create(2, 3, 0);
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
            var b = type.create(2, 3, 0);
            b.putRow(DUMMY_ROW);
            b.putRow(new Term[]{DUMMY_ROW[2], term, DUMMY_ROW[2]});
            assertEquals(expected, b.lexEnd(1, 1));
        }
    }

    private static final int B_SP_LEN = ByteVector.SPECIES_PREFERRED.length();

    @SuppressWarnings({"unchecked", "rawtypes"}) @ParameterizedTest @MethodSource("types")
    void testOutOfOrderOffer(BatchType<?> type) {
        Batch<?> ex1 = type.create(1, 3, 0);
        Batch<?> ex2 = type.create(2, 3, 0);
        Batch<?> ex3 = type.create(4, 3, 0);
        ex1.putRow(DUMMY_ROW);
        for (int i = 0; i < 2; i++) ex2.putRow(DUMMY_ROW);
        for (int i = 0; i < 4; i++) ex3.putRow(DUMMY_ROW);

        for (var permutation : List.of(List.of(2, 1, 0), List.of(1, 2, 0), List.of(0, 2, 1))) {
            Batch<?> b1 = type.create(1, 3, B_SP_LEN);
            Batch<?> b2 = type.create(2, 3, 2*B_SP_LEN);
            Batch<?> b3 = type.create(4, 3, 4*B_SP_LEN);
            b1.reserve(1, B_SP_LEN);
            b2.reserve(2, 2*B_SP_LEN);
            b3.reserve(4, 4*B_SP_LEN);
            assertTrue(b1.beginOffer());
            for (int c : permutation)
                assertTrue(b1.offerTerm(c, DUMMY_ROW[c]));
            assertTrue(b1.commitOffer());

            assertTrue(((Batch)b2).offerRow(b1, 0));
            ((Batch)b2).putRow(b1, 0);

            // put by term
            b3.beginPut();
            for (int c = 0; c < 3; c++)
                b3.putTerm(c, b1.get(0, c));
            b3.commitPut();
            assertTrue(b3.beginOffer());
            // offer by term
            for (int c = 0; c < 3; c++)
                assertTrue(b3.offerTerm(c, b1.get(0, c)));
            assertTrue(b3.commitOffer());
            // offer by term from b1
            assertTrue(b3.beginOffer());
            for (int c = 0; c < 3; c++)
                assertTrue(((Batch)b3).offerTerm(c, b1, 0, c));
            assertTrue(b3.commitOffer());
            // put by term from b1
            b3.beginPut();
            for (int c = 0; c < 3; c++)
                ((Batch)b3).putTerm(c, b1, 0, c);
            b3.commitPut();

            String ctx = "permutation="+permutation;
            assertBatchesEquals((Batch)ex1, (Batch)b1, ctx);
            assertBatchesEquals((Batch)ex2, (Batch)b2, ctx);
            assertBatchesEquals((Batch)ex3, (Batch)b3, ctx);
        }
    }

    @ParameterizedTest @MethodSource("types")
    <B extends Batch<B>> void testNullRow(BatchType<B> type) {
        Size sz = new Size(4, 2);
        B n = type.create(1, 2, 0);
        n.putRow(new Term[]{null, null});

        B uo0 = type.create(1, 2, 0);
        uo0.beginPut();
        uo0.putTerm(1, sz.terms[0][1]);
        uo0.putTerm(0, sz.terms[0][0]);
        uo0.commitPut();
        uo0.putRow(n, 0);
        if (type == Batch.COMPRESSED) assertTrue(((CompressedBatch)uo0).validate());

        B uo1 = type.create(2, 2, 0);
        uo1.beginPut();
        uo1.putTerm(1, sz.terms[0][1]);
        uo1.putTerm(0, sz.terms[0][0]);
        uo1.commitPut();
        uo1.beginPut();
        uo1.commitPut();
        if (type == Batch.COMPRESSED) assertTrue(((CompressedBatch)uo1).validate());

        B o0 = type.create(2, 2, 0);
        o0.putRow(sz.terms[0]);
        o0.putRow(n, 0);
        if (type == Batch.COMPRESSED) assertTrue(((CompressedBatch)o0).validate());

        B o1 = type.create(1, 2, 0);
        o1.putRow(sz.terms[0]);
        o1.beginPut();
        o1.commitPut();
        if (type == Batch.COMPRESSED) assertTrue(((CompressedBatch)o1).validate());

        B expected = type.create(2, 2, 0);
        expected.putRow(sz.terms[0]);
        expected.beginPut();
        expected.commitPut();
        assertEquals(2, expected.rows);

        assertBatchesEquals(expected, uo0, "uo0");
        assertBatchesEquals(expected, uo1, "uo1");
        assertBatchesEquals(expected, o0, "o0");
        assertBatchesEquals(expected, o1, "o1");
    }
}