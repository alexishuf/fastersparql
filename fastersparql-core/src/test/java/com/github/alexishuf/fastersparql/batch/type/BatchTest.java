package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.model.RopeArrayMap;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.ByteRope;
import com.github.alexishuf.fastersparql.model.rope.Rope;
import com.github.alexishuf.fastersparql.model.rope.RopeDict;
import com.github.alexishuf.fastersparql.sparql.PrefixAssigner;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import jdk.incubator.vector.ByteVector;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.model.rope.RopeDict.DT_integer;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.IntStream.range;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class BatchTest {
    private static final List<BatchType<?>> TYPES = List.of(
            TermBatchType.INSTANCE,
            CompressedBatchType.INSTANCE
    );

    static final class Size {
        private static final int ALIGNMENT = ByteVector.SPECIES_PREFERRED.length();
        private static final int P_EX = (int) RopeDict.internIri(Rope.of("<http://www.example.org/ns#>"), 0, 28);
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
                        case 1 -> Term.prefixed(P_EX, ("R"+r+"C"+c+">").getBytes(UTF_8));
                        case 2 -> Term.typed("\""+(r*cols + c), DT_integer);
                        case 3 -> null;
                        default -> throw new IllegalArgumentException();
                    };
                    if (terms[r][c] != null) {
                        unaligned += terms[r][c].local.length;
                        aligned   += terms[r][c].local.length;
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
                for (int c = 0; c < cols; c++) batch.putTerm(terms[r][c]);
                batch.commitPut();
            }
            return batch;
        }

        public <B extends Batch<B>> B reverseFill(B batch) {
            for (int r = 0; r < rows; r++) {
                batch.beginPut();
                for (int c = 0; c < cols; c++) batch.putTerm(terms[rows-1-r][cols-1-c]);
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

    private interface ForEachSizeTest {
        <B extends Batch<B>> void run(BatchType<B> type, Size size, String ctx);
    }

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
                assertEquals(s.rows, b.rowsCapacity(), ctx);
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

    @SuppressWarnings("SimplifiableAssertion")
    static <B extends Batch<B>> void assertBatchesEquals(B expected, B batch, String outerCtx) {
        var assigner = new PrefixAssigner(new RopeArrayMap());
        assertEquals(expected.rows, batch.rows, outerCtx);
        assertEquals(expected.cols, batch.cols, outerCtx);
        for (int r = 0, rows = expected.rows, cols = expected.cols; r < rows; r++) {
            for (int c = 0; c < cols; c++) {
                String ctx = ", r=" + r + ", c=" + c+", "+outerCtx;
                assertEquals(expected.get(r, c), batch.get(r, c), ctx);
                assertTrue(batch.equals(r, c, expected, r, c), ctx);
                assertEquals(expected.hash(r, c), batch.hash(r, c), ctx);

                Term t = expected.get(r, c);
                assertEquals(t == null ? 0 : t.flaggedDictId, batch.flaggedId(r, c), ctx);
                assertEquals(t == null ? null : t.type(), batch.termType(r, c), ctx);
                assertEquals(t == null ? 0 : t.asDatatypeId(), batch.asDatatypeId(r, c), ctx);
                assertEquals(t == null ? null : t.datatypeTerm(), batch.datatypeTerm(r, c), ctx);
                assertEquals(t == null ? 0 : t.hashCode(), batch.hash(r, c), ctx);

                ByteRope ex = new ByteRope(), ac = new ByteRope();
                if (t != null) t.toSparql(ex, assigner);
                batch.writeSparql(ac, r, c, assigner);
                assertEquals(ex, ac, ctx);

                ex.clear().append(t == null ? ByteRope.EMPTY : t);
                batch.writeNT(ac.clear(), r, c);
                assertEquals(ex, ac, ctx);
            }
            assertTrue(batch.equals(r, expected, r), "r="+r+", "+outerCtx);
            assertEquals(expected.hash(r), batch.hash(r), "r="+r+", "+outerCtx);
        }
        assertTrue(expected.equals(batch), outerCtx);
        assertTrue(batch.equals(expected), outerCtx);
        assertEquals(expected.hashCode(), batch.hashCode());
    }

    static void assertBatchesEquals(Size size, Batch<?> batch, String outerCtx) {
        PrefixAssigner assigner = new PrefixAssigner(new RopeArrayMap());
        for (int r = 0, rows = size.rows, cols = size.cols; r < rows; r++) {
            for (int c = 0; c < cols; c++) {
                Term t = size.terms[r][c];
                String ctx = "r=" + r + ", c=" + c+", "+outerCtx;
                assertEquals(t, batch.get(r, c), ctx);
                assertTrue(batch.equals(r, c, t), ctx);

                assertEquals(t == null ? 0 : t.flaggedDictId, batch.flaggedId(r, c), ctx);
                assertEquals(t == null ? 0 : t.flaggedDictId, batch.flaggedId(r, c), ctx);
                assertEquals(t == null ? null : t.type(), batch.termType(r, c), ctx);
                assertEquals(t == null ? 0 : t.asDatatypeId(), batch.asDatatypeId(r, c), ctx);
                assertEquals(t == null ? null : t.datatypeTerm(), batch.datatypeTerm(r, c), ctx);
                assertEquals(t == null ? 0 : t.hashCode(), batch.hash(r, c), ctx);

                ByteRope ex = new ByteRope(), ac = new ByteRope();
                if (t != null)
                    t.toSparql(ex, assigner);
                batch.writeSparql(ac, r, c, assigner);
                assertEquals(ex, ac, ctx);

                ex.clear().append(t == null ? ByteRope.EMPTY : t);
                batch.writeNT(ac.clear(), r, c);
                assertEquals(ex, ac, ctx);
            }
            assertTrue(batch.equals(r, size.terms[r]), "r="+r+", "+outerCtx);
        }
    }



    @Test void testPut() {
        forEachSize(new ForEachSizeTest() {
            @Override
            public <B extends Batch<B>> void run(BatchType<B> type, Size size, String ctx) {
                int reqBytes = size.requiredBytes(type);
                B b1 = type.create(1, size.cols, 1);
                B b2 = type.create(1, size.cols, 0);
                B b3 = type.create(0, size.cols, 0);
                B b4 = size.reverseFill(type.create(size.rows, size.cols, reqBytes));
                B b5 = size.reverseFill(type.create(size.rows, size.cols, reqBytes));
                b4.clear(size.cols*2);
                b5.clear(size.cols*2);
                b4.clear(size.cols);
                b5.clear(size.cols);
                for (int r = 0; r < size.rows; r++) {
                    b1.beginPut();
                    for (int c = 0; c < size.cols; c++) b1.putTerm(size.terms[r][c]);
                    b1.commitPut();

                    b2.beginPut();
                    for (int c = 0; c < size.cols; c++) b2.putTerm(b1, r, c);
                    b2.commitPut();

                    b4.beginPut();
                    for (int c = 0; c < size.cols; c++) b4.putTerm(size.terms[r][c]);
                    b4.commitPut();

                    b5.beginPut();
                    for (int c = 0; c < size.cols; c++) b5.putTerm(size.terms[r][c]);
                    b5.commitPut();
                }
                b3.put(b1);
                assertBatchesEquals(size, b1, ctx);
                assertBatchesEquals(size, b2, ctx);
                assertBatchesEquals(size, b3, ctx);
                assertBatchesEquals(size, b4, ctx);
                assertBatchesEquals(size, b5, ctx);
                assertBatchesEquals(b1, b2, ctx);
                assertBatchesEquals(b2, b3, ctx);
                assertBatchesEquals(b2, b4, ctx);
                assertBatchesEquals(b2, b5, ctx);
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
                b4.clear(size.cols*2);
                b5.clear(size.cols*2);
                b4.clear(size.cols);
                b5.clear(size.cols);

                b1.reserve(size.rows, reqBytes);
                b2.reserve(size.rows, reqBytes);
                b3.reserve(size.rows, reqBytes);

                assertTrue(b1.hasCapacity(size.rows, reqBytes));
                assertTrue(b2.hasCapacity(size.rows, reqBytes));
                assertTrue(b3.hasCapacity(size.rows, reqBytes));
                assertTrue(b4.hasCapacity(size.rows, reqBytes));
                assertTrue(b5.hasCapacity(size.rows, reqBytes));
                for (int r = 0; r < size.rows; r++) {
                    assertTrue(b1.beginOffer());
                    for (int c = 0; c < size.cols; c++)
                        assertTrue(b1.offerTerm(size.terms[r][c]), "r=, "+r+"c="+c);
                    assertTrue(b1.commitOffer());
                    assertTrue(b2.beginOffer());
                    for (int c = 0; c < size.cols; c++)
                        assertTrue(b2.offerTerm(b1, r, c), "r= "+r+",c="+c);
                    assertTrue(b2.commitOffer());

                    assertTrue(b4.beginOffer());
                    for (int c = 0; c < size.cols; c++)
                        assertTrue(b4.offerTerm(size.terms[r][c]), "r=, "+r+"c="+c);
                    assertTrue(b4.commitOffer());
                    assertTrue(b5.beginOffer());
                    for (int c = 0; c < size.cols; c++)
                        assertTrue(b5.offerTerm(b1, r, c), "r= "+r+",c="+c);
                    assertTrue(b5.commitOffer());
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
                            ex.putTerm(null);
                            for (Term term : row) ex.putTerm(term);
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
                            for (Term t : row) {
                                ex.putTerm(t);
                                ex.putTerm(null);
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
                            for (int i = 0; i < row.length/2; i++) ex.putTerm(row[i]);
                        },
                        (Function<Vars, Vars>)in -> {
                            var out = new Vars.Mutable(10);
                            for (int i = 0; i < in.size()/2; i++)
                                out.add(in.get(i));
                            return out;
                        }),
                arguments("rightHalf",
                        (BiConsumer<Term[], Batch<?>>)(row, ex) -> {
                            for (int i = row.length/2; i < row.length; ++i) ex.putTerm(row[i]);
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
                        (BiConsumer<Term[], Batch<?>>)(row, ex)
                                -> { for (Term t : row) ex.putTerm(t); },
                        (Function<Vars, Vars>)in -> in
                ),
                arguments("drop-odd",
                        (Function<Size, List<Integer>>)s -> range(0, s.rows).filter(i -> (i%2)==0)
                                .boxed().toList(),
                        (BiConsumer<Term[], Batch<?>>)(row, ex)
                                -> { for (Term t : row) ex.putTerm(t); },
                        (Function<Vars, Vars>)in -> in
                ),
                arguments("drop-first-half",
                        (Function<Size, List<Integer>>)s -> range(s.rows/2, s.rows).boxed().toList(),
                        (BiConsumer<Term[], Batch<?>>)(row, ex) -> { for (Term t : row) ex.putTerm(t); },
                        (Function<Vars, Vars>)in -> in
                ),
                arguments("drop-second-half",
                        (Function<Size, List<Integer>>)s -> range(0, s.rows/2).boxed().toList(),
                        (BiConsumer<Term[], Batch<?>>)(row, ex) -> { for (Term t : row) ex.putTerm(t); },
                        (Function<Vars, Vars>)in -> in
                ),

                arguments("top-left",
                        (Function<Size, List<Integer>>)s -> range(0, s.rows/2).boxed().toList(),
                        (BiConsumer<Term[], Batch<?>>)(row, ex) -> {
                            for (int i = 0; i < row.length/2; i++) ex.putTerm(row[i]);
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
                            for (int i = row.length/2; i < row.length; i++) ex.putTerm(row[i]);
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
                            for (int i = 0; i < row.length/2; i++) ex.putTerm(row[i]);
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
                            for (int i = row.length/2; i < row.length; i++) ex.putTerm(row[i]);
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
                            for (int i = 0; i < row.length / 2; i++) ex.putTerm(row[i]);
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
                            for (int i = row.length / 2; i < row.length; i++) ex.putTerm(row[i]);
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
                            for (int i = row.length / 2; i < row.length; i++) ex.putTerm(row[i]);
                            ex.putTerm(null);
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
                RowFilter<B> rowFilter = (batch, row) -> !survivors.contains(row);
                var filter = out.equals(in) ? type.filter(rowFilter)
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


}