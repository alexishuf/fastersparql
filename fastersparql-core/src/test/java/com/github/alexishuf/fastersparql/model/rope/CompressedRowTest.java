package com.github.alexishuf.fastersparql.model.rope;

import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.row.CompressedRow;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.function.Consumer;

import static com.github.alexishuf.fastersparql.model.rope.RopeDict.*;
import static com.github.alexishuf.fastersparql.model.row.RowType.COMPRESSED;
import static java.util.Arrays.copyOf;
import static org.junit.jupiter.api.Assertions.*;

class CompressedRowTest {

    @BeforeEach void    setUp() { RopeDict.reset(); }
    @AfterEach  void tearDown() { RopeDict.reset(); }

    @Test
    void rawTest() {
        Term i1 = Term.typed("1", DT_integer);
        Term type = Term.RDF_TYPE;
        Consumer<CompressedRow.Builder> withTerms = b -> b.set(0, i1).set(2, type);
        Consumer<CompressedRow.Builder> withRow = b -> {
            withTerms.accept(b);
            byte[] row = b.build();
            b.set(0, row, 0).set(2, row, 2);
        };
        for (Consumer<CompressedRow.Builder> filler : List.of(withTerms, withRow)) {
            var b = COMPRESSED.builder(3);
            filler.accept(b);
            byte[] row = b.build();
            assertArrayEquals(new byte[]{
                    /*  0 */   0, 0, 0, 0,                        // hash
                    /*  4 */   3, 0,                              // columns
                    /*  6 */  26, 0,                              // offset[0]
                    /*  8 */  28, 0,                              // offset[1]
                    /* 10 */  28, 0,                              // offset[2]
                    /* 12 */  33, 0,                              // offset[*]
                    /* 14 */ (byte) DT_integer, 0, 0, (byte) 0x80, // id[0]
                    /* 18 */ 0, 0, 0, 0,          // id[1]
                    /* 22 */ (byte) P_RDF, 0, 0, 0,          // id[2]
                    /* 26 */ '"', '1',                            // string[0]
                    /* 28 */ 't', 'y', 'p', 'e', '>',             // string[1]
            }, row);

            assertEquals(3, COMPRESSED.columns(row));
            assertEquals(i1, COMPRESSED.get(row, 0));
            assertNull(COMPRESSED.get(row, 1));
            assertEquals(type, COMPRESSED.get(row, 2));

            assertTrue(COMPRESSED.equalsSameVars(row, row));
            assertTrue(COMPRESSED.equalsSameVars(row, copyOf(row, row.length)));
            assertEquals("[1, null, a]", COMPRESSED.toString(row));
        }
    }

    @Test
    void rawTestProjectInPlace() {
        Term x = Term.typed("1", DT_integer);
        Term y = Term.typed("23", DT_INT);
        Term z = Term.typed("-2.3", DT_decimal);
        var b = COMPRESSED.builder(3);
        b.set(0, x);
        b.set(1, y);
        b.set(2, z);
        var merger = COMPRESSED.merger(Vars.of("y", "z"),
                                       Vars.of("x", "y", "z"),  Vars.EMPTY);
        byte[] row = b.build();
        row[0] = row[1] = row[2] = row[3] = 23; // fill bogus hash
        byte[] same = merger.projectInPlace(row);
        assertSame(row, same);
        assertArrayEquals(new byte[] {
                /*  0 */  0, 0, 0, 0,                         // hash (must be reset)
                /*  4 */  2, 0,                               // columns
                /*  6 */ 20, 0,                               // offset[0]
                /*  8 */ 23, 0,                               // offset[1]
                /* 10 */ 28, 0,                               // offset[*]
                /* 12 */ (byte)DT_INT,     0, 0, (byte)0x80, // ids[0]
                /* 16 */ (byte)DT_decimal, 0, 0, (byte)0x80, // ids[1]
                /* 20 */ '"', '2', '3',                      // strings[0]
                /* 23 */ '"', '-', '2', '.', '3',            // strings[1]
        }, copyOf(row, 28));
        assertTrue(COMPRESSED.equalsSameVars(row, row));
        assertTrue(COMPRESSED.equalsSameVars(row, copyOf(row, row.length)));
        assertTrue(COMPRESSED.equalsSameVars(row, copyOf(row, 28)));

        assertEquals(2, COMPRESSED.columns(row));
        assertEquals(y, COMPRESSED.get(row, 0));
        assertEquals(z, COMPRESSED.get(row, 1));

        var b2 = COMPRESSED.builder(2);
        b2.set(0, y);
        b2.set(1, z);
        byte[] row2 = b2.build();
        COMPRESSED.equalsSameVars(row, row2);
        COMPRESSED.equalsSameVars(row2, row);
        assertEquals(COMPRESSED.hash(row2), COMPRESSED.hash(row));
    }

    @Test
    void rawTestMerge() {
        Term type = Term.RDF_TYPE;
        Term i23 = Term.typed("-23", DT_integer);
        Term d23 = Term.typed("2.3", DT_decimal);
        Term bob = Term.lang("bob", "en");

        var b = COMPRESSED.builder(3);
        byte[] left = b.set(1, type).set(2, bob).build();
        byte[] right = b.set(1, i23).set(2, d23).build();

        var merger = COMPRESSED.merger(Vars.of("y1", "e", "x2", "x1", "y2"),
                                       Vars.of("z", "x1", "y1"),
                                       Vars.of("z", "x2", "y2"));
        byte[] merged = merger.merge(left, right);
        byte[] mergedLeft = merger.merge(left, null);
        byte[] mergedRight = merger.merge(null, right);

        assertArrayEquals(new byte[]{
                /*  0 */  0, 0, 0, 0,  //hash
                /*  4 */  5, 0,        //cols

                /*  6 */ 38, 0,        //offset[0]
                /*  8 */ 46, 0,        //offset[1]
                /* 10 */ 46, 0,        //offset[2]
                /* 12 */ 50, 0,        //offset[3]
                /* 14 */ 55, 0,        //offset[4]
                /* 16 */ 59, 0,        //offset[5]

                /* 18 */ 0,                0, 0, 0,          //id[0]
                /* 22 */ 0,                0, 0, 0,          //id[1]
                /* 26 */ (byte)DT_integer, 0, 0, (byte)0x80, //id[2]
                /* 30 */ (byte)P_RDF,      0, 0, 0,          //id[3]
                /* 34 */ (byte)DT_decimal, 0, 0, (byte)0x80, //id[4]

                /* 38 */  '"', 'b', 'o', 'b', '"', '@', 'e', 'n', //string[0]
                /* 46 */                                          //string[1]
                /* 46 */  '"', '-', '2', '3',                     //string[2]
                /* 50 */  't', 'y', 'p', 'e', '>',                //string[3]
                /* 55 */  '"', '2', '.', '3',                     //string[4]
        }, merged);

        assertEquals(5,    COMPRESSED.columns(merged));
        assertEquals(bob,  COMPRESSED.get(merged, 0));
        assertNull(        COMPRESSED.get(merged, 1));
        assertEquals(i23,  COMPRESSED.get(merged, 2));
        assertEquals(type, COMPRESSED.get(merged, 3));
        assertEquals(d23,  COMPRESSED.get(merged, 4));
        assertTrue(COMPRESSED.equalsSameVars(merged, copyOf(merged, merged.length)));
        byte[] builtMerged = COMPRESSED.builder(5)
                .set(0, bob).set(2, i23).set(3, type).set(4, d23).build();
        assertTrue(COMPRESSED.equalsSameVars(merged, builtMerged));
        assertTrue(COMPRESSED.equalsSameVars(builtMerged, merged));
        assertEquals(COMPRESSED.hash(builtMerged), COMPRESSED.hash(merged));

        assertArrayEquals(new byte[]{
                /*  0 */  0, 0, 0, 0,  //hash
                /*  4 */  5, 0,        //cols

                /*  6 */ 38, 0,        //offset[0]
                /*  8 */ 46, 0,        //offset[1]
                /* 10 */ 46, 0,        //offset[2]
                /* 12 */ 46, 0,        //offset[3]
                /* 14 */ 51, 0,        //offset[4]
                /* 16 */ 51, 0,        //offset[5]

                /* 18 */ 0,                0, 0, 0, //id[0]
                /* 22 */ 0,                0, 0, 0, //id[1]
                /* 26 */ 0,                0, 0, 0, //id[2]
                /* 30 */ (byte)P_RDF,      0, 0, 0, //id[3]
                /* 34 */ 0,                0, 0, 0, //id[4]

                /* 38 */  '"', 'b', 'o', 'b', '"', '@', 'e', 'n', //string[0]
                /* 46 */                                          //string[1]
                /* 46 */                                          //string[2]
                /* 46 */  't', 'y', 'p', 'e', '>'                 //string[3]
                /* 51 */                                          //string[4]
        }, mergedLeft);

        assertEquals(5,    COMPRESSED.columns(mergedLeft));
        assertEquals(bob,  COMPRESSED.get(mergedLeft, 0));
        assertNull(        COMPRESSED.get(mergedLeft, 1));
        assertNull(        COMPRESSED.get(mergedLeft, 2));
        assertEquals(type, COMPRESSED.get(mergedLeft, 3));
        assertNull(        COMPRESSED.get(mergedLeft, 4));
        assertTrue(COMPRESSED.equalsSameVars(mergedLeft, mergedLeft));
        assertTrue(COMPRESSED.equalsSameVars(mergedLeft, copyOf(mergedLeft, mergedLeft.length)));
        byte[] builtMergedLeft = COMPRESSED.builder(5).set(0, bob).set(3, type).build();
        assertTrue(COMPRESSED.equalsSameVars(mergedLeft, builtMergedLeft));
        assertTrue(COMPRESSED.equalsSameVars(builtMergedLeft, mergedLeft));
        assertEquals(COMPRESSED.hash(builtMergedLeft), COMPRESSED.hash(mergedLeft));

        assertArrayEquals(new byte[]{
                /*  0 */  0, 0, 0, 0,  //hash
                /*  4 */  5, 0,        //cols

                /*  6 */ 38, 0,        //offset[0]
                /*  8 */ 38, 0,        //offset[1]
                /* 10 */ 38, 0,        //offset[2]
                /* 12 */ 42, 0,        //offset[3]
                /* 14 */ 42, 0,        //offset[4]
                /* 16 */ 46, 0,        //offset[5]

                /* 18 */ 0,                0, 0, 0,          //id[0]
                /* 22 */ 0,                0, 0, 0,          //id[1]
                /* 26 */ (byte)DT_integer, 0, 0, (byte)0x80, //id[2]
                /* 30 */ 0,                0, 0, 0,          //id[3]
                /* 34 */ (byte)DT_decimal, 0, 0, (byte)0x80, //id[4]

                /* 38 */                                          //string[0]
                /* 38 */                                          //string[1]
                /* 38 */  '"', '-', '2', '3',                     //string[2]
                /* 42 */                                          //string[3]
                /* 42 */  '"', '2', '.', '3',                     //string[4]
        }, mergedRight);

        assertEquals(5,    COMPRESSED.columns(mergedRight));
        assertNull(        COMPRESSED.get(mergedRight, 0));
        assertNull(        COMPRESSED.get(mergedRight, 1));
        assertEquals(i23,  COMPRESSED.get(mergedRight, 2));
        assertNull(        COMPRESSED.get(mergedRight, 3));
        assertEquals(d23,  COMPRESSED.get(mergedRight, 4));
        assertTrue(COMPRESSED.equalsSameVars(mergedRight, copyOf(mergedRight, mergedRight.length)));
        byte[] builtMergedRight = COMPRESSED.builder(5)
                .set(2, i23).set(4, d23).build();
        assertTrue(COMPRESSED.equalsSameVars(mergedRight, builtMergedRight));
        assertTrue(COMPRESSED.equalsSameVars(builtMergedRight, mergedRight));
        assertEquals(COMPRESSED.hash(builtMergedRight), COMPRESSED.hash(mergedRight));
    }

//    record B(List<String> ntTerms) implements Runnable {
//        public void run() {
//            var b = COMPRESSED.builder(ntTerms.size());
//            var sb = new StringBuilder();
//            int[] begins = new int[ntTerms.size()];
//            int[] ends = new int[ntTerms.size()];
//
//            for (int i = 0; i < ntTerms.size(); i++) {
//                ends[i] = begins[i] = sb.length();
//                String nt = ntTerms.get(i);
//                if (nt != null) {
//                    ends[i] += nt.length();
//                    sb.append(nt).append('\t');
//                }
//            }
//            Rope rope1 = new ByteRope(("."+sb).getBytes(UTF_8)).sub(1, sb.length());
//            for (int i = 0; i < ntTerms.size(); i++) {
//                if (ntTerms.get(i) != null)
//                    assertSame(b, b.set(i, rope1, begins[i], ends[i]));
//            }
//            byte[] r1 = b.build();
//
//            for (int i = 0; i < ntTerms.size(); i++)
//                b.set(i, new ByteRope("_:garbage1".getBytes(UTF_8)), 0, 10);
//
//            BufferRope rope2 = new BufferRope(ByteBuffer.wrap(sb.toString().getBytes(UTF_8)));
//            for (int i = 0; i < ntTerms.size(); i++) {
//                b.set(i, rope2, begins[i], ends[i]);
//            }
//            byte[] r2 = b.build();
//
//            for (int i = 0; i < ntTerms.size(); i++)
//                b.set(i, Term.valueOf("_:garbage3"));
//            byte[] r3 = b.build();
//
//            // test equality
//            assertTrue(COMPRESSED.equalsSameVars(r1, r1));
//            assertTrue(COMPRESSED.equalsSameVars(r2, r2));
//            assertTrue(COMPRESSED.equalsSameVars(r1, r2));
//            assertTrue(COMPRESSED.equalsSameVars(r2, r1));
//            assertFalse(COMPRESSED.equalsSameVars(r1, r3));
//            assertFalse(COMPRESSED.equalsSameVars(r2, r3));
//
//            //caching of hash code does not affect equality
//            int h1 = COMPRESSED.hash(r1);
//            assertTrue(COMPRESSED.equalsSameVars(r1, r2));
//            assertTrue(COMPRESSED.equalsSameVars(r2, r1));
//
//            // equal rows have the same hash
//            assertEquals(h1, COMPRESSED.hash(r2));
//
//            // test getNT()
//            for (int i = 0; i < ntTerms.size(); i++) {
//                if (ntTerms.get(i) == null) {
//                    assertNull(COMPRESSED.get(r1, i));
//                    assertNull(COMPRESSED.get(r2, i));
//                } else {
//                    assertEquals(ntTerms.get(i), requireNonNull(COMPRESSED.get(r1, i)).toString());
//                    assertEquals(ntTerms.get(i), requireNonNull(COMPRESSED.get(r2, i)).toString());
//                }
//            }
//
//            //test trivial projection
//            byte[] r1Copy = Arrays.copyOf(r1, r1.length);
//            byte[] r2Copy = Arrays.copyOf(r2, r2.length);
//            var vars = new Vars.Mutable(ntTerms.size());
//            for (int i = 0; i < ntTerms.size(); i++)
//                vars.add(Rope.of('x', i));
//            var trivialMerger = COMPRESSED.projector(vars, vars);
//            assertSame(trivialMerger.merge(r1, null), r1);
//            assertSame(trivialMerger.merge(r2, null), r2);
//            assertSame(trivialMerger.projectInPlace(r1), r1);
//            assertSame(trivialMerger.projectInPlace(r2), r2);
//            assertArrayEquals(r1Copy, r1);
//            assertArrayEquals(r2Copy, r2);
//        }
//    }
//
//    static Stream<Arguments> testBuild() {
//        List<B> list = new ArrayList<>();
//        String dbl = "\"^^<http://www.w3.org/2001/XMLSchema#double>";
//        String itg = "\"^^<http://www.w3.org/2001/XMLSchema#integer>";
//        String en = "\"@en-US";
//        String Alice = "http://example.org/Alice";
//        String Bob = "http://example.org/Bob";
//
//        //single-term rows
//        list.add(new B(List.of("\"2.3E+05"+dbl)));
//        list.add(new B(List.of("\"bob"+en)));
//        list.add(new B(List.of("\"bob\"")));
//
//        //two-term rows
//        list.add(new B(List.of("\"23"+itg, "\"bob\"")));
//        list.add(new B(List.of("\"bob\"", Bob)));
//        list.add(new B(List.of(Alice, "\"alice"+en)));
//
//        // two-term rows with null
//        list.add(new B(asList(null, "\"23"+itg)));
//        list.add(new B(asList(Alice, null)));
//        list.add(new B(asList("\"bob\"", null)));
//
//        //fully null rows
//        list.add(new B(singletonList(null)));
//        list.add(new B(asList(null, null)));
//        list.add(new B(asList(null, null, null)));
//
//        //long rows
//        for (int columns : List.of(3, 8, 32_768)) {
//            ArrayList<String> terms = new ArrayList<>();
//            for (int i = 0; i < columns; i++)
//                terms.add((i & 1) == 0 ? "<http://example.org/"+i+">" : "\""+i+itg);
//            list.add(new B(terms));
//        }
//
//        return list.stream().map(Arguments::arguments);
//    }
//
//    @ParameterizedTest @MethodSource
//    void testBuild(B b) { b.run(); }
//
//
//    record P(List<String> nt, CompressedRow.Merger merger) implements Runnable {
//        @Override public void run() {
//            var inVars = Vars.from(IntStream.range(0, nt.size()).mapToObj(i -> Rope.of('x', i)));
//            var b = COMPRESSED.builder(nt.size());
//            for (int i = 0; i < nt.size(); i++) {
//                if (nt.get(i) != null) {
//                    ByteRope rope = new ByteRope(nt.get(i).getBytes(UTF_8));
//                    b.set(i, rope, 0, rope.len());
//                }
//            }
//            byte[] row = b.build();
//            for (int i = 0; i < nt.size(); i++) {
//                assertEquals(nt.get(i) == null ? null : new ByteRope(nt.get(i).getBytes(UTF_8)),
//                             COMPRESSED.get(row, i), "i="+i);
//            }
//            byte[] projected = merger.projectInPlace(row);
//            if (merger.outVars.size() <= inVars.size() && row.length < 32_768)
//                assertSame(row, projected);
//            for (int i = 0; i < merger.outVars.size(); i++) {
//                int source   = inVars.indexOf(merger.outVars.get(i));
//                var expected = source < 0 || nt.get(source) == null ? null
//                             : Term.valueOf(nt.get(source));
//                var ctx      = "i=" + i + ", source=" + source;
//                assertEquals(expected, COMPRESSED.get(projected, i), ctx);
//            }
//        }
//    }
//
//    static Stream<Arguments> testProject() {
//        List<P> list = new ArrayList<>();
//        Map<Integer, CompressedRow.Merger> reverseProjector = new HashMap<>();
//        Map<Integer, CompressedRow.Merger> firstHalfProjector = new HashMap<>();
//        Map<Integer, CompressedRow.Merger> secondHalfProjector = new HashMap<>();
//        Map<Integer, CompressedRow.Merger> withBogusProjector = new HashMap<>();
//        testBuild().map(a -> ((B)a.get()[0]).ntTerms).forEach(terms -> {
//            int size = terms.size();
//            Vars in = Vars.from(IntStream.range(0, size).mapToObj(i -> "x" + i));
//            list.add(new P(terms, COMPRESSED.projector(in, in)));
//
//            Vars reverse = in.reverse();
//            list.add(new P(terms, reverseProjector.computeIfAbsent(size, k
//                    -> COMPRESSED.projector(reverse, in))));
//
//            var firstHalf = new Vars.Mutable(in.size());
//            for (int i = 0; i < Math.min(1, in.size()/2); i++)
//                firstHalf.add(in.get(i));
//            list.add(new P(terms, firstHalfProjector.computeIfAbsent(size, k
//                    -> COMPRESSED.projector(firstHalf, in))));
//
//            var secondHalf = new Vars.Mutable(in.size());
//            for (int i = in.size()/2; i < in.size(); i++)
//                secondHalf.add(in.get(i));
//            list.add(new P(terms, secondHalfProjector.computeIfAbsent(size, k
//                    -> COMPRESSED.projector(secondHalf, in))));
//
//            var withBogus = new Vars.Mutable(in.size()+2);
//            withBogus.add(0, Rope.of("before"));
//            withBogus.add(Term.valueOf("?after"));
//            list.add(new P(terms, withBogusProjector.computeIfAbsent(size, k
//                    -> COMPRESSED.projector(withBogus, in))));
//        });
//        return list.stream().map(Arguments::arguments);
//    }
//
//    @ParameterizedTest @MethodSource
//    void testProject(P p) { p.run(); }
}