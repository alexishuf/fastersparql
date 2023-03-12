package com.github.alexishuf.fastersparql.model.row;

import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.Rope;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class RowTypeTest {
    public static Stream<Arguments> types() {
        return Stream.of(arguments(RowType.ARRAY),
                         arguments(RowType.LIST),
                         arguments(RowType.COMPRESSED));
    }

    @ParameterizedTest @MethodSource("types")
    public void testCreateEmpty(RowType<Object> rt) {
        for (int cols = 0; cols < 16; cols++) {
            var b = rt.builder(cols);
            var row = b.build();
            for (int i = 0; i < cols; i++)
                assertNull(rt.get(row, i));
        }
    }

    @ParameterizedTest @MethodSource("types")
    public void testCreateWithNumberIRIs(RowType<Object> rt) {
        for (int columns = 0; columns < 16; columns++) {
            var b = rt.builder(columns);
            for (int i = 0; i < columns; i++)
                b.set(i, Term.iri("http://example.org/"+i));
            Object row = b.build();

            for (int i = 0; i < columns; i++)
                b.set(i, Term.iri("http://example.org/"+i));
            Object row2 = b.build();

            for (int i = 0; i < columns; i++)
                assertEquals(Term.iri("http://example.org/"+i), rt.get(row, i));
            int finalColumns = columns;

            assertThrows(IndexOutOfBoundsException.class, () -> rt.get(row, finalColumns));
            assertThrows(IndexOutOfBoundsException.class, () -> rt.get(row, finalColumns+1));
            assertThrows(IndexOutOfBoundsException.class, () -> rt.get(row, -1));

            //test equals()
            assertTrue(rt.equalsSameVars(row , row2));
            assertTrue(rt.equalsSameVars(row2, row ));
            assertTrue(rt.equalsSameVars(row , row ));

            // test hash()
            assertEquals(rt.hash(row), rt.hash(row2));
        }
    }

    @ParameterizedTest @MethodSource("types")
    public void testGetFromNull(RowType<?> rt) {
        assertNull(rt.get(null, 0));
        assertNull(rt.get(null, 16));
    }

    @ParameterizedTest @MethodSource("types")
    public void testToString(RowType<Object> rt) {
        assertEquals("[]", rt.toString(rt.builder(0).build()));
        var b = rt.builder(2);
        b.set(0, Term.RDF_TYPE);
        b.set(1, Term.lang("test", "en"));
        assertEquals("[a, \"test\"@en]",
                     rt.toString(b.build()));
    }

    public static Stream<Arguments> testMerge() {
        record D(Vars out, Vars left, Vars right) {
            D(String out, String left, String right) {
                this(Vars.of(Arrays.stream(out.split(",")).filter(s -> !s.isEmpty()).toArray(String[]::new)),
                     Vars.of(Arrays.stream(left.split(",")).filter(s -> !s.isEmpty()).toArray(String[]::new)),
                     Vars.of(Arrays.stream(right.split(",")).filter(s -> !s.isEmpty()).toArray(String[]::new)));
            }
            Arguments toArgs(RowType<?> rt) { return arguments(rt, out, left, right); }
        }
        List<D> data = List.of(
                new D("x,y", "x,y", ""),
                new D("x,y", "x,y", "z"),
                new D("x,y", "x,z", "y"),
                new D("x,y", "x,z", "y,z"),
                new D("x,y", "x,z", "x,y,z"),
                new D("x,y", "x,z", "y,x,z"),
                new D("x,y", "x,z", "x,z,y"),
                new D("x", "", "x,y"),
                new D("y", "", "x,y"),
                new D("z", "", "x,y"),
                new D("x", "x,y", ""),
                new D("y", "x,y", ""),
                new D("z", "x,y", "")
        );
        return types().map(a -> (RowType<?>)a.get()[0])
                      .flatMap(rt -> data.stream().map(d -> d.toArgs(rt)));
    }

    @ParameterizedTest @MethodSource
    public void testMerge(RowType<Object> rt, Vars out, Vars leftVars, Vars rightVars) {
        var b = rt.builder(leftVars.size());
        for (int i = 0; i < leftVars.size(); i++)
            b.set(i, Term.iri("http://example.org/l/"+i));
        Object left = b.build();

        b = rt.builder(rightVars.size());
        for (int i = 0; i < rightVars.size(); i++)
            b.set(i, Term.iri("http://example.org/r/"+i));
        Object right = b.build();

        var merger = rt.merger(out, leftVars, rightVars);
        var merged = merger.merge(left, right);
        assertEquals(out.size(), rt.columns(merged));

        List<Term> actual = new ArrayList<>(), expected = new ArrayList<>();
        for (int i = 0; i < out.size(); i++) {
            Rope name = out.get(i);
            int idx = leftVars.indexOf(name);
            if (idx >= 0)
                expected.add(Term.iri("http://example.org/l/"+idx));
            else if ((idx = rightVars.indexOf(name)) >= 0)
                expected.add(Term.iri("http://example.org/r/"+idx));
            else
                expected.add(null);
            actual.add(rt.get(merged, i));
        }
        assertEquals(expected, actual);

        var mb = rt.builder(out.size());
        for (int i = 0; i < out.size(); i++) {
            int src = leftVars.indexOf(out.get(i));
            if (src >= 0)
                mb.set(i, Term.iri("http://example.org/l/"+src));
            else if ((src = rightVars.indexOf(out.get(i))) >= 0)
                mb.set(i, Term.iri("http://example.org/r/"+src));
        }
        Object built = mb.build();
        assertTrue(rt.equalsSameVars(merged, merged));
        assertTrue(rt.equalsSameVars(merged, built));
        assertTrue(rt.equalsSameVars(built, merged));
    }

    public static Stream<Arguments> testProjectInPlace() {
        record D(Vars out, Vars in) {
            D(String out, String in) {
                this(Vars.of(Arrays.stream(out.split(",")).filter(s -> !s.isEmpty()).toArray(String[]::new)),
                     Vars.of(Arrays.stream(in.split(",")).filter(s -> !s.isEmpty()).toArray(String[]::new)));
            }
            Arguments toArgs(RowType<?> rt) { return arguments(rt, out, in); }
        }
        List<D> data = new ArrayList<>(List.of(
                new D("x", "x"),
                new D("x,y", "x,y"),
                new D("x", "x,y"),
                new D("y", "x,y"),
                new D("x,z", "x,y"),
                new D("z,x", "x,y"),
                new D("z,y", "x,y"),
                new D("z", "x,y")
        ));
        testMerge().filter(a -> !((Vars)a.get()[1]).intersects((Vars)a.get()[3]))
                .forEach(a -> data.add(new D((Vars)a.get()[1], (Vars)a.get()[3])));
        return types().flatMap(a -> data.stream().map(d -> d.toArgs((RowType<?>)a.get()[0])));
    }

    @ParameterizedTest @MethodSource
    public void testProjectInPlace(RowType<Object> rt, Vars outVars, Vars inVars) {
        var b = rt.builder(inVars.size());
        for (int i = 0; i < inVars.size(); i++)
            b.set(i, Term.iri("http://example.org/"+i));
        var in = b.build();

        var projector = rt.projector(outVars, inVars);
        Object copy = projector.merge(in, null);
        Object inPlace = projector.projectInPlace(in);

        assertEquals(outVars.size(), rt.columns(copy));
        assertEquals(outVars.size(), rt.columns(inPlace));

        List<Term> copyTerms = new ArrayList<>(), inPlaceTerms = new ArrayList<>();
        List<Term> expected = new ArrayList<>();
        for (int i = 0; i < outVars.size(); i++) {
            int src = inVars.indexOf(outVars.get(i));
            expected.add(src < 0 ? null : Term.iri("http://example.org/"+src));
            copyTerms.add(rt.get(copy, i));
            inPlaceTerms.add(rt.get(inPlace, i));
        }
        assertEquals(copyTerms, expected);
        assertEquals(inPlaceTerms, expected);

        var pb = rt.builder(outVars.size());
        for (int i = 0; i < outVars.size(); i++) {
            int src = inVars.indexOf(outVars.get(i));
            if (src >= 0) pb.set(i, Term.iri("http://example.org/"+src));
        }
        Object built = pb.build();
        assertTrue(rt.equalsSameVars(copy, copy));
        assertTrue(rt.equalsSameVars(built, built));
        assertTrue(rt.equalsSameVars(inPlace, inPlace));
        assertTrue(rt.equalsSameVars(copy, built));
        assertTrue(rt.equalsSameVars(inPlace, built));
    }

    @ParameterizedTest @MethodSource("types")
    public void testHashCollisionRegression(RowType<Object> rt) {
        List<Integer> hashes = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            hashes.add(rt.hash(rt.builder(3).set(0, Term.valueOf("_:"+i+""))
                                            .set(1, Term.valueOf("\""+i+"\"")).build()));
        }
        assertEquals(5, hashes.stream().distinct().count());
        assertEquals(5, hashes.stream().map(h -> h & 7).distinct().count());
    }

}