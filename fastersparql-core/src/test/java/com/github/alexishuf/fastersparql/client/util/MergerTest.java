package com.github.alexishuf.fastersparql.client.util;

import com.github.alexishuf.fastersparql.model.BindType;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.row.RowType;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.model.BindType.*;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class MergerTest {
    @SuppressWarnings("unused") static Stream<Arguments> testMerge() {
        return Stream.of(
                //     lVars   rVars   bindType  left          right    expected
                asList("x,y",  "y,z",  JOIN,     "_:l0,_:l1",  "_:r1",  "_:l0,_:l1,_:r1"),
                asList("x,y",  "z,y",  JOIN,     "_:l0,_:l1",  "_:r0",  "_:l0,_:l1,_:r0"),
                asList("x,y",  "y",    JOIN,     "_:l0,_:l1",  "",      "_:l0,_:l1"),
                asList("x",    "x,y",  JOIN,     "_:l0",       "_:r1",  "_:l0,_:r1"),
                asList("y",    "x,y",  JOIN,     "_:l0",       "_:r0",  "_:l0,_:r0"),
                asList("y",    "x,y",  LEFT_JOIN,"_:l0",       "_:r0",  "_:l0,_:r0"),
                /* drop all rVars */
                asList("x",    "x,y",  EXISTS,   "_:l0",       "_:r0",  "_:l0"),
                asList("x,y",  "y,z",  MINUS,    "_:l0,_:l1",  "_:r1",  "_:l0,_:l1")
        ).map(l -> arguments(
                Vars.of(l.get(0).toString().split(",")),      // leftVars
                Vars.of(l.get(1).toString().split(",")),      // rightVars
                l.get(2),                                           // bindType
                Term.array((Object[]) l.get(3).toString().split(",")), // left
                Term.array((Object[]) (l.get(4).equals("") ? new String[0] : l.get(4).toString().split(","))), // right
                Term.array((Object[]) l.get(5).toString().split(","))  // expected
        ));
    }

    @ParameterizedTest @MethodSource
    void testMerge(Vars leftVars, Vars rightVars, BindType bindType,
                   Term[] left, Term[] right, Term[] expected) {
        Vars rightFreeVars = rightVars.minus(leftVars);
        var merger = RowType.ARRAY.merger(bindType, leftVars, rightFreeVars);
        assertArrayEquals(expected, merger.merge(left, right));
    }

    static Stream<Arguments> testProjection() {
        return Stream.of(
                arguments(Vars.of("x"), Vars.of("x"),
                        singletonList(1), singletonList(1)),
                arguments(Vars.of("x"), Vars.of("y"),
                        singletonList(1), singletonList(null)),

                arguments(Vars.of("x", "y"), Vars.of("x", "y"), asList(1, 2), asList(1, 2)),
                arguments(Vars.of("x", "y"), Vars.of("x", "y"), asList(1, null), asList(1, null)),
                arguments(Vars.of("x", "y"), Vars.of("x", "z"), asList(1, 2), asList(1, null)),
                arguments(Vars.of("x", "y"), Vars.of("x", "z"), asList(null, 2), asList(null, null)),
                arguments(Vars.of("x", "y"), Vars.of("x", "z"),
                        asList(null, null), asList(null, null)),

                arguments(Vars.of("x", "y"), Vars.of("z", "w"), asList(1, 2), asList(null, null)),
                arguments(Vars.of("x", "y"), Vars.of("x"), singletonList(1), asList(1, null)),
                arguments(Vars.of("x", "y"), Vars.of("y"), singletonList(1), asList(null, 1))
        );
    }

    @ParameterizedTest @MethodSource("testProjection")
    void testProjection(Vars outVars, Vars inVars, List<@Nullable Integer> inInts,
                        List<@Nullable Integer> expectedInts) {
        var in = Term.literalList(Term.XSD_INTEGER, inInts);
        var expected = Term.literalList(Term.XSD_INTEGER, expectedInts);
        var projector = RowType.LIST.projector(outVars, inVars);
        var inCopy = new ArrayList<>(in);
        var out = projector.merge(in, null);
        assertEquals(expected, out);
        assertEquals(inCopy, in);
    }

    @ParameterizedTest @MethodSource("testProjection")
    void testProjectionArray(Vars outVars, Vars inVars, List<@Nullable Integer> inList,
                             List<@Nullable Integer> expectedList) {
        var in = Term.literalArray(Term.XSD_INTEGER, inList);
        var expected = Term.literalArray(Term.XSD_INTEGER, expectedList);
        var projector = RowType.ARRAY.projector(outVars, inVars);
        var out = projector.merge(in, null);
        assertArrayEquals(expected, out);
        assertArrayEquals(Term.literalArray(Term.XSD_INTEGER, inList), in);
    }
}