package com.github.alexishuf.fastersparql.client.util;

import com.github.alexishuf.fastersparql.client.BindType;
import com.github.alexishuf.fastersparql.client.model.row.RowOperations;
import com.github.alexishuf.fastersparql.client.model.row.impl.ArrayOperations;
import com.github.alexishuf.fastersparql.client.model.row.impl.ListOperations;
import com.github.alexishuf.fastersparql.client.model.row.impl.StringArrayOperations;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.client.BindType.*;
import static com.github.alexishuf.fastersparql.client.util.Merger.forProjection;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class MergerTest {

    private final StringArrayOperations ROW_OPS = StringArrayOperations.get();

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
                asList(l.get(0).toString().split(",")), // leftVars
                asList(l.get(1).toString().split(",")), // rightVars
                l.get(2),                                     // bindType
                l.get(3).toString().split(","),         // left
                l.get(4).equals("") ? new String[0] : l.get(4).toString().split(","), // right
                Arrays.stream(l.get(5).toString().split(","))
                      .map(s -> s.equals("null") ? null : s).toArray(String[]::new)  // expected
        ));
    }

    @ParameterizedTest @MethodSource
    void testMerge(List<String> leftVars, List<String> rightVars, BindType bindType,
                   String[] left, String[] right, String[] expected) {
        List<String> rightFreeVars = Merger.rightFreeVars(leftVars, rightVars);
        Merger<String[]> merger = Merger.forMerge(ROW_OPS, leftVars, rightFreeVars, bindType);
        assertArrayEquals(expected, merger.merge(left, right));
    }

    static Stream<Arguments> testProjection() {
        return Stream.of(
                arguments(singletonList("x"), singletonList("x"),
                        singletonList(1), singletonList(1)),
                arguments(singletonList("x"), singletonList("y"),
                        singletonList(1), singletonList(null)),

                arguments(asList("x", "y"), asList("x", "y"), asList(1, 2), asList(1, 2)),
                arguments(asList("x", "y"), asList("x", "y"), asList(1, null), asList(1, null)),
                arguments(asList("x", "y"), asList("x", "z"), asList(1, 2), asList(1, null)),
                arguments(asList("x", "y"), asList("x", "z"), asList(null, 2), asList(null, null)),
                arguments(asList("x", "y"), asList("x", "z"),
                        asList(null, null), asList(null, null)),

                arguments(asList("x", "y"), asList("z", "w"), asList(1, 2), asList(null, null)),
                arguments(asList("x", "y"), singletonList("x"), singletonList(1), asList(1, null)),
                arguments(asList("x", "y"), singletonList("y"), singletonList(1), asList(null, 1))
        );
    }

    @ParameterizedTest @MethodSource("testProjection")
    void testProjection(List<String> outVars, List<String> inVars, List<@Nullable Integer> in,
                        List<@Nullable Integer> expected) {
        Merger<List<Integer>> projector = forProjection(ListOperations.get(), outVars, inVars);
        List<@Nullable Integer> inCopy = new ArrayList<>(in);
        List<@Nullable Integer> out = projector.merge(in, null);
        assertEquals(expected, out);
        assertEquals(inCopy, in);
    }

    @ParameterizedTest @MethodSource("testProjection")
    void testProjectionArray(List<String> outVars, List<String> inVars, List<@Nullable Integer> inList,
                             List<@Nullable Integer> expectedList) {
        @Nullable Integer[] in = inList.toArray(new Integer[0]);
        @Nullable Integer[] expected = expectedList.toArray(new Integer[0]);

        RowOperations rowOps = ArrayOperations.PROVIDER.get(Integer[].class);
        Merger<Integer[]> projector = forProjection(rowOps, outVars, inVars);
        @Nullable Integer[] out = projector.merge(in, null);
        assertArrayEquals(expected, out);
        assertArrayEquals(inList.toArray(new Integer[0]), in);
    }
}