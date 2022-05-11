package com.github.alexishuf.fastersparql.client.util;

import com.github.alexishuf.fastersparql.client.BindType;
import com.github.alexishuf.fastersparql.client.model.row.impl.StringArrayOperations;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.client.BindType.*;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
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
        Merger<String[]> merger = new Merger<>(ROW_OPS, leftVars, "",
                                               rightVars, rightVars, bindType);
        assertArrayEquals(expected, merger.merge(left, right));
    }
}