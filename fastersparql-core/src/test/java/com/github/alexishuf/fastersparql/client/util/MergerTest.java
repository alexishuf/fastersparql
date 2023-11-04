package com.github.alexishuf.fastersparql.client.util;

import com.github.alexishuf.fastersparql.batch.type.TermBatch;
import com.github.alexishuf.fastersparql.model.BindType;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.batch.type.TermBatchType.TERM;
import static com.github.alexishuf.fastersparql.model.BindType.*;
import static com.github.alexishuf.fastersparql.sparql.expr.Term.termList;
import static java.util.Arrays.asList;
import static java.util.stream.IntStream.range;
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
                termList((Object[]) l.get(3).toString().split(",")), // left
                termList((Object[]) (l.get(4).equals("") ? new String[0] : l.get(4).toString().split(","))), // right
                termList((Object[]) l.get(5).toString().split(","))  // expected
        ));
    }

    @ParameterizedTest @MethodSource
    void testMerge(Vars leftVars, Vars rightVars, BindType bindType,
                   List<Term> left, List<Term> right, List<Term> expected) {
        TermBatch lb = TermBatch.of(range(0, left.size()).mapToObj(i -> (Term)null).toList(), left);
        int lr = 1;
        TermBatch rb = TermBatch.of(right);
        TermBatch eb = TermBatch.of(expected);

        Vars rightFreeVars = rightVars.minus(leftVars);
        Vars outVars = bindType.resultVars(leftVars, rightVars);
        var merger = TERM.merger(outVars, leftVars, rightFreeVars);
        TermBatch ac = merger.merge(null, lb, lr, rb);
        assertEquals(eb, ac);
        ac.recycle();
    }
}