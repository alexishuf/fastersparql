package com.github.alexishuf.fastersparql.operators.row;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.TermBatch;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.Rope;
import com.github.alexishuf.fastersparql.sparql.binding.BatchBinding;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.sparql.expr.Term.termList;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class BatchBindingTest {

    @SuppressWarnings("unused") static Stream<Arguments> test() {
        record D(Vars vars, List<Term> terms) {}
        List<D> list = List.of(
                new D(Vars.EMPTY, termList()),
                new D(Vars.of("x"), termList("<x>")),
                new D(Vars.of("x"), termList("\"23\"")),
                new D(Vars.of("x"), termList("_:b1")),
                new D(Vars.of("x", "y"), termList("<a>", "<b>")),
                new D(Vars.of("y", "x"), termList("<b>", "<a>")),
                new D(Vars.of("x", "y"), termList("<a>", null)),
                new D(Vars.of("x", "y"), termList(null, "<b>")),
                new D(Vars.of("x", "y", "z1"), termList("<a>", "<b>", "<c>")),
                new D(Vars.of("z1", "x", "y"), termList("<c>", "<a>", "<b>")),
                new D(Vars.of("x", "y", "z1"), termList("<a>", "<b>", null)),
                new D(Vars.of("x", "y", "z1"), termList("<a>", null, "<c>")),
                new D(Vars.of("x", "y", "z1"), termList(null, "<b>", "<c>"))

        );
        return list.stream().map(d -> {
            TermBatch b = Batch.TERM.create(2, d.vars.size(), 0);
            b.beginPut();
            for (int i = 0; i < d.vars.size(); i++) b.putTerm(i, (Term)null);
            b.commitPut();
            b.putRow(d.terms);
            return arguments(d.vars, b);
        });
    }

    @ParameterizedTest @MethodSource("test")
    void testGet(Vars vars, TermBatch batch) {
        BatchBinding<TermBatch> b = new BatchBinding<>(Batch.TERM, vars).setRow(batch, 1);
        for (int c = 0; c < vars.size(); c++) {
            assertEquals(batch.get(1, c), b.get(c), "c="+c);
            assertEquals(batch.get(1, c), b.get(vars.get(c)), "c="+c);
        }
        assertNull(b.get(Rope.of("outside")));
        assertNull(b.get(Term.valueOf("?outside")));
        if (b.size() > 0) {
            assertThrows(IndexOutOfBoundsException.class, () -> b.get(-1));
            assertThrows(IndexOutOfBoundsException.class, () -> b.get(vars.size()));
        }
    }

}