package com.github.alexishuf.fastersparql.batch;

import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static com.github.alexishuf.fastersparql.batch.IntsBatch.intsBatch;
import static com.github.alexishuf.fastersparql.batch.type.Batch.TERM;
import static com.github.alexishuf.fastersparql.model.rope.SharedRopes.DT_integer;
import static org.junit.jupiter.api.Assertions.assertEquals;

class SingletonBItTest {
    @ParameterizedTest @ValueSource(ints = {1, 2, 3})
    void test(int min) {
        var bath = TERM.createSingleton(1).putRow(new Term[]{Term.typed(23, DT_integer)});
        try (var it = new SingletonBIt<>(bath, TERM, Vars.of("x"))) {
            assertEquals(intsBatch(23), it.minBatch(min).nextBatch(null));
        }
    }
}