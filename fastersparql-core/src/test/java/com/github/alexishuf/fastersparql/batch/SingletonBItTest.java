package com.github.alexishuf.fastersparql.batch;

import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.util.owned.Guard;
import com.github.alexishuf.fastersparql.util.owned.Guard.ItGuard;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static com.github.alexishuf.fastersparql.batch.IntsBatch.intsBatch;
import static com.github.alexishuf.fastersparql.batch.type.TermBatchType.TERM;
import static org.junit.jupiter.api.Assertions.assertEquals;

class SingletonBItTest {
    private static final Vars X = Vars.of("x");

    @ParameterizedTest @ValueSource(ints = {1, 2, 3})
    void test(int min) {
        try (var expected = new Guard.BatchGuard<>(intsBatch(23), this);
             var g = new ItGuard<>(this, new SingletonBIt<>(intsBatch(23), TERM, X))) {
            g.it.minBatch(min);
            assertEquals(expected.get(), g.nextBatch());
        }
    }
}