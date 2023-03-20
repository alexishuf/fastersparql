package com.github.alexishuf.fastersparql.batch.operators;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.model.Vars;


class ConcatBItTest extends AbstractMergeBItTest {

    @Override protected void run(Scenario scenario) {
        MergeScenario s = (MergeScenario) scenario;
        try (var bit = new ConcatBIt<>(s.operands(), Batch.TERM, Vars.of("x"))) {
            s.drainer().drainOrdered(bit, s.expectedInts(), s.error());
        }
    }
}