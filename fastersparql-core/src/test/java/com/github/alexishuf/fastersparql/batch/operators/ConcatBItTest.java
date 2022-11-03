package com.github.alexishuf.fastersparql.batch.operators;

import com.github.alexishuf.fastersparql.client.model.Vars;

class ConcatBItTest extends AbstractMergeBItTest {

    @Override protected void run(Scenario scenario) {
        MergeScenario s = (MergeScenario) scenario;
        try (var bit = new ConcatBIt<>(s.operands(), Integer.class, Vars.EMPTY)) {
            s.drainer().drainOrdered(bit, s.expected(), s.error());
        }
    }
}