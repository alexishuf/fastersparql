package com.github.alexishuf.fastersparql.client.model.batch.operators;

class ConcatBItTest extends AbstractMergeBItTest {

    @Override protected void run(Scenario scenario) {
        MergeScenario s = (MergeScenario) scenario;
        try (var bit = new ConcatBIt<>(s.operands().iterator(), Integer.class)) {
            s.drainer().drainOrdered(bit, s.expected(), s.error());
        }
    }
}