package com.github.alexishuf.fastersparql.batch.operators;

import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.row.NotRowType;

class ConcatBItTest extends AbstractMergeBItTest {

    @Override protected void run(Scenario scenario) {
        MergeScenario s = (MergeScenario) scenario;
        try (var bit = new ConcatBIt<>(s.operands(), NotRowType.INTEGER, Vars.EMPTY)) {
            s.drainer().drainOrdered(bit, s.expected(), s.error());
        }
    }
}