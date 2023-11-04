package com.github.alexishuf.fastersparql.batch.operators;

import com.github.alexishuf.fastersparql.batch.type.TermBatchType;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal;


class ConcatBItTest extends AbstractMergeBItTest {

    @Override protected void run(Scenario scenario) {
        MergeScenario s = (MergeScenario) scenario;
        try (var bit = new ConcatBIt<>(s.operands(), TermBatchType.TERM, Vars.of("x"))) {
            s.drainer().drainOrdered(bit, s.expectedInts(), s.error());
        } catch (Throwable t) {
            ThreadJournal.dumpAndReset(System.out, 100);
            throw t;
        }
    }
}