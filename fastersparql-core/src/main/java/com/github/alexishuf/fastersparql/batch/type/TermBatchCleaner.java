package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.batch.type.TermBatch.Concrete;
import com.github.alexishuf.fastersparql.sparql.expr.FinalTerm;
import com.github.alexishuf.fastersparql.util.concurrent.CleanerBackgroundTask;
import com.github.alexishuf.fastersparql.util.owned.SpecialOwner;

import java.util.function.Supplier;

import static com.github.alexishuf.fastersparql.batch.type.BatchType.PREFERRED_BATCH_TERMS;

class TermBatchCleaner
        extends CleanerBackgroundTask<TermBatch>
        implements SpecialOwner.Recycled {
    public static final TermBatchCleaner INSTANCE = new TermBatchCleaner();

    static final class NewTermBatchFactory implements Supplier<TermBatch> {
        @Override public TermBatch get() {
            var terms = new FinalTerm[PREFERRED_BATCH_TERMS];
            return new Concrete(terms, 0, 1).takeOwnership(RECYCLED);
        }
        @Override public String toString() {return "NewTermBatchFactory";}
    }

    public TermBatchCleaner() {super("TermBatchCleaner", new NewTermBatchFactory());}

    @Override public String journalName() {return "TermBatchCleaner";}

    public void sched(TermBatch node, Object currentOwner) {
        sched(node.transferOwnership(currentOwner, this));
    }

    @Override protected void clear(TermBatch b) {b.clearAndMarkRecycled(this);}

    @Override protected void handle(TermBatch o) {o.doRecycleToShared(this);}
}
