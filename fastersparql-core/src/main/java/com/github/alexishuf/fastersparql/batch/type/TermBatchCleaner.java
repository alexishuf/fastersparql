package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.sparql.expr.FinalTerm;

import java.util.function.Supplier;

import static com.github.alexishuf.fastersparql.batch.type.BatchType.PREFERRED_BATCH_TERMS;

class TermBatchCleaner
        extends ObjBatchCleaner<TermBatch> {
    public static final TermBatchCleaner INSTANCE = new TermBatchCleaner();

    static final class NewTermBatchFactory implements Supplier<TermBatch> {
        @Override public TermBatch get() {
            var terms = new FinalTerm[PREFERRED_BATCH_TERMS];
            return new TermBatch.Concrete(terms, 0, 1).takeOwnership(RECYCLED);
        }
        @Override public String toString() {return "NewTermBatchFactory";}
    }

    public TermBatchCleaner() {super("TermBatchCleaner", new NewTermBatchFactory());}
}
