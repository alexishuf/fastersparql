package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.sparql.expr.FinalTerm;

public final class TermBatchType extends ObjBatchType<FinalTerm, TermBatch> {
    public static final TermBatchType TERM = new TermBatchType();

    @SuppressWarnings("SameReturnValue") public static TermBatchType get() { return TERM; }

    private TermBatchType() {
        super(TermBatch.class, FinalTerm.class, TermBatchCleaner.INSTANCE.newInstance,
              TermBatchCleaner.INSTANCE.clearElseMake, TermBatch.BYTES);
        TermBatchCleaner.INSTANCE.pool = this.pool;
    }

}
