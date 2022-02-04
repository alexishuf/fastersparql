package com.github.alexishuf.fastersparql.operators.impl;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.util.reactive.AbstractProcessor;
import com.github.alexishuf.fastersparql.client.util.sparql.VarUtils;
import com.github.alexishuf.fastersparql.operators.row.RowOperations;
import org.reactivestreams.Publisher;

import java.util.List;

public final class ProjectingProcessor<T> extends AbstractProcessor<T, T> {
    private final List<String> outVars, inVars;
    private final RowOperations rowOps;
    private final int[] indices;

    public ProjectingProcessor(Results<? extends T> source, List<String> outVars,
                               RowOperations rowOps) {
        this(source.publisher(), outVars, source.vars(), rowOps);
    }
    public ProjectingProcessor(Publisher<? extends T> source, List<String> outVars,
                               List<String> inVars, RowOperations rowOps) {
        super(source);
        this.outVars = outVars;
        this.inVars = inVars;
        this.rowOps = rowOps;
        this.indices = VarUtils.projectionIndices(outVars, inVars);
    }

    @Override protected void handleOnNext(T item) {
        @SuppressWarnings("unchecked")
        T row = (T) rowOps.createEmpty(outVars);
        for (int i = 0; i < indices.length; i++) {
            int inIdx = indices[i];
            if (inIdx >= 0)
                rowOps.set(row, i, outVars.get(i), rowOps.get(item, inIdx, inVars.get(inIdx)));
        }
        emit(row);
    }
}
