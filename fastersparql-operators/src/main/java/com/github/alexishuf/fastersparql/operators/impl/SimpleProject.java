package com.github.alexishuf.fastersparql.operators.impl;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.util.reactive.AbstractProcessor;
import com.github.alexishuf.fastersparql.client.util.sparql.VarUtils;
import com.github.alexishuf.fastersparql.operators.BidCosts;
import com.github.alexishuf.fastersparql.operators.OperatorFlags;
import com.github.alexishuf.fastersparql.operators.Project;
import com.github.alexishuf.fastersparql.operators.errors.IllegalOperatorArgumentException;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import com.github.alexishuf.fastersparql.operators.providers.ProjectProvider;
import com.github.alexishuf.fastersparql.operators.row.RowOperations;
import lombok.Value;
import org.checkerframework.checker.index.qual.NonNegative;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

@Value
public class SimpleProject implements Project {
    RowOperations rowOperations;

    public static class Provider implements ProjectProvider {
        @Override public @NonNegative int bid(long flags) {
            int bid = BidCosts.BUILTIN_COST;
            if ((flags & OperatorFlags.ASYNC) != 0) bid += BidCosts.MINOR_COST;
            return bid;
        }

        @Override public Project create(long flags, RowOperations rowOperations) {
            return new SimpleProject(rowOperations);
        }
    }

    @Override public <R> Results<R> checkedRun(Plan<R> inputPlan, List<String> vars) {
        for (String v : vars) {
            if (v == null)
                throw new IllegalOperatorArgumentException("null var name in vars=" + vars);
        }
        LinkedHashSet<String> varsSet = new LinkedHashSet<>(vars);
        if (varsSet.size() < vars.size())
            vars = new ArrayList<>(varsSet);
        Results<R> input = inputPlan.execute();
        return new Results<>(vars, input.rowClass(),
                             new ProjectingProcessor<>(input, vars, rowOperations));
    }

    private static final class ProjectingProcessor<T> extends AbstractProcessor<T, T> {
        private final List<String> outVars;
        private final RowOperations rowOps;
        private final int[] indices;
        private final List<String> inVars;

        public ProjectingProcessor(Results<? extends T> source, List<String> outVars,
                                   RowOperations rowOps) {
            super(source.publisher());
            this.outVars = outVars;
            this.rowOps = rowOps;
            this.inVars = source.vars();
            this.indices = VarUtils.projectionIndices(outVars, inVars);
        }

        @Override protected void handleOnNext(T item) {
            @SuppressWarnings("unchecked")
            T row = (T)rowOps.createEmpty(outVars);
            for (int i = 0; i < indices.length; i++) {
                int inIdx = indices[i];
                if (inIdx >= 0)
                    rowOps.set(row, i, outVars.get(i), rowOps.get(item, inIdx, inVars.get(inIdx)));
            }
            emit(row);
        }
    }
}
