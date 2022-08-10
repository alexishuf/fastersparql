package com.github.alexishuf.fastersparql.operators.impl;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.model.row.RowOperations;
import com.github.alexishuf.fastersparql.operators.BidCosts;
import com.github.alexishuf.fastersparql.operators.OperatorFlags;
import com.github.alexishuf.fastersparql.operators.Project;
import com.github.alexishuf.fastersparql.operators.errors.IllegalOperatorArgumentException;
import com.github.alexishuf.fastersparql.operators.plan.ProjectPlan;
import com.github.alexishuf.fastersparql.operators.providers.ProjectProvider;
import org.checkerframework.checker.index.qual.NonNegative;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

public final class SimpleProject implements Project {
    private final RowOperations rowOperations;

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

    public SimpleProject(RowOperations rowOperations) {
        this.rowOperations = rowOperations;
    }


    @Override public <R> Class<R> rowClass() {
        //noinspection unchecked
        return (Class<R>) rowOperations.rowClass();
    }

    @Override public <R> Results<R> checkedRun(ProjectPlan<R> plan) {
        List<String> vars = plan.publicVars();
        for (String v : vars) {
            if (v == null)
                throw new IllegalOperatorArgumentException("null var name in vars=" + vars);
        }
        LinkedHashSet<String> varsSet = new LinkedHashSet<>(vars);
        if (varsSet.size() < vars.size())
            vars = new ArrayList<>(varsSet);
        Results<R> input = plan.input().execute();
        ProjectingProcessor<R> processor
                = new ProjectingProcessor<>(input, vars, rowOperations, plan);
        return new Results<>(vars, input.rowClass(), processor);
    }

}
