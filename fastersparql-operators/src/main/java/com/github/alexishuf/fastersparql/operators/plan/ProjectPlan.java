package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.operators.Project;
import lombok.Value;
import lombok.experimental.Accessors;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

@Value @Accessors(fluent = true)
public class ProjectPlan<R> implements Plan<R> {
    Project op;
    Plan<R> input;
    List<String> vars;

    @Override public Results<R> execute() {
        return op.run(this);
    }

    @Override public List<String> publicVars() {
        return input.publicVars();
    }

    @Override public List<String> allVars() {
        return input.allVars();
    }

    @Override public Plan<R> bind(Map<String, String> var2ntValue) {
        return new ProjectPlan<>(op, input.bind(var2ntValue), removeBound(var2ntValue.keySet()));
    }

    @Override public Plan<R> bind(List<String> vars, List<String> ntValues) {
        return new ProjectPlan<>(op, input.bind(vars, ntValues), removeBound(vars));
    }

    @Override public Plan<R> bind(List<String> vars, String[] ntValues) {
        return new ProjectPlan<>(op, input.bind(vars, ntValues), removeBound(vars));
    }

    private List<String> removeBound(Collection<String> bound) {
        if (bound == null || bound.isEmpty())
            return vars;
        ArrayList<String> remaining = new ArrayList<>();
        for (String name : this.vars) {
            if (!bound.contains(name))
                remaining.add(name);
        }
        return remaining;
    }
}
