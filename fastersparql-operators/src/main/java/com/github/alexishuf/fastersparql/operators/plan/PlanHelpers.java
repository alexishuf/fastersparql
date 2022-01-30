package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.client.util.sparql.VarUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

class PlanHelpers {

    public static <R> List<Plan<R>> bindAll(List<Plan<R>>  plans,
                                            List<String> vars, String[] ntValues) {
        VarUtils.checkBind(vars, ntValues);
        List<Plan<R>> bound = new ArrayList<>(plans.size());
        for (Plan<R> plan : plans) bound.add(plan.bind(vars, ntValues));
        return bound;
    }

    public static <R> List<Plan<R>> bindAll(List<Plan<R>>  plans,
                                            List<String> vars, List<String> ntValues) {
        VarUtils.checkBind(vars, ntValues);
        List<Plan<R>> bound = new ArrayList<>(plans.size());
        for (Plan<R> plan : plans) bound.add(plan.bind(vars, ntValues));
        return bound;
    }

    public static <R> List<Plan<R>> bindAll(List<Plan<R>>  plans,
                                            Map<String, String> var2ntValue) {
        VarUtils.checkBind(var2ntValue);
        List<Plan<R>> bound = new ArrayList<>(plans.size());
        for (Plan<R> plan : plans) bound.add(plan.bind(var2ntValue));
        return bound;
    }
}
