package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.client.util.sparql.Binding;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static java.util.Collections.emptyList;

public class PlanHelpers {

    private static List<String> varsUnion(List<? extends Plan<?>> plans,
                                          Function<Plan<?>, List<String>> varsGetter) {
        int nPlans = plans.size();
        if (nPlans < 2)
            return nPlans == 0 ? emptyList() : varsGetter.apply(plans.get(0));
        List<String> first = varsGetter.apply(plans.get(0));
        ArrayList<String> all = new ArrayList<>(Math.min(10, first.size() * nPlans));
        all.addAll(first);
        boolean changed = false;
        for (int i = 1; i < nPlans; i++) {
            for (String v : varsGetter.apply(plans.get(i))) {
                if (!all.contains(v)) {
                    all.add(v);
                    changed = true;
                }
            }
        }
        return changed ? all : first;
    }

    public static List<String> publicVarsUnion(List<? extends Plan<?>> plans) {
        return varsUnion(plans, Plan::publicVars);
    }

    public static List<String> allVarsUnion(List<? extends Plan<?>> plans) {
        return varsUnion(plans, Plan::allVars);
    }

    public static <R> List<Plan<R>> bindAll(List<? extends Plan<R>>  plans,
                                            Binding binding) {
        List<Plan<R>> boundList = new ArrayList<>(plans.size());
        boolean change = false;
        for (Plan<R> plan : plans) {
            Plan<R> bound = plan.bind(binding);
            change |= bound != plan;
            boundList.add(bound);
        }
        //noinspection unchecked
        return change ? boundList : (List<Plan<R>>) plans;
    }
}
