package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.client.util.sparql.Binding;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

public class PlanHelpers {

    public static <R> List<String> publicVarsUnion(List<? extends Plan<R>> plans) {
        LinkedHashSet<String> set = new LinkedHashSet<>(16 * plans.size());
        for (Plan<R> plan : plans) set.addAll(plan.publicVars());
        return new ArrayList<>(set);
    }

    public static <R> List<String> allVarsUnion(List<? extends Plan<R>> plans) {
        LinkedHashSet<String> set = new LinkedHashSet<>(16 * plans.size());
        for (Plan<R> plan : plans) set.addAll(plan.allVars());
        return new ArrayList<>(set);
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
