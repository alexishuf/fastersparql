package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.client.util.sparql.Binding;

import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.emptyList;

public class PlanHelpers {

    public static <T> boolean addUnique(List<T> destination, List<T> values) {
        int oldSize = destination.size();
        for (T v : values) {
            if (!destination.contains(v))
                destination.add(v);
        }
        return destination.size() > oldSize;
    }

    public static <R> List<String> publicVarsUnion(List<? extends Plan<R>> plans) {
        int size = plans.size(), changes = 0;
        if (size == 0)
            return emptyList();
        List<String> first = plans.get(0).publicVars();
        if (size > 1) {
            List<String> all = new ArrayList<>(8 * size);
            for (Plan<R> plan : plans)
                changes += addUnique(all, plan.publicVars()) ? 1 : 0;
            return changes == 1 ? first : all;
        }
        return first;
    }

    public static <R> List<String> allVarsUnion(List<? extends Plan<R>> plans) {
        int size = plans.size();
        if (size == 0)
            return emptyList();
        List<String> first = plans.get(0).allVars();
        if (size > 1) {
            List<String> all = new ArrayList<>(first.size() * size);
            int changes = 0;
            for (Plan<R> plan : plans)
                changes += addUnique(all, plan.allVars()) ? 1 : 0;
            return changes == 1 ? first : all;
        }
        return first;
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
