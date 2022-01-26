package com.github.alexishuf.fastersparql.operators.plan;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.*;

class PlanHelpers {
    public static void checkBind(@Nullable List<String> vars, int valuesSize) {
        int size = vars == null ? 0 : vars.size();
        if (size != valuesSize) {
            String msg = "vars.size()=" + size + " != ntValues.size()=" + valuesSize;
            throw new IllegalArgumentException(msg);
        } else if (size > 0) {
            for (String v : vars) {
                if (v == null) throw new IllegalArgumentException("null in vars=" + vars);
                if (v.isEmpty()) throw new IllegalArgumentException("empty string in vars=" + vars);
            }
        }
    }

    public static <R> Plan<R> bindWithMap(Plan<R> plan, List<String> vars, List<String> ntValues) {
        checkBind(vars, ntValues.size());
        if (vars.isEmpty())
            return plan;
        Map<String, String> map = new HashMap<>();
        for (int i = 0, varsSize = vars.size(); i < varsSize; i++)
            map.put(vars.get(i), ntValues.get(i));
        return plan.bind(map);
    }

    public static <R> Plan<R> bindWithMap(Plan<R> plan, List<String> vars, String[] ntValues) {
        checkBind(vars, ntValues.length);
        if (vars.isEmpty())
            return plan;
        Map<String, String> map = new HashMap<>();
        for (int i = 0, varsSize = vars.size(); i < varsSize; i++)
            map.put(vars.get(i), ntValues[i]);
        return plan.bind(map);
    }

    public static <R> Plan<R> bindWithList(Plan<R> plan, List<String> vars, String[] ntValues) {
        return plan.bind(vars, Arrays.asList(ntValues));
    }

    public static <R> Plan<R> bindWithList(Plan<R> plan, Map<String, String> var2ntValue) {
        int size = var2ntValue.size();
        if (size == 0)
            return plan;
        List<String> vars = new ArrayList<>(size), values = new ArrayList<>(size);
        for (Map.Entry<String, String> e : var2ntValue.entrySet()) {
            vars.add(e.getKey());
            values.add(e.getValue());
        }
        return plan.bind(vars, values);
    }

    public static <R> Plan<R> bindWithArray(Plan<R> plan,
                                            List<String> vars, List<String> ntValues) {
        return plan.bind(vars, ntValues.toArray(new String[0]));
    }

    public static <R> Plan<R> bindWithArray(Plan<R> plan, Map<String, String> var2ntValue) {
        int size = var2ntValue.size(), i = 0;
        if (size == 0)
            return plan;
        List<String> vars = new ArrayList<>(size);
        String[] values = new String[size];
        for (Map.Entry<String, String> e : var2ntValue.entrySet()) {
            vars.add(e.getKey());
            values[i++] = e.getValue();
        }
        return plan.bind(vars, values);
    }

    public static <R> List<Plan<R>> bindAll(List<Plan<R>>  plans,
                                            List<String> vars, String[] ntValues) {
        checkBind(vars, ntValues.length);
        List<Plan<R>> bound = new ArrayList<>(plans.size());
        for (Plan<R> plan : plans) bound.add(plan.bind(vars, ntValues));
        return bound;
    }

    public static <R> List<Plan<R>> bindAll(List<Plan<R>>  plans,
                                            List<String> vars, List<String> ntValues) {
        checkBind(vars, ntValues.size());
        List<Plan<R>> bound = new ArrayList<>(plans.size());
        for (Plan<R> plan : plans) bound.add(plan.bind(vars, ntValues));
        return bound;
    }

    public static <R> List<Plan<R>> bindAll(List<Plan<R>>  plans,
                                            Map<String, String> var2ntValue) {
        List<Plan<R>> bound = new ArrayList<>(plans.size());
        for (Plan<R> plan : plans) bound.add(plan.bind(var2ntValue));
        return bound;
    }
}
