package com.github.alexishuf.fastersparql.client.util.sparql;

import com.github.alexishuf.fastersparql.client.model.Results;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.*;

public class VarUtils {
    /**
     * Check if {@code map} maps valid variable names to either null or non-empty variable values.
     *
     * @param map the {@link Map} to check
     */
    public static void checkBind(@Nullable Map<String, String> map) {
        if (map == null || map.isEmpty())
            return;
        for (Map.Entry<String, String> e : map.entrySet()) {
            String name = e.getKey(), value = e.getValue();
            if (name == null)
                throw new IllegalArgumentException("Null var name (key) in "+map);
            if (name.isEmpty())
                throw new IllegalArgumentException("Empty var name (key) in "+map);
            char c = name.charAt(0);
            if (c == '?' || c == '$')
                throw new IllegalArgumentException("Var name cannot start with ? or $ "+map);
            if (value != null && value.isEmpty())
                throw new IllegalArgumentException("Empty string is not a valid NT term. map="+map);
        }
    }

    /**
     * Check whether {@code vars, values} is valid input for {@link VarUtils#toMap(List, String[])}
     * or {@link VarUtils#toMap(List, List)}.
     *
     * @param vars the list of variable names
     * @param values the list or array of values for each variable.
     */
    public static void checkBind(@Nullable List<String> vars, @Nullable Object values) {
        int valuesSize;
        if (values instanceof List)
            valuesSize = ((List<?>)values).size();
        else if (values instanceof String[])
            valuesSize = ((String[])values).length;
        else if (values != null)
            throw new IllegalArgumentException("Bad type for values="+values);
        else
            valuesSize = 0;

        if (vars == null && values != null && valuesSize > 0)
            throw new IllegalArgumentException("vars is null but values is not empty");
        if (values == null && vars != null && !vars.isEmpty())
            throw new IllegalArgumentException("values is null but vars is not empty");
        int size = vars == null ? 0 : vars.size();
        if (size != valuesSize) {
            String msg = "vars.size()=" + size + " != ntValues.size()=" + valuesSize;
            throw new IllegalArgumentException(msg);
        } else if (size > 0) {
            for (String v : vars) {
                if (v == null) throw new IllegalArgumentException("null in vars=" + vars);
                if (v.isEmpty()) throw new IllegalArgumentException("empty string in vars=" + vars);
                char c = v.charAt(0);
                if (c == '?' || c == '$')
                    throw new IllegalArgumentException("Var name starting with ? or $. vars="+vars);
            }
        }
    }

    /**
     * Convert a list of var names and values into a map from name to value.
     *
     * @param varNames distinct list of non-null variable names.
     * @param ntValues list of values. The i-th value corresponds to the i-th var
     * @return a {@link Map} mapping each var name to its value.
     */
    public static Map<String, String> toMap(List<String> varNames, List<String> ntValues) {
        checkBind(varNames, ntValues);
        if (varNames == null || varNames.isEmpty())
            return Collections.emptyMap();
        Map<String, String> map = new HashMap<>();
        for (int i = 0, size = varNames.size(); i < size; i++) {
            String v = ntValues.get(i);
            if (v != null && v.isEmpty()) {
                throw new IllegalArgumentException("Empty string (assigned to "+varNames.get(i)+
                                                   " is not a valid NT term");
            }
            map.put(varNames.get(i), v);
        }
        return map;
    }

    /**
     * Convert a list of var names and values into a map from name to value.
     *
     * @param varNames distinct list of non-null variable names.
     * @param ntValues array of values. The i-th value corresponds to the i-th var
     * @return a {@link Map} mapping each var name to its value.
     */
    public static Map<String, String> toMap(List<String> varNames, String[] ntValues) {
        checkBind(varNames, ntValues);
        if (varNames == null || varNames.isEmpty())
            return Collections.emptyMap();
        Map<String, String> map = new HashMap<>();
        for (int i = 0, size = varNames.size(); i < size; i++) {
            String v = ntValues[i];
            if (v != null && v.isEmpty()) {
                throw new IllegalArgumentException("Empty string (assigned to "+varNames.get(i)+
                                                   " is not a valid NT term");
            }
            map.put(varNames.get(i), v);
        }
        return map;
    }

    /**
     * Create distinct list of all variables in the given {{@link Results}} retaining the order
     * of the {@link Results} objects and of the {@link Results#vars()} lists.
     *
     * @param results the list of {@link Results} objects to get the variable lists from.
     * @return a non-null, list of non-null variable names preserving the order of inputs.
     */
    public static <R> List<String> union(List<Results<R>> results) {
        LinkedHashSet<String> set = new LinkedHashSet<>(results.size() * 16);
        for (Results<?> r : results) set.addAll(r.vars());
        return new ArrayList<>(set);
    }

    /**
     * Create a distinct list of all variables in {@code left} and {@code right}, preserving
     * the order in which the variables appear (left comes first).
     *
     * @param left first list of variables. If {@code null} will treat as empty.
     * @param right second list of variables. If {@code null} will treat as empty
     * @return a non-null, list of non-null variable names preserving the order of inputs.
     */
    public static List<String> union(List<String> left, List<String> right) {
        int sum = left.size() + right.size();
        LinkedHashSet<String> set = new LinkedHashSet<>(sum + (sum/2 + 1));
        set.addAll(left);
        set.addAll(right);
        return new ArrayList<>(set);
    }

    /***
     * Create an {@code int[]} where the i-th element is the index of the i-th {@code outVar}
     * in {@code inVars}, or -1 if it is not found.
     *
     * @param outVars the list of output variables
     * @param inVars the list of input (i.e., available) variables.
     * @return an array with the indices of {@code outVars} within {@code inVars}.
     */
    public static int[] projectionIndices(List<String> outVars, List<String> inVars) {
        int[] indices = new int[outVars.size()];
        for (int i = 0; i < indices.length; i++)
            indices[i] = inVars.indexOf(outVars.get(i));
        return indices;
    }
}
