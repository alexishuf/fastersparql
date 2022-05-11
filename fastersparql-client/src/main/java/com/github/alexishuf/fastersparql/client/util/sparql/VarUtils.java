package com.github.alexishuf.fastersparql.client.util.sparql;

import com.github.alexishuf.fastersparql.client.model.Results;

import java.util.*;

public class VarUtils {
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
     * Tests whether two sets of variables have at least one intersecting variable.
     *
     * @param a one set of variable names
     * @param b another set of variable names
     * @return true if {@code a} and {@code b} intersect.
     */
    public static boolean hasIntersection(Collection<String> a, Collection<String> b) {
        if (a instanceof Set) {
            for (String name : b) if (a.contains(name)) return true;
        } else {
            for (String name : a) if (b.contains(name)) return true;
        }
        return false;
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
