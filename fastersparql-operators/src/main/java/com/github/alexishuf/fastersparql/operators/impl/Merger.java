package com.github.alexishuf.fastersparql.operators.impl;

import com.github.alexishuf.fastersparql.client.util.sparql.VarUtils;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import com.github.alexishuf.fastersparql.operators.row.RowOperations;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.*;

public class Merger<R> {
    private final RowOperations rowOps;
    private final Plan<R> right;
    private final List<String> outVars;
    private final List<String> joinVars;
    private final int mapCapacity;
    private final int[] mergeIndices;
    private final int[] bindIndices;

    public Merger(RowOperations rowOps, List<String> leftPublicVars, Plan<R> right) {
        this(rowOps, leftPublicVars, right, VarUtils.union(leftPublicVars, right.publicVars()));
    }

    public Merger(RowOperations rowOps, List<String> leftPublicVars, Plan<R> right,
                  List<String> outVars) {
        this.rowOps = rowOps;
        this.right = right;
        this.outVars = outVars;
        Collection<String> rightVars = right.allVars();
        List<String> rightUnboundVars = new ArrayList<>(rightVars.size());
        this.joinVars = new ArrayList<>(leftPublicVars.size());
        for (String name : rightVars) {
            if (leftPublicVars.contains(name))
                joinVars.add(name);
            else
                rightUnboundVars.add(name);
        }
        int joinVarsSize = joinVars.size();
        this.bindIndices = new int[joinVarsSize];
        for (int i = 0; i < bindIndices.length; i++)
            bindIndices[i] = leftPublicVars.indexOf(joinVars.get(i));
        this.mapCapacity = joinVarsSize < 3 ? 4 : (int)((float)joinVarsSize / 0.75f + 1.0);
        this.mergeIndices = new int[outVars.size()];
        for (int i = 0; i < mergeIndices.length; i++) {
            String name = outVars.get(i);
            int idx = leftPublicVars.indexOf(name);
            if (idx >= 0)
                mergeIndices[i] = -1 * (idx + 1);
            else
                mergeIndices[i] = rightUnboundVars.indexOf(name) + 1;
        }
    }

    public List<String> outVars() { return outVars; }

    public boolean isProduct() { return joinVars.isEmpty(); }

    public List<String> joinVars() { return joinVars; }

    public Plan<R> bind(@Nullable R leftRow) {
        Map<String, String> var2nt = new HashMap<>(this.mapCapacity);
        for (int i = 0, size = joinVars.size(); i < size; i++) {
            String name = joinVars.get(i);
            var2nt.put(name, rowOps.getNT(leftRow, bindIndices[i], name));
        }
        return right.bind(var2nt);
    }

    public R merge(@Nullable R left, @Nullable R right) {
        Object row = rowOps.createEmpty(outVars);
        for (int i = 0; i < mergeIndices.length; i++) {
            String name = outVars.get(i);
            int composite = mergeIndices[i];
            R src = composite < 0 ? left : right;
            rowOps.set(row, i, name, rowOps.get(src, Math.abs(composite)-1, name));
        }
        //noinspection unchecked
        return (R)row;
    }
}
