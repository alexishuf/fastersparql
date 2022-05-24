package com.github.alexishuf.fastersparql.client.util;

import com.github.alexishuf.fastersparql.client.BindType;
import com.github.alexishuf.fastersparql.client.model.row.RowOperations;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class Merger<R> {
    private final RowOperations rowOps;
    private final List<String> outVars;
    private final int @Nullable[] sources;

    private Merger(RowOperations rowOps, List<String> outVars, int @Nullable [] sources) {
        this.rowOps = rowOps;
        this.outVars = outVars;
        this.sources = sources;
    }

    /**
     * Create a merger for use when merging a left and a right row where the right
     * involved in the given {@link BindType}.
     *
     * @param rowOps The {@link RowOperations} for {@code R}
     * @param leftPublicVars the vars in {@code left} rows to be given in
     *                       {@link Merger#merge(Object, Object)}. This list may be held by
     *                       reference and must not change.
     * @param rightVars the vars of the {@code right} row to be given in future
     *                  {@link Merger#merge(Object, Object)} calls
     * @param bindType the type of bind operation that will generate the rows fed to
     *                 {@link Merger#merge(Object, Object)}.
     * @param <R> the type of row
     * @return a new {@link Merger}.
     */
    public static <R> Merger<R> forMerge(RowOperations rowOps, List<String> leftPublicVars,
                                         List<String> rightVars, BindType bindType) {
        int @Nullable [] sources;
        List<String> outVars;
        if (bindType.isJoin()) {
            outVars = new ArrayList<>(leftPublicVars.size() + rightVars.size());
            outVars.addAll(leftPublicVars);
            outVars.addAll(rightVars);
            sources = findSources(outVars, leftPublicVars, rightVars);
        } else {
            outVars = leftPublicVars;
            sources = null;
        }
        return new Merger<>(rowOps, outVars, sources);
    }


    /**
     * Create a {@link Merger} for projection, making {@link Merger#merge(Object, Object)}
     * ignore the {@code right} parameter.
     *
     * @param rowOps the {@link RowOperations} for {@code R}
     * @param outVars the desired list of vars to be output from {@link Merger#merge(Object, Object)}
     * @param inVars the vars of {@code left} rows provided to {@link Merger#merge(Object, Object)}
     * @param <R> the row type.
     * @return a new {@link Merger}.
     */
    public static <R> Merger<R> forProjection(RowOperations rowOps, List<String> outVars,
                                              List<String> inVars) {
        return new Merger<>(rowOps, outVars, findSources(outVars, inVars, Collections.emptyList()));
    }

    /**
     * Create a {@link Merger} for a no-op projection, where {@link Merger#merge(Object, Object)}
     * ignores {@code right} and returns {@code left}, which must have {@code outVars} as variables.
     *
     * @param rowOps {@link RowOperations} for R
     * @param outVars the expected variables for {@code left} in future
     *                {@link Merger#merge(Object, Object)} calls
     * @param <R> the row type
     * @return a new {@link Merger}
     */
    public static <R> Merger<R> identity(RowOperations rowOps, List<String> outVars) {
        return new Merger<>(rowOps, outVars, null);
    }

    /**
     * Get a copy of {@code rightPublicVars} without any vars contained in {@code leftPublicVars}.
     *
     * If the values for the {@code right} parameter in {@link Merger#merge(Object, Object)}
     * will be obtained by binding a plan or query that originally produced {@code rightPublicVars},
     * then this method computes vars in said {@code right} rows by removing vars which will not
     * be present in the {@link Merger#merge(Object, Object)} call due to them being replaced with
     * values from the {@code left} row.
     *
     * @param leftPublicVars the vars of {@code left} rows provided to
     *                       {@link Merger#merge(Object, Object)}.
     * @param rightPublicVars the vars in the right-side operand before the bind operation.
     * @return {@code rightPublicVars} sans vars in {@code leftPublicVars} the
     *         {@code rightPublicVars} reference itself may be returned if no change is necessary.
     */
    public static List<String> rightFreeVars(List<String> leftPublicVars,
                                             List<String> rightPublicVars) {
        ArrayList<String> copy = new ArrayList<>(rightPublicVars);
        boolean change = false;
        for (int i = copy.size()-1; i >= 0; i--) {
            if (leftPublicVars.contains(copy.get(i))) {
                copy.remove(i);
                change = true;
            }
        }
        return change ? copy : rightPublicVars;
    }

    private static int @Nullable [] findSources(List<String> out, List<String> leftPublic,
                                                List<String> rightFree) {
        if (out == leftPublic)
            return null;
        boolean trivial = out.size() == leftPublic.size();
        int[] sources = new int[out.size()];
        for (int i = 0; i < sources.length; i++) {
            String name = out.get(i);
            int idx = leftPublic.indexOf(name);
            trivial &= idx == i;
            sources[i] = idx >= 0 ? idx + 1 : -(rightFree.indexOf(name) +1);
        }
        return trivial ? null : sources;
    }

    /**
     * Tests whether a join between results sets with given variables is a cartesian product.
     *
     * @param leftPublicVars publicly exposed variables of the left-side operand
     * @param rightAllVars all variables of the right side operand
     * @return {@code true} iff no var in {@code rightAllVars} is in {@code leftPublicVars}.
     */
    public static boolean isProduct(List<String> leftPublicVars, List<String> rightAllVars) {
        boolean product = true;
        for (String name : rightAllVars) {
            if (leftPublicVars.contains(name)) {
                product = false;
                break;
            }
        }
        return product;
    }

    public RowOperations        rowOps() { return rowOps; }
    public List<String>        outVars() { return outVars; }
    public boolean       isTrivialLeft() { return sources == null; }

    @SuppressWarnings("unchecked")
    public R merge(@Nullable R left, @Nullable R right) {
        if (sources == null)
            return left == null ? (R) rowOps.createEmpty(outVars) : left;
        R merged = (R) rowOps.createEmpty(outVars);
        for (int i = 0; i < sources.length; i++) {
            int idx = sources[i];
            if (idx != 0) {
                String var = outVars.get(i);
                Object value = rowOps.get(idx > 0 ? left : right, Math.abs(idx) - 1, var);
                rowOps.set(merged, i, var, value);
            }
        }
        return merged;
    }
}
