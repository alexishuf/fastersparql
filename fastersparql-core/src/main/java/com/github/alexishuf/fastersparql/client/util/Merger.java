package com.github.alexishuf.fastersparql.client.util;

import com.github.alexishuf.fastersparql.client.BindType;
import com.github.alexishuf.fastersparql.client.model.Vars;
import com.github.alexishuf.fastersparql.client.model.row.RowType;
import org.checkerframework.checker.nullness.qual.Nullable;

public final class Merger<R, I> {
    public final RowType<R, I> rowType;
    public final Vars outVars;
    private final int @Nullable[] sources;

    private Merger(RowType<R, I> rowType, Vars outVars, int @Nullable [] sources) {
        this.rowType = rowType;
        this.outVars = outVars;
        this.sources = sources;
    }

    /**
     * Create a merger for use when merging a left and a right row where the right
     * involved in the given {@link BindType}.
     *
     * @param rowType The {@link RowType} for {@code R}
     * @param leftPublicVars the vars in {@code left} rows to be given in
     *                       {@link Merger#merge(Object, Object)}. This list may be held by
     *                       reference and must not change.
     * @param rightVars the vars of the {@code right} row to be given in future
     *                  {@link Merger#merge(Object, Object)} calls
     * @param type the type of bind operation that will generate the rows fed to
     *                 {@link Merger#merge(Object, Object)}.
     * @param <R> the type of row
     * @return a new {@link Merger}.
     */
    public static <R, I> Merger<R, I> forMerge(RowType<R, I> rowType, Vars leftPublicVars,
                                               Vars rightVars, BindType type,
                                               @Nullable Vars outVars) {
        int @Nullable [] sources;
        if (outVars == null)
            outVars = type.resultVars(leftPublicVars, rightVars);
        sources = type.isJoin() ? findSources(outVars, leftPublicVars, rightVars) : null;
        return new Merger<>(rowType, outVars, sources);
    }


    /**
     * Create a {@link Merger} for projection, making {@link Merger#merge(Object, Object)}
     * ignore the {@code right} parameter.
     *
     * @param rowType the {@link RowType} for {@code R}
     * @param outVars the desired list of vars to be output from {@link Merger#merge(Object, Object)}
     * @param inVars the vars of {@code left} rows provided to {@link Merger#merge(Object, Object)}
     * @param <R> the row type.
     * @return a new {@link Merger}.
     */
    public static <R, I> Merger<R, I> forProjection(RowType<R, I> rowType, Vars outVars,
                                                    Vars inVars) {
        return new Merger<>(rowType, outVars, findSources(outVars, inVars, Vars.EMPTY));
    }

    /**
     * Create a {@link Merger} for a no-op projection, where {@link Merger#merge(Object, Object)}
     * ignores {@code right} and returns {@code left}, which must have {@code outVars} as variables.
     *
     * @param rowType {@link RowType} for R
     * @param outVars the expected variables for {@code left} in future
     *                {@link Merger#merge(Object, Object)} calls
     * @param <R> the row type
     * @return a new {@link Merger}
     */
    public static <R, I> Merger<R, I> identity(RowType<R, I> rowType, Vars outVars) {
        return new Merger<>(rowType, outVars, null);
    }

    private static int @Nullable [] findSources(Vars out, Vars leftPublic,
                                                Vars rightFree) {
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

    public RowType<R, I> rowType() { return rowType; }
    public Vars  outVars() { return outVars; }

    public Vars rightFreeVars() {
        if (sources == null) return Vars.EMPTY;
        Vars vars = new Vars.Mutable(sources.length);
        for (int i = 0; i < sources.length; i++)
            if (sources[i] < 0)
                vars.add(outVars.get(i));
        return vars;
    }

    public R merge(@Nullable R left, @Nullable R right) {
        if (sources == null)
            return left == null ? rowType.createEmpty(outVars) : left;
        R merged = rowType.createEmpty(outVars);
        for (int i = 0; i < sources.length; i++) {
            int idx = sources[i];
            if (idx != 0) {
                var value = rowType.get(idx > 0 ? left : right, Math.abs(idx) - 1);
                rowType.set(merged, i, value);
            }
        }
        return merged;
    }
}
