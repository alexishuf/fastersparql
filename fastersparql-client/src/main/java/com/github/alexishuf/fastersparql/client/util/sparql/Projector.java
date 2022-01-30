package com.github.alexishuf.fastersparql.client.util.sparql;


import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.PolyNull;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;

public final class Projector {
    public static final Projector IDENTITY = new Projector(null);

    private final int @Nullable [] indices;

    private Projector(int @Nullable [] indices) {
        this.indices = indices;
    }

    public static Projector createFor(List<String> outVars, List<String> inVars) {
        if (outVars.equals(inVars))
            return IDENTITY;
        return new Projector(VarUtils.projectionIndices(outVars, inVars));
    }

    public <T>  @Nullable T @PolyNull [] project(@Nullable T @PolyNull[] input) {
        if (indices == null)
            return input;
        if (input == null) return null;
        Class<?> componentType = input.getClass().getComponentType();
        //noinspection unchecked
        @Nullable T[] out = (T[]) Array.newInstance(componentType, indices.length);
        for (int i = 0; i < indices.length; i++) {
            int idx = indices[i];
            out[i] = idx >= 0 && idx < input.length ? input[idx] : null;
        }
        return out;
    }

    public <T> @PolyNull List<@Nullable T> project(@PolyNull List<@Nullable T> input) {
        if (indices == null)
            return input;
        int inputSize = input.size();
        ArrayList<T> out = new ArrayList<>(indices.length);
        for (int idx : indices)
            out.add(idx >= 0 && idx < inputSize ? input.get(idx) : null);
        return out;
    }
}
