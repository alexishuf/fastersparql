package com.github.alexishuf.fastersparql.operators.row.impl;

import com.github.alexishuf.fastersparql.operators.row.RowMatcher;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class ArrayMatcher implements RowMatcher {
    protected final int[] leftIndices, rightIndices;

    public ArrayMatcher(List<String> leftVars, List<String> rightVars) {
        int capacity = Math.min(leftVars.size(), rightVars.size());
        int[] lArray = new int[capacity], rArray = new int[capacity];
        int size = 0;
        for (int lIdx = 0, lSize = leftVars.size(); lIdx < lSize; lIdx++) {
            int rIdx = rightVars.indexOf(leftVars.get(lIdx));
            if (rIdx >= 0) {
                lArray[size] = lIdx;
                rArray[size++] = rIdx;
            }
        }
        leftIndices  = Arrays.copyOf(lArray, size);
        rightIndices = Arrays.copyOf(rArray, size);
    }

    @Override public boolean hasIntersection() {
        return leftIndices.length > 0;
    }

    @Override public boolean matches(Object left, Object right) {
        Object[] lArray = (Object[]) left, rArray = (Object[]) right;
        for (int i = 0; i < leftIndices.length; i++) {
            Object lTerm = lArray[ leftIndices[i]];
            Object rTerm = rArray[rightIndices[i]];
            if (!Objects.equals(lTerm, rTerm))
                return false;
        }
        return true;
    }
}
