package com.github.alexishuf.fastersparql.operators.row.impl;

import com.github.alexishuf.fastersparql.client.util.CSUtils;
import com.github.alexishuf.fastersparql.operators.row.RowOperations;
import com.github.alexishuf.fastersparql.operators.row.RowOperationsProvider;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

public class CharSequenceArrayOperations extends ArrayOperations {
    private static final CharSequenceArrayOperations INSTANCE = new CharSequenceArrayOperations();
    public static class Provider implements RowOperationsProvider {
        @Override public RowOperations get() { return INSTANCE; }
        @Override public Class<?> rowClass() { return CharSequence[].class; }
    }

    @Override public Object createEmpty(List<String> vars) {
        return new CharSequence[vars.size()];
    }

    @Override public boolean equalsSameVars(Object left, Object right) {
        if (left == right) return true;
        if (left == null || right == null) return false;
        CharSequence[] lArray = (CharSequence[]) left, rArray = (CharSequence[]) right;
        if (lArray.length != rArray.length)
            return false;
        for (int i = 0; i < lArray.length; i++) {
            CharSequence lCS = lArray[i], rCS = rArray[i];
            if (lCS == rCS)
                continue;
            if (lCS == null || rCS == null || lCS.length() != rCS.length())
                return false;
            if (!CSUtils.startsWith(lCS, 0, lCS.length(), rCS))
                return false;
        }
        return true;
    }

    @Override public int hash(@Nullable Object row) {
        if (row == null) return 0;
        CharSequence[] a = (CharSequence[]) row;
        int h = 0;
        for (CharSequence cs : a)
            h = 31 * h + CSUtils.hash(cs);
        return h;
    }
}
