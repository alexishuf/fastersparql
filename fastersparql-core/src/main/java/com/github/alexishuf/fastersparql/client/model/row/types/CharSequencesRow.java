package com.github.alexishuf.fastersparql.client.model.row.types;

import com.github.alexishuf.fastersparql.client.util.CSUtils;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.reflect.Array;

@SuppressWarnings("unchecked")
public class CharSequencesRow<I extends CharSequence> extends ArrayRow<I> {
    public static final CharSequencesRow<CharSequence> INSTANCE
            = new CharSequencesRow<>(CharSequence.class);

    public CharSequencesRow(Class<I> itemClass) {
        super((Class<I[]>) Array.newInstance(itemClass, 0).getClass(), itemClass);
    }

    @Override public boolean equalsSameVars(I[] left, I[] right) {
        if (left == right) return true;
        if (left == null || right == null) return false;
        if (left.length != right.length)
            return false;
        for (int i = 0; i < left.length; i++) {
            CharSequence lCS = left[i], rCS = right[i];
            if (lCS == rCS)
                continue;
            if (lCS == null || rCS == null || lCS.length() != rCS.length())
                return false;
            return CharSequence.compare(lCS, rCS) == 0;
        }
        return true;
    }

    @Override public boolean needsCustomHash() { return true; }

    @Override public int hash(@Nullable Object row) {
        if (row == null) return 0;
        int h = 0;
        for (I cs : (I[])row)
            h = 31 * h + CSUtils.hash(cs);
        return h;
    }
}
