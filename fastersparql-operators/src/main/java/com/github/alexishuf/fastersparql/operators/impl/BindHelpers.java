package com.github.alexishuf.fastersparql.operators.impl;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BindHelpers {
    public static void checkBind(@Nullable Map<String, String> map) {
        if (map == null || map.isEmpty())
            return;
        for (Map.Entry<String, String> e : map.entrySet()) {
            if (e.getKey() == null)
                throw new IllegalArgumentException("null variable name (key) in "+map);
            if (e.getKey().isEmpty())
                throw new IllegalArgumentException("empty variable name (key) in "+map);
            String v = e.getValue();
            if (v != null && v.isEmpty())
                throw new IllegalArgumentException("empty string is not a valid NT term. map="+map);
        }
    }

    public static void checkBind(@Nullable List<String> vars, @Nullable Object values,
                                  int valuesSize) {
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
            }
        }
    }

    public static Map<String, String> toMap(List<String> varNames, List<String> ntValues) {
        checkBind(varNames, ntValues, ntValues.size());
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

    public static Map<String, String> toMap(List<String> varNames, String[] ntValues) {
        checkBind(varNames, ntValues, ntValues.length);
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
}
