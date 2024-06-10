package com.github.alexishuf.fastersparql.util;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

public class StreamNodeRegistry {
    private static final ConcurrentHashMap<String, StreamNode> map = new ConcurrentHashMap<>();

    public static void register(@Nullable String name, @Nullable StreamNode node) {
        if (name == null)
            return;
        if (node == null)
            map.remove(name);
        else
            map.put(name, node);
    }

    public static @Nullable StreamNode get(@Nullable String name) {
        return name == null ? null : map.getOrDefault(name, null);
    }

    public static Stream<StreamNode> stream(@Nullable String name) {
        if (name == null)
            return Stream.empty();
        return Stream.ofNullable(map.getOrDefault(name, null));
    }
}
