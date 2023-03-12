package com.github.alexishuf.fastersparql.util;

import com.github.alexishuf.fastersparql.exceptions.FSInvalidArgument;
import com.github.alexishuf.fastersparql.model.MediaType;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.regex.Pattern;

public class HeaderUtils {

    private static final Pattern END_COLON = Pattern.compile("\\s*:\\s*");
    private static final Pattern SANE_HEADER = Pattern.compile("[-.a-z0-9]+");

    public static String sanitizeHeaderName(@Nullable String name) {
        if (name == null || name.isEmpty())
            throw new FSInvalidArgument("Header name cannot be empty");
        if (SANE_HEADER.matcher(name).matches())
            return name;
        name = END_COLON.matcher(name.trim().toLowerCase()).replaceAll("");
        if (!MediaType.TOKEN.matcher(name).matches())
            throw new FSInvalidArgument("Invalid header name: "+name);
        return name;
    }
}
