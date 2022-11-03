package com.github.alexishuf.fastersparql.client.util;

import com.github.alexishuf.fastersparql.client.exceptions.SparqlClientInvalidArgument;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.regex.Pattern;

public class HeaderUtils {
    private static final Pattern END_COLON = Pattern.compile("\\s*:\\s*$");

    private static final Pattern SANE_HEADER = Pattern.compile("[a-z0-9-.]+");

    static final String TOKEN_STR = "[^()<>@,;:\\\\\"/\\[\\]?={}\\t\\n\\r ]+";

    /**
     * See <a href="https://datatracker.ietf.org/doc/html/rfc2616/#section-2.2">token
     * token in RFC 2616</a>.
     */
    public static final Pattern TOKEN = Pattern.compile(TOKEN_STR);

    static String checkToken(@Nullable CharSequence cs, String name) {
        if (cs == null)
            throw new SparqlClientInvalidArgument(name+" cannot be null");
        if (cs.length() == 0)
            throw new SparqlClientInvalidArgument(name+" cannot be empty");
        if (!TOKEN.matcher(cs).matches())
            throw new SparqlClientInvalidArgument("Invalid "+name+": \""+cs+"\"");
        return cs.toString();
    }

    public static String sanitizeHeaderName(@Nullable String name) {
        String sanitized = name;
        if (name != null && !name.isEmpty()) {
            if (SANE_HEADER.matcher(name).matches())
                return name;
            sanitized = END_COLON.matcher(name).replaceAll("").trim().toLowerCase();
        }
        return checkToken(sanitized, "Header name");
    }
}
