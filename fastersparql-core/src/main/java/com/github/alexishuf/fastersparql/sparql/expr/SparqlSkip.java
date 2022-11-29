package com.github.alexishuf.fastersparql.sparql.expr;

import org.checkerframework.checker.nullness.qual.Nullable;

import static com.github.alexishuf.fastersparql.client.util.Skip.alphabet;

public class SparqlSkip {
    public static final long[]     VAR_MARK = alphabet("$?").get();
    public static final long[] NUMBER_FIRST = alphabet("+-").digits().get();
    public static final long[] BOOL_FOLLOW  = alphabet("_-%:").nonAscii().letters().digits().invert().get();
    public static final long[]       IRIREF = alphabet("<>\"{}|^`]-").whitespace().control().invert().get();
    public static final long[]      VARNAME =  alphabet("_")  .nonAscii().letters().digits().get();
    public static final long[]     BN_LABEL = alphabet("_-.").nonAscii().letters().digits().get();
    public static final long[]      LANGTAG =  alphabet("-")  .nonAscii().letters().digits().get();
    public static final long[]    PN_PREFIX = BN_LABEL;
    public static final long[]     PN_LOCAL = alphabet("_-.%:").nonAscii().letters().digits().get();

    public static boolean isVar(@Nullable String s, int pos, int end) {
        if (s == null || pos >= (end == -1 ? s.length() : end))
            return false;
        char c = s.charAt(pos);
        return c == '$' || c == '?';
    }
}
