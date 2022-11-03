package com.github.alexishuf.fastersparql.sparql.expr;

import static com.github.alexishuf.fastersparql.client.util.Skip.alphabet;

public class SparqlSkip {
    public static final long[] VAR_MARK = alphabet("$?").get();
    public static final long[]   IRIREF = alphabet("<>\"{}|^`]-").whitespace().control().invert().get();
    public static final long[]  VARNAME =  alphabet("_")  .nonAscii().letters().digits().get();
    public static final long[] BN_LABEL = alphabet("_-.").nonAscii().letters().digits().get();
    public static final long[]  LANGTAG =  alphabet("-")  .nonAscii().letters().digits().get();
}
