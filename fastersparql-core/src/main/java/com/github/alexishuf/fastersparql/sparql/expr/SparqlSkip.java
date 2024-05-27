package com.github.alexishuf.fastersparql.sparql.expr;

import static com.github.alexishuf.fastersparql.model.rope.Rope.*;
import static java.nio.charset.StandardCharsets.UTF_8;


@SuppressWarnings("unused")
public class SparqlSkip {
    public static final int[]     VAR_MARK = alphabet("$?");
    public static final int[] NUMBER_FIRST = alphabet("+-", Range.DIGIT);
    public static final int[]  BOOL_FOLLOW = invert(withNonAscii(alphabet("_-%:", Range.ALPHANUMERIC)));
    public static final int[]     A_FOLLOW = alphabet(",.;/(){}[]!=<>", Range.WS);
    public static final int[]       IRIREF = invert(alphabet("<>\"{}|^`]-", Range.WS));
    public static final int[]      VARNAME = withNonAscii(alphabet("_", Range.ALPHANUMERIC));
    public static final int[]     BN_LABEL = withNonAscii(alphabet("_-.", Range.ALPHANUMERIC));
    public static final int[]      LANGTAG =  withNonAscii(alphabet("-", Range.ALPHANUMERIC));
    public static final int[]    PN_PREFIX = BN_LABEL;
    public static final int[]     PN_LOCAL = withNonAscii(alphabet("_-.%:\\/^[]{}+!@#$&*()", Range.ALPHANUMERIC));
    public static final int[] PN_LOCAL_LAST = withNonAscii(alphabet("_-:", Range.ALPHANUMERIC));
    public static final int[] BAD_IRI_START = alphabet("?$\"'(");

    public static final int[] LIT_ESCAPED = alphabet("\"\\\r\n");
    public static final int[] LIT_ESCAPE_NAME = alphabet("\"\\rn");
    public static final int[] LIT_ESCAPE_NAME_SINGLE = alphabet("'\\rn");
    public static final int[] UNTIL_LIT_ESCAPED = invert(LIT_ESCAPED);

    public static final byte[] BN_PREFIX_u8 = "_:".getBytes(UTF_8);
    public static final byte[] PREFIX_u8 = "PREFIX".getBytes(UTF_8);
    public static final byte[] BASE_u8 = "BASE".getBytes(UTF_8);
    public static final byte[] ASK_u8 = "ASK".getBytes(UTF_8);
    public static final byte[] SELECT_u8 = "SELECT".getBytes(UTF_8);
    public static final byte[] CONSTRUCT_u8 = "CONSTRUCT".getBytes(UTF_8);
    public static final byte[] DESCRIBE_u8 = "DESCRIBE".getBytes(UTF_8);
    public static final byte[] WHERE_u8 = "WHERE".getBytes(UTF_8);
    public static final byte[] DISTINCT_u8 = "DISTINCT".getBytes(UTF_8);
    public static final byte[] REDUCED_u8 = "REDUCED".getBytes(UTF_8);
    public static final byte[] PRUNED_u8 = "PRUNED".getBytes(UTF_8);
    public static final byte[] FROM_u8 = "FROM".getBytes(UTF_8);
    public static final byte[] LIMIT_u8 = "LIMIT".getBytes(UTF_8);
    public static final byte[] OFFSET_u8 = "OFFSET".getBytes(UTF_8);
}
