package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.sparql.expr.Term;
import org.checkerframework.checker.nullness.qual.Nullable;

public class SharedKind {
    private static final byte IS_SHARED    = 0x1;
    private static final byte IS_LIT       = 0x2;
    private static final byte IS_IRI_OR_BLANK = 0x4;

    public static final byte WHOLE_LIT          = IS_LIT;
    public static final byte WHOLE_UNKNOWN      = 0;
    public static final byte WHOLE_IRI_OR_BLANK = IS_IRI_OR_BLANK;
    public static final byte SUFF_LIT           = IS_SHARED|IS_LIT;
    public static final byte PREF_IRI_OR_BLANK  = IS_SHARED|IS_IRI_OR_BLANK;

    public static byte asKind(Term.@Nullable Type type, boolean hasShared) {
        return switch (type) {
            case LIT  -> hasShared ? SUFF_LIT : WHOLE_LIT;
            case null -> hasShared ? PREF_IRI_OR_BLANK : WHOLE_UNKNOWN;
            default   -> hasShared ? PREF_IRI_OR_BLANK : WHOLE_IRI_OR_BLANK;
        };
    }

    public static byte make(boolean hasShared, boolean isLit) {
        return (byte)((hasShared ? IS_SHARED : 0) | (isLit ? IS_LIT : IS_IRI_OR_BLANK));
    }
    public static byte lit(boolean shared) {
        return shared ? SUFF_LIT : WHOLE_LIT;
    }
    public static byte iriOrBlank(boolean shared) {
        return shared ? PREF_IRI_OR_BLANK : WHOLE_IRI_OR_BLANK;
    }
    public static byte whole(boolean isLit) {
        return isLit ? WHOLE_LIT : WHOLE_IRI_OR_BLANK;
    }

    public static boolean isLit(byte sharedKind) { return (sharedKind&IS_LIT) != 0; }
    public static boolean isSuffix(byte sharedKind) { return sharedKind == SUFF_LIT; }
    public static boolean isPrefix(byte sharedKind) { return (sharedKind& PREF_IRI_OR_BLANK) != 0; }

    public static String toString(byte sharedKind) {
        if ((sharedKind&IS_SHARED) == 0) {
            if      ((sharedKind&IS_LIT) != 0)          return "WHOLE_LIT";
            else if ((sharedKind&IS_IRI_OR_BLANK) != 0) return "WHOLE_IRI_OR_BLANK";
            else                                        return "WHOLE_UNKNOWN";
        } else if ((sharedKind&IS_LIT) != 0) {
            return "SUFF_LIT";
        } else if ((sharedKind&IS_IRI_OR_BLANK) != 0) {
            return "PREF_IRI_OR_BLANK";
        }  else {
            return "SHARED_UNKNOWN";
        }
    }
}
