package com.github.alexishuf.fastersparql.util;


import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;

public class CSUtils {
    public static final byte[] BITS_2_BASE64 = {
            'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L',
            'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X',
            'Y', 'Z', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j',
            'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v',
            'w', 'x', 'y', 'z', '0', '1', '2', '3', '4', '5', '6', '7',
            '8', '9', '+', '/'
    };

    public static final byte[] BASE64_2_BITS;
    static {
        byte[] char2value = new byte[128];
        Arrays.fill(char2value, (byte)-1);
        for (int value = 0; value < BITS_2_BASE64.length; value++)
            char2value[BITS_2_BASE64[value]] = (byte)value;
        BASE64_2_BITS = char2value;
    }

    /**
     * Compute a {@link String}-compatible hash code even if cs is not a {@link String}.
     *
     * @param cs the {@link CharSequence} to compute a hash for.
     * @return a hash code for {@code cs} or zero if {@code cs == null}.
     */
    public static int hash(@Nullable CharSequence cs) {
        if (cs instanceof String) {
            return cs.hashCode();
        } else if (cs == null) {
            return 0;
        } else {
            int h = 0;
            for (int i = 0, len = cs.length(); i < len; i++)
                h = 31 * h + cs.charAt(i);
            return h;
        }
    }

    public static boolean startsWith(CharSequence cs, int begin, String prefix) {
        int prefixLen = prefix.length();
        if (cs instanceof String s)
            return s.regionMatches(begin, prefix, 0, prefixLen);
        boolean ok = begin+prefixLen <= cs.length();
        for (int i = 0; i < prefixLen && ok; i++)
            ok = cs.charAt(begin+i) == prefix.charAt(i);
        return ok;
    }

    public static @NonNegative int findNotEscaped(CharSequence cs, int from, char ch) {
        int length = cs.length();
        if (cs instanceof String string) {
            for (int i = from; i < length; i++) {
                i = string.indexOf(ch, i);
                if (i < 0) {
                    return cs.length();
                } else if (i > 0 && string.charAt(i-1) == '\\') {
                    if (i > 1 && string.charAt(i-2) == '\\')
                        return i; // \ at i-1 is a escaped \
                    //else: \ at i-1 starts a escape sequence
                } else {
                    return i; // no \ before ch at i
                }
            }
        }
        boolean escaped = from  > 0 && cs.charAt(from-1) == '\\'
                       && (from < 2 || cs.charAt(from-2) != '\\');
        for (int i = from, len = cs.length(); i < len; i++) {
            if (escaped) {
                escaped = false;
            } else {
                char c = cs.charAt(i);
                if (c == '\\')
                    escaped = true;
                else if (c == ch)
                    return i;
            }
        }
        return cs.length();
    }
}
