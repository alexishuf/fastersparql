package com.github.alexishuf.fastersparql.client.util;


import org.checkerframework.checker.index.qual.Positive;

public class CSUtils {

    /**
     * Equivalent to {@link CSUtils#skipUntilIn(CharSequence, int, int, char)} with
     * {@code end = cs.length()}.
     */
    public static int skipUntil(CharSequence cs, int from, char ch) {
        return skipUntilIn(cs, from, cs.length(), ch);
    }

    /**
     * Get the index of the {@code ch} in {@code cs} in the range from {@code from} to {@code end}.
     *
     * @param cs the {@link CharSequence} where to search for {@code ch}
     * @param from the first index to search
     * @param end the first index to <strong>not</strong> search
     * @param ch the character to search for
     * @return The lowest index {@code from <= i < end} where {@code cs.charAt(i) == ch}
     *         or else {@code end}.
     */
    public static int skipUntilIn(CharSequence cs, int from, int end, char ch) {

        if (cs instanceof String) {
            int i = ((String) cs).indexOf(ch, from);
            return i < 0 || i >= end  ? end : i;
        } else {
            for (int i = from; i < end; i++) {
                if (cs.charAt(i) == ch) return i;
            }
            return end;
        }
    }

    /**
     * Equivalent to {@link CSUtils#skipUntilIn(CharSequence, int, int, char, char)}
     * with {@code cs.length} as {@code end}
     */
    public static int skipUntil(CharSequence cs, int from, char ch0, char ch1) {
        return skipUntilIn(cs, from, cs.length(), ch0, ch1);
    }

    /**
     * Get the index of the first char after {@code from} that is {@code c0} or {@code c1}.
     *
     * @param cs The {@link CharSequence} where to search
     * @param from the first index to search
     * @param end the first index to <strong>not</strong> search
     * @param c0 one char to look for
     * @param c1 alternative char to look for
     * @return {@code i} where {@code cs.charAt(i)==c0}, {@code cs.charAt(i)==c0} or {@code i==end}.
     */
    public static int skipUntilIn(CharSequence cs, int from, int end, char c0, char c1) {
        for (int i = from; i < end; i++) {
            char c = cs.charAt(i);
            if (c == c0 || c == c1) return i;
        }
        return end;
    }

    /**
     * Equivalent to {@link CSUtils#skipUntilIn(CharSequence, int, int, char, char, char)}
     * with {@code end = cs.length()}.
     */
    public static int skipUntil(CharSequence cs, int from, char c0, char c1, char c2) {
        return skipUntilIn(cs, from, cs.length(), c0, c1, c2);
    }

    /**
     * Find the first index in {@code cs} after {@code from} with {@code c0}, {@code c1} or
     * {@code c2}.
     *
     * @param cs the input {@link CharSequence} where to search
     * @param from the first index to search
     * @param end the first index to <strong>not</strong> search
     * @param c0 one char to search for
     * @param c1 an alternative char to search for
     * @param c2 another alternative char to search for
     * @return The lowest {@code from <= i < end} where {@code cs.charAt(i)} is one of the
     *         given characters or {@code end} if there is no such {@code i}.
     */
    public static int skipUntilIn(CharSequence cs, int from, int end, char c0, char c1, char c2) {
        for (int i = from; i < end; i++) {
            char c = cs.charAt(i);
            if (c == c0 || c == c1 || c == c2) return i;
        }
        return end;
    }

    public static @Positive int skipSpaceAnd(CharSequence cs, int from, char skip) {
        for (int i = from, len = cs.length(); i < len; i++) {
            char c = cs.charAt(i);
            if (c < '\t' || (c > '\r' && c != ' ' && c != skip))
                return i;
        }
        return cs.length();
    }

    public static @Positive int findNotEscaped(CharSequence cs, int from, char ch) {
        int length = cs.length();
        if (cs instanceof String) {
            String string = (String) cs;
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

    public static String charName(char c) {
        String name = Character.getName(c);
        return name == null ? "code point "+(int)c : name;
    }

    public static String charNames(char... cs) {
        int length = cs.length;
        if      (length == 0) return "";
        else if (length == 1) return charName(cs[0]);
        StringBuilder b = new StringBuilder(length * 16);
        for (char c : cs)
            b.append(charName(c)).append(", ");
        b.setLength(b.length()-2);
        return b.toString();
    }
}
