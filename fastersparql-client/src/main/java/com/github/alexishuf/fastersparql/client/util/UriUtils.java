package com.github.alexishuf.fastersparql.client.util;

import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.PolyNull;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

@Slf4j
public class UriUtils {
    /**
     * HEX[i] == Integer.toHexString(i).toUpperCase().charAt(0)
     */
    private static final byte[] HEX = {
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'
    };
    /**
     * As per RFC 2396 the following characters are subject to percent-escaping (except in
     * locations where they are expected in the URI grammar):
     *
     * reserved    = ";" | "/" | "?" | ":" | "@" | "&" | "=" | "+" |
     *               "$" | ","
     * control     = <US-ASCII coded characters 00-1F and 7F hexadecimal>
     * space       = <US-ASCII coded character 20 hexadecimal>
     * delims      = "<" | ">" | "#" | "%" | <">
     * unwise      = "{" | "}" | "|" | "\" | "^" | "[" | "]" | "`"
     */
    private static final byte[] FORBIDDEN_QUERYPARAM;

    static {
        String forbidden = ";/?:@&=+$,"     + // reserved
                           /* control < 0x1F handled in code */
                           "\u007F"         + // control
                           " "              + // space
                           "<>#%\""         + // delims
                           "{}|\\^[]`"      ; // unwise
        byte[] arr = forbidden.getBytes(StandardCharsets.UTF_8);
        Arrays.sort(arr);
        FORBIDDEN_QUERYPARAM = arr;
    }

    static boolean needsEscape(char c) {
        return c < 0x1F || (c < 128 && Arrays.binarySearch(FORBIDDEN_QUERYPARAM, (byte)c) >= 0);
    }

    static byte hexValue(char c) {
        if (c <= '9')
            return (byte)(c-'0');
        else if (c <= 'F')
            return (byte)(10+c-'A');
        else if (c <= 'f' && c >= 'a')
            return (byte)(10+c-'a');
        return -1;
    }

    /**
     * Checks if {@code in} has a character that would require a %-escape if used as a
     * query param name or value.
     *
     * @param in the {@link CharSequence} to test. If null, will return false as there is
     *           nothing to escape
     * @return {@code} true iff {@code in} is not {@code null} and the specified character range
     *         contains a character that is forbidden in a query parameter name or value.
     */
    public static boolean needsEscape(@Nullable CharSequence in) {
        return in != null && needsEscape(in, 0, in.length());
    }

    /**
     * Checks if {@code in} from {@code begin} (inclusive) to {@code end} (exclusive) has
     * a character that would require a %-escape if used as a query param name or value.
     *
     * @param in the {@link CharSequence} to test. If null, will return false as there is
     *           nothing to escape
     * @param begin index of first character to check
     * @param end index of first character after {@code begin} to not check
     * @return {@code} true iff {@code in} is not {@code null} and the specified character range
     *         contains a character that is forbidden in a query parameter name or value.
     */
    public static boolean needsEscape(@Nullable CharSequence in, int begin, int end) {
        if (in == null)
            return false;
        int state = 0, code = 0;
        for (int i = begin; i < end; i++) {
            char c = in.charAt(i);
            switch (state) {
                case 0:
                    if      (c == '%') state = 1; // begin %-escape
                    else if (needsEscape(c)) return true;
                    break;
                case 1:
                    if ((code = hexValue(c)) < 0)
                        return true; // bad %-escape
                    state = 2;
                    break;
                case 2:
                    byte nibble = hexValue(c);
                    if (nibble < 0 || (code = code << 4 | nibble) > 128)
                        return true; // bad %-escape
                    state = 0; //valid %-escape
                    break;
            }
        }
        return state != 0;
    }

    private static StringBuilder doEscapeQueryParam(CharSequence in, int begin, int end,
                                                    StringBuilder out) {
        int len = end - begin;
        out.ensureCapacity(out.length() + len*2);
        for (int i = begin; i < end; i++) {
            char c = in.charAt(i);
            if (needsEscape(c))
                out.append('%').append((char)HEX[(c&0xF0) >> 4]).append((char)HEX[c&0x0F]);
            else
                out.append(c);
        }
        return out;
    }

    /**
     * Equivalent to {@link UriUtils#escapeQueryParam(CharSequence, int, int)} with
     * {@code begin=0} and {@code end=string.length}.
     *
     * @param string the string to escape.
     * @return null if {@code string==null}, else an equivalent string with forbidden chars
     * percent-escaped. If {@code string}, does not require any percent-escaping, itself will
     * be returned, which may expose mutability if the input {@link CharSequence} is mutable.
     */
    public static @PolyNull CharSequence escapeQueryParam(@PolyNull CharSequence string) {
        if (string == null)
            return null;
        return escapeQueryParam(string, 0, string.length());
    }

    /**
     * Escape any character not allowed in query parameter names or values, as defined per
     * <a href="https://datatracker.ietf.org/doc/html/rfc2396">RFC 2396</a>.
     *
     * Only {@code string.subSequence(begin, end)} will be processed. Data before or after
     * this range will not be included in the result.
     *
     * @param string the value to be escaped as if it were used as a query parameter name or value
     * @param begin first character to escape
     * @param end first character after {@code begin} to not escape.
     * @return the %-escaped string equivalent to {@code string.subSequence(begin, end)},
     *         or null if {@code string} was null. If escapes are not required, a subsequence
     *         object will be returned, which may reflect mutations on {@code string} depending
     *         on its implementation.
     */
    public static @PolyNull CharSequence escapeQueryParam(@PolyNull CharSequence string,
                                                          int begin, int end) {
        if (end < begin)
            throw new IllegalArgumentException("end="+end+"< begin="+begin);
        int len = end - begin;
        if (!needsEscape(string, begin, end))
            return len == string.length() ? string : string.subSequence(begin, end);
        StringBuilder builder = new StringBuilder(len* 2);
        return doEscapeQueryParam(string, begin, end, builder);
    }

    /**
     * Append {@code string} to {@code builder}, percent-escaping any character not allowed in
     * query parameter names or values, as defined per
     * <a href="https://datatracker.ietf.org/doc/html/rfc2396">RFC 2396</a>.
     *
     * @param builder the output {@link StringBuilder}. If null, will create
     *                a new {@link StringBuilder}
     * @param string the input string possibly containing disallowed characters
     * @return either {@code builder} or a new {@link StringBuilder} if {@code builder == null}.
     */
    public static StringBuilder escapeQueryParam(@Nullable StringBuilder builder,
                                                 CharSequence string) {
        return escapeQueryParam(builder, string, 0, string == null ? 0 : string.length());
    }

    /**
     * Append {@code string} from {@code begin} (inclusive) to {@code end} (exclusive) into
     * {@code builder} percent-escaping any character not allowed in query parameter names or
     * values, as defined per <a href="https://datatracker.ietf.org/doc/html/rfc2396">RFC 2396</a>.
     *
     * @param builder the output {@link StringBuilder}. If null, will create
     *                a new {@link StringBuilder}
     * @param string the input string possibly containing disallowed characters
     * @param begin first index in {@code string} to include.
     * @param end first index in {@code string} to <strong>not</strong> include
     * @return either {@code builder} or a new {@link StringBuilder} if {@code builder == null}.
     */
    public static StringBuilder escapeQueryParam(@Nullable StringBuilder builder,
                                                 CharSequence string,
                                                 @NonNegative int begin, @NonNegative int end) {
        boolean needs = needsEscape(string, begin, end);
        int capacity = (end - begin) << (needs ? 1 : 0);
        if (builder == null) builder = new StringBuilder(capacity);
        else                 builder.ensureCapacity(capacity);
        return needs ? doEscapeQueryParam(string, begin, end, builder)
                     : builder.append(string, begin, end);
    }

    /**
     * Escape any character not allowed in query parameter names or values, as defined per
     * <a href="https://datatracker.ietf.org/doc/html/rfc2396">RFC 2396</a>
     *
     * @param string the value to be escaped as if it were used as a query parameter name or value
     * @return the %-escaped string, or null if {@code string} was null
     */
    public static @PolyNull String escapeQueryParam(@PolyNull String string) {
        return string == null ? null : escapeQueryParam((CharSequence) string).toString();
    }

    /**
     * Replace all %-escapes with the escaped characters
     *
     * @param string the string to have %-escapes decoded
     * @return a new string with escapes decoded or the same instance there are no escapes.
     */
    public static @PolyNull String unescape(@PolyNull String string) {
        if (string == null)
            return null;
        int begin = 0, i = string.indexOf('%'), len = string.length();
        if (i < 0)
            return string;
        StringBuilder b = new StringBuilder(len + 16);
        while (i >= 0) {
            b.append(string, begin, i);
            if (i + 2 < len) {
                try {
                    b.append((char) Integer.parseInt(string.substring(i+1, i+3), 16));
                } catch (NumberFormatException e) {
                    log.warn("Invalid %-escape {}, will not decode", string.substring(i, i+3));
                    b.append(string, i, i + 3);
                }
            } else {
                log.debug("Truncated %-escape on string end: \"{}\"", string);
            }
            i = string.indexOf('%', begin = i+3);
        }
        return b.append(string, begin, len).toString();
    }
}
