package com.github.alexishuf.fastersparql.util;

import com.github.alexishuf.fastersparql.model.rope.ByteRope;
import com.github.alexishuf.fastersparql.model.rope.PlainRope;
import com.github.alexishuf.fastersparql.model.rope.Rope;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.PolyNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class UriUtils {
    private static final Logger log = LoggerFactory.getLogger(UriUtils.class);

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
     * <pre>
     * reserved    = ";" | "/" | "?" | ":" | "@" | "&" | "=" | "+" |
     *               "$" | ","
     * control     = <US-ASCII coded characters 00-1F and 7F hexadecimal>
     * space       = <US-ASCII coded character 20 hexadecimal>
     * delims      = "<" | ">" | "#" | "%" | <">
     * unwise      = "{" | "}" | "|" | "\" | "^" | "[" | "]" | "`"
     * </pre>
     */
    private static final int[] FORBIDDEN_QUERYPARAM = Rope.alphabet(
            ";/?:@&=+$," + // reserved
            "<>#%\""     + // delims
            "{}|\\^[]`"  , // unwise
            Rope.Range.WS);
    private static final int[] SAFE_QUERYPARAM = Rope.invert(FORBIDDEN_QUERYPARAM);

    static byte hexValue(byte c) {
        if (c <= '9')
            return (byte)(c-'0');
        else if (c <= 'F')
            return (byte)(10+c-'A');
        else if (c <= 'f' && c >= 'a')
            return (byte)(10+c-'a');
        return -1;
    }

    /**
     * Checks if {@code in} from {@code begin} (inclusive) to {@code end} (exclusive) has
     * a character that would require a %-escape if used as a query param name or value.
     *
     * @param in the {@link Rope} to test. If null, will return false as there is
     *           nothing to escape
     * @return {@code} true iff {@code in} is not {@code null} and the specified character range
     *         contains a character that is forbidden in a query parameter name or value.
     */
    public static boolean needsEscape(@Nullable Rope in) {
        if (in == null)
            return false;
        int state = 0, code = 0;
        for (int i = 0, end = in.len(); i < end; i++) {
            byte c = in.get(i);
            switch (state) {
                case 0 -> {
                    if (c == '%') state = 1; // begin %-escape
                    else if (Rope.contains(FORBIDDEN_QUERYPARAM, c)) return true;
                }
                case 1 -> {
                    if ((code = hexValue(c)) < 0)
                        return true; // bad %-escape
                    state = 2;
                }
                case 2 -> {
                    byte nibble = hexValue(c);
                    if (nibble < 0 || (code = code << 4 | nibble) > 128)
                        return true; // bad %-escape
                    state = 0; //valid %-escape
                }
            }
        }
        return state != 0;
    }

    public static boolean needsEscape(@Nullable CharSequence in) {
        if (in == null)
            return false;
        int state = 0, code = 0;
        for (int i = 0, end = in.length(); i < end; i++) {
            char c = in.charAt(i);
            boolean ascii = c < 128;
            switch (state) {
                case 0 -> {
                    if      (c == '%') state = 1; // begin %-escape
                    else if (ascii && Rope.contains(FORBIDDEN_QUERYPARAM, (byte) c)) return true;
                }
                case 1 -> {
                    if (!ascii || (code = hexValue((byte)c)) < 0)
                        return true; // bad %-escape
                    state = 2;
                }
                case 2 -> {
                    if (!ascii) return true;
                    byte nibble = hexValue((byte) c);
                    if (nibble < 0 || (code = code << 4 | nibble) > 128)
                        return true; // bad %-escape
                    state = 0; //valid %-escape
                }
            }
        }
        return state != 0;
    }

    private static ByteRope doEscapeQueryParam(Rope in, ByteRope out) {
        int end = in.len();
        out.ensureFreeCapacity(2* end);
        for (int i, consumed = 0; consumed < end; consumed = i+1) {
            i = in.skip(consumed, end, SAFE_QUERYPARAM);
            if (i < end) {
                out.append(in, consumed, i).append('%');
                byte c = in.get(i);
                out.append((char)HEX[(c&0xF0) >> 4]).append((char)HEX[c&0x0F]);
            } else {
                out.append(in, consumed, end);
            }
        }
        return out;
    }

    /**
     * Escape any character not allowed in query parameter names or values, as defined per
     * <a href="https://datatracker.ietf.org/doc/html/rfc2396">RFC 2396</a>.
     * <p>
     * Only {@code string.subSequence(begin, end)} will be processed. Data before or after
     * this range will not be included in the result.
     *
     * @param string the value to be escaped as if it were used as a query parameter name or value
     * @return the %-escaped string equivalent to {@code string.subSequence(begin, end)},
     *         or null if {@code string} was null. If escapes are not required, a subsequence
     *         object will be returned, which may reflect mutations on {@code string} depending
     *         on its implementation.
     */
    public static Rope escapeQueryParam(Rope string) {
        return needsEscape(string)
                ? doEscapeQueryParam(string, new ByteRope())
                : string;
    }
    /** {@link UriUtils#escapeQueryParam(Rope)} for {@link CharSequence}s */
    public static CharSequence escapeQueryParam(CharSequence cs) {
        return needsEscape(cs)
                ? doEscapeQueryParam(new ByteRope(cs), new ByteRope()).toString()
                : cs;
    }

    /**
     * Append {@code string} to {@code builder}, percent-escaping any character not allowed in
     * query parameter names or values, as defined per
     * <a href="https://datatracker.ietf.org/doc/html/rfc2396">RFC 2396</a>.
     *
     * @param builder the output {@link ByteRope}.
     * @param string the input string possibly containing disallowed characters
     */
    public static void escapeQueryParam(ByteRope builder, Rope string) {
        if (needsEscape(string))
            doEscapeQueryParam(string, builder);
        else
            builder.append(string);
    }

    /** {@link UriUtils#escapeQueryParam(ByteRope, Rope)} for {@link CharSequence}s */
    public static void escapeQueryParam(StringBuilder builder, CharSequence string) {
        if (needsEscape(string))
            builder.append(doEscapeQueryParam(new ByteRope(string), new ByteRope()));
        else
            builder.append(string);
    }

    /**
     * Replace all %-escapes with the escaped characters
     *
     * @param string the string to have %-escapes decoded
     * @param dst if {@code != null}, result will be written to it and {@code dst} itself
     *            will be returned by this call
     * @return a new string with escapes decoded or the same instance there are no escapes.
     */
    public static @PolyNull ByteRope unescape(@PolyNull PlainRope string, int begin, int end,
                                                 @Nullable ByteRope dst) {
        if (string == null)
            return null;
        int i = string.skipUntil(begin, end, '%');
        if (dst == null)
            dst = new ByteRope(end-begin);
        while (i < end) {
            dst.append(string, begin, i);
            if (i + 2 < end) {
                try {
                    dst.append((char) Integer.parseInt(string.toString(i+1, i+3), 16));
                } catch (NumberFormatException e) {
                    log.warn("Invalid %-escape {}, will not decode", string.sub(i, i+3));
                    dst.append(string, i, i + 3);
                }
            } else {
                log.debug("Truncated %-escape on string end: \"{}\"", string);
            }
            i = string.skipUntil(begin = i+3, end, '%');
        }
        return dst.append(string, begin, end);
    }
    public static @PolyNull SegmentRope unescape(@PolyNull PlainRope string, int begin, int end) {
        return unescape(string, begin, end, null);
    }

    public static @PolyNull ByteRope unescapeToRope(@PolyNull String string) {
        return unescapeToRope(string, null);
    }
    public static @PolyNull ByteRope unescapeToRope(@PolyNull String string,
                                                    @Nullable ByteRope dst) {
        if (string == null)
            return null;
        int begin = 0, len = string.length(), i = string.indexOf('%');
        if (dst == null)
            dst = new ByteRope(len);
        while (i != -1) {
            dst.append(string, begin, i);
            if (i + 2 < len) {
                try {
                    dst.append((char) Integer.parseInt(string.substring(i+1, i+3), 16));
                } catch (NumberFormatException e) {
                    log.warn("Invalid %-escape {}, will not decode", string.substring(i, i+3));
                    dst.append(string, i, i + 3);
                }
            } else {
                log.debug("Truncated %-escape on string end: \"{}\"", string);
            }
            i = string.indexOf('%', begin = i+3);
        }
        return dst.append(string, begin, len);
    }

    public static @PolyNull String unescape(@PolyNull String string) {
        if (string == null)
            return null;
        int begin = 0, len = string.length(), i = string.indexOf('%');
        if (i == -1)
            return string;
        var b = new StringBuilder(len);
        while (i != -1) {
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
