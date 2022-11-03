package com.github.alexishuf.fastersparql.client.util;

import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.common.returnsreceiver.qual.This;

public class Skip {
    /** An alphabet equivalent to the {@code [a-zA-Z0-9]} regex accepting only ASCII chars. */
    public static final long[] ALPHANUMERIC = alphabet().letters().digits().get();
    /** An alphabet equivalent to the {@code [0-9]} regex accepting only ASCII chars. */
    public static final long[] DIGITS = alphabet().digits().get();
    /** An alphabet for control characters and whitespace */
    public static final long[] WS = alphabet().control().whitespace().get();
    /** An alphabet everything that is not control characters nor whitespace */
    public static final long[] UNTIL_WS = alphabet().control().whitespace().invert().get();
    /** Singleton alphabet for '\n'. */
    public static final long[] UNTIL_LF = alphabet("\n").invert().get();

    /** Builder class for creating {@code long[2]} masks used by {@link Skip}'s
     *  {@code skip*()} methods*/
    public static final class AlphabetBuilder {
        final long[] alphabet = new long[2];

        /** Add all chars c such that {@code c >= begin && c <= end}. */
        public @This AlphabetBuilder addInclusive(int begin, int end) {
            assert begin > 0 && end < 128;
            for (int i = begin; i <= end; i++)
                alphabet[(i>>6)&1] |= 1L << i;
            return this;
        }
        /** Add all digits (0-9) to the mask */
        public @This AlphabetBuilder digits() { return addInclusive('0', '9'); }
        /** Add space (' '), tabs ('\t') and line break chars '\r' and '\n' */
        public @This AlphabetBuilder whitespace() { return add('\t', '\r', '\n', ' '); }
        /** Add all control chars other than NULL (0x00) i.e., DEL (0x7F) and
         *  everything below 0x20 (' '). */
        public @This AlphabetBuilder control() { return addInclusive('\u0001', ' ').add('\u007F'); }
        /** Add all letters (A-Z and a-z) to the mask */
        public @This AlphabetBuilder letters() { return addInclusive('a', 'z').addInclusive('A', 'Z'); }
        /** Add the given characters */
        public @This AlphabetBuilder add(char... characters) {
            for (char c : characters) {
                assert c < 128;
                alphabet[(c>>6)&1] |= 1L << c;
            }
            return this;
        }
        /** Remove the given characters */
        public @This AlphabetBuilder remove(char... values) {
            for (char c : values) {
                assert c < 128;
                alphabet[(c>>6)&1] &= ~(1L << c);
            }
            return this;
        }
        /** Treat any non-ASCII as PRESENT. */
        public @This AlphabetBuilder  nonAscii() { alphabet[0] |= 1; return this; }
        /** Invert this mask, i.e., turn {@code skip()} into {@code skipUntil()}. */
        public @This AlphabetBuilder invert() {
            alphabet[0] = ~alphabet[0];
            alphabet[1] = ~alphabet[1];
            return this;
        }
        public long[] get() { return alphabet; }
    }

    /** Start building a mask with the characters in given array of strings {@code a}. */
    public static AlphabetBuilder alphabet(String... a) {
        AlphabetBuilder builder = new AlphabetBuilder();
        for (String s : a)
            builder.add(s.toCharArray());
        return builder;
    }

    /** Turn an alphabet into a builder so that {@link AlphabetBuilder#get()} would return an
     *  array equals to {@code alphabet}. */
    public static AlphabetBuilder alphabet(long[] source) {
        var b = new AlphabetBuilder();
        b.alphabet[0] = source[0];
        b.alphabet[1] = source[1];
        return b;
    }

    /** Tests if {@code alphabet} contains the character {@code c}. */
    public static boolean contains(long[] alphabet, char c) {
        if (c < 128)
            return (alphabet[(c>>6)&1] & (1L << c)) != 0;
        else
            return (alphabet[0] & 1) == 1;
    }

    /**
     * Find the first {@code i >= begin} and {@code < end}, going from {@code end-1}
     * until {@code begin} such that {@code in[i]} is <strong>not</strong> in {@code mask}.
     *
     * @param in string to be searched
     * @param begin where the range to be searched within {@code in} starts.
     *              Note that this is the beginning of the range but since this method is
     *              {@code reverseSkip}, iteration ENDS here.
     * @param end index of the first char after the range to be searched within {@code in}, i.e.,
     *            the exclusive end of the range. Note that since the method is {@code reverseSkip},
     *            the first char to be evaluated for alphabet membership will be the one at
     *            {@code end-1}.
     * @param alphabet Set of chars to skip. Once a char outside this set is met, its index will
     *                 be returned. Use {@link Skip#alphabet(String...)} to create such set.
     *
     * @return the aforementioned {@code i} or {@code begin} if all chars in the range
     *         were present in {@code alphabet}.
     */
    public static @NonNegative int
    reverseSkip(CharSequence in, @NonNegative int begin, @NonNegative int end, long[] alphabet) {
        for (int i = end-1; i >= begin; --i) {
            char c = in.charAt(i);
            if (c < 128) {
                if ((alphabet[(c >> 6) & 1] & 1L << c) == 0) // c not in alphabet
                    return i;
            } else if ((alphabet[0] & 1) == 0) { // c is not ASCII but alphabet does not allow it
                return i;
            } // else: alphabet was inverted, thus all non-ASCII chars were added to it.
        }
        return begin;
    }

    /**
     * First {@code i >= begin && i < end} where {@code in[i]} is <strong>not</strong>
     * in {@code alphabet}.
     *
     * @param in string whose chars will be iterated
     * @param begin where the range to be searched within {@code in} starts. This will be
     *              the first character to be tested against {@code alphabet}
     * @param end non-inclusive end of the range to be searched within {@code in}, i.e., the
     *            index of the first char to not be tested against {@code alphabet}
     * @param alphabet set of chars to be skipped, i.e., the allowed chars. Once a char
     *                 that is not in this set is met, this method returns its index.
     *                 Use {@link Skip#alphabet(String...)} to create such set.
     *
     * @return Aforementioned {@code i} or {@code end}, if no such char was found.
     */
    public static @NonNegative int skip(CharSequence in, int begin, int end, long[] alphabet) {
        for (int i = begin; i < end; ++i) {
            char c = in.charAt(i);
            if (c < 128) {
                if ((alphabet[(c >> 6) & 1] & (1L << c)) == 0) // c not in alphabet
                    return i;
            } else if ((alphabet[0] & 1) == 0) { // c is not ASCII but alphabet does not allow it
                return i;
            } // else: alphabet was inverted, thus all non-ASCII chars were added to it.
        }
        return end;
    }

    /**
     * First {@code i >= begin} and {@code < end} such that {@code in[i] == c}.
     *
     * <p>This is faster than {@link Skip#skip(CharSequence, int, int, long[])} with an inverted alphabet.
     * However, if the caller can handle the {@code -1} return and {@code end == in.length},
     * {@link String#indexOf(int, int)} should be used instead.</p>
     *
     * @param in The string where to search
     * @param begin where to start (inclusive) the search in {@code in}
     * @param end where to stop (exclusive) the search in {@code in}
     * @param c character to search for
     * @return Aforementioned {@code i} or {@code end} if {@code c} was nto found
     *  */
    public static @NonNegative int skipUntil(String in, int begin, int end, char c) {
        int i = in.indexOf(c, begin);
        return i < 0 ? end : Math.min(i, end);
    }
}
