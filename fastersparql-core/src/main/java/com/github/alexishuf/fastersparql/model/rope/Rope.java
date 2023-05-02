package com.github.alexishuf.fastersparql.model.rope;

import com.github.alexishuf.fastersparql.sparql.expr.Term;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import java.util.*;

import static com.github.alexishuf.fastersparql.model.rope.ByteRope.EMPTY;
import static com.github.alexishuf.fastersparql.model.rope.ByteRope.EMPTY_UTF8;
import static java.lang.System.arraycopy;
import static java.lang.foreign.ValueLayout.JAVA_INT;
import static java.lang.foreign.ValueLayout.JAVA_LONG;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.nio.charset.StandardCharsets.UTF_8;

@SuppressWarnings("unused")
public abstract class Rope implements CharSequence, Comparable<Rope> {
    public int len;

    private static void raiseBadRange(int begin, int end) {
        if (end < begin)
            throw new IllegalArgumentException("negative range");
        throw new IndexOutOfBoundsException(begin < 0 ? begin : end);
    }

    private static IllegalArgumentException staticAccessorBadType(Object o) {
        return new IllegalArgumentException("Expected Rope, byte[] or CharSequence, got"+o);
    }

    public static String toString(Object o, int begin, int end) {
        return o instanceof byte[] a ? new String(a, begin, end-begin, UTF_8) : o.toString();
    }

    public static byte get(Object o, int i) {
        return switch (o) {
            case       byte[] a  -> a[i];
            case         Rope r  -> r.get(i);
            case CharSequence cs -> {
                char c = cs.charAt(i);
                if (c > 127) throw new IllegalArgumentException("Non-ASCII char at index"+i);
                yield (byte)c;
            }
            default -> throw staticAccessorBadType(o);
        };
    }

    public static boolean isEscaped(Object o, int begin, int i) {
        if (o instanceof Rope r) return r.isEscaped(begin, i);
        int not = i-1;
        switch (o) {
            case byte[] u8       -> { while (not >= begin && u8[not] == '\\') --not; }
            case CharSequence cs -> { while (not >= begin && cs.charAt(not) == '\\') --not; }
            default -> throw staticAccessorBadType(o);
        }
        return ((i-not) & 1) == 0;
    }

    public Rope(int len) { this.len = len; }

    /** Get the number of bytes in this {@link Rope}. */
    public final int len() { return len; }

    /**
     * Get the i-th UTF-8 byte in this {@link Rope}.
     *
     * @throws IndexOutOfBoundsException iff {@code i < 0} or {@code i >= len()}
     */
    public abstract byte get(int i);

    /**
     * If non-null, this is an array where bytes from {@link #backingArrayOffset()} ot
     * {@link #backingArrayOffset()}{@code + }{@link #len()} correspond to the UTF-8 bytes
     * of {@code this} rope.
     */
    public byte @Nullable[] backingArray() {
        if (this instanceof Term t && t.flaggedDictId == 0) return t.local;
        return null;
    }

    /**
     * If {@link #backingArray()} is not null, this is the index into it where the
     * {@link #len()} bytes of {@code this} rope are stored.
     */
    public int backingArrayOffset() {
        return 0;
    }

    /**
     * Get a {@code byte[]} with {@code length == end-begin} containing bytes from {@code begin} to
     * {@code end} (non-inclusive).
     */
    public final byte[] toArray(int begin, int end) {
        byte[] a = new byte[end-begin];
        return copy(begin, end, a, 0);
    }

    /**
     * Copy bytes from {@code begin} (inclusive) to {@code end} (non-inclusive) into {@code dest}
     * starting at {@code offset}.
     *
     * @param begin index of first byte to include
     * @param end non-inclusive end of byte range
     * @param dest {@code byte[]} where to copy bytes from {@code this}
     * @param offset {@code get(begin)} will be written to {@code dest[offset]} and so on.
     * @return {@code dest};
     */
    public byte[] copy(int begin, int end, byte[] dest, int offset) {
        if (end < begin || begin < 0 || end > len) raiseBadRange(begin, end);
        int len = end - begin;
        if (this instanceof Term t) {
            int fId = t.flaggedDictId;
            if (fId == 0) {
                arraycopy(t.local, begin, dest, offset, len);
            } else {
                byte[] f = fId < 0 ? t.local : RopeDict.get(fId&0x7fffffff).u8();
                int written = 0;
                if (begin < f.length)
                    arraycopy(f, begin, dest, offset, written += Math.min(end, f.length)-begin);
                if (end > f.length) {
                    byte[] s = fId < 0 ? RopeDict.get(fId&0x7fffffff).u8() : t.local;
                    int srcPos = Math.max(begin, f.length) - f.length;
                    arraycopy(s, srcPos, dest, offset+written, len - written);
                }
            }
        } else {
            while (begin < end) dest[offset++] = get(begin++);
        }
        return dest;
    }

    /** Equivalent to {@code out.write(toArray(0, len())); return len}. */
    public int write(OutputStream out) throws IOException {
        byte[] buf = new byte[128];
        for (int i = 0, end = len; i < end; i += 128) {
            int n = Math.min(end-i, 128);
            out.write(copy(i, i+n, buf, 0), 0, n);
        }
        return len;
    }

    /**
     * Get a {@link Rope} with only the chars in the {@code [begin, end)} range.
     *
     * @param begin index of the first char to include in the sub-{@link Rope}
     * @param end {@link Rope#len()} or first index to NOT include in the sub-{@link Rope}
     * @return a {@link Rope} {@code sub} with {@code len()==end-begin} where
     *         {@code sub.get(i) == this.get(begin+i)}.
     */
    public Rope sub(int begin, int end) {
        if (!(this instanceof Term t)) throw new UnsupportedOperationException();
        if (begin < 0 || end > len || end < begin) raiseBadRange(begin, end);
        if (begin == 0 && end == len) return this;
        int rLen = end-begin;
        if (t.flaggedDictId == 0) {
            return new ByteRope(t.local, begin, rLen);
        } else if (t.flaggedDictId > 0) {
            byte[] prefix = RopeDict.get(t.flaggedDictId).u8();
            if (begin >= prefix.length)
                return new ByteRope(t.local, begin - prefix.length, rLen);
            else if (end <= prefix.length)
                return new ByteRope(prefix, begin, rLen);
        } else {
            byte[] suffix = RopeDict.get(t.flaggedDictId & 0x7fffffff).u8();
            if (end <= t.local.length)
                return new ByteRope(t.local, begin, rLen);
            else if (begin >= t.local.length)
                return new ByteRope(suffix, begin - t.local.length, rLen);
        }
        // substring crosses shared/local segments
        return new ByteRope(toArray(begin, end));
    }

    public static List<Rope> ropeList(Object... items) {
        ArrayList<Rope> ropes = new ArrayList<>(items.length);
        for (Object obj : items) {
            switch (obj) {
                case Collection<?> coll -> coll.forEach(o -> ropes.add(of(o)));
                case Object[] a -> { for (var o : a) ropes.add(of(o)); }
                default -> ropes.add(of(obj));
            }
        }
        return ropes;
    }

    public static Set<Rope> ropeSet(Object... items) {
        LinkedHashSet<Rope> ropes = new LinkedHashSet<>(items.length);
        for (Object obj : items) {
            if (obj instanceof List<?> l)
                ropes.addAll(ropeList(l.toArray()));
            else
                ropes.add(obj instanceof Rope r ? r : new ByteRope(obj));
        }
        return ropes;
    }

    public static Rope of(Object o) {
        return o == null ? null : o instanceof Rope r ? r : new ByteRope(o);
    }

    public static ByteRope of(Object a0, Object a1) {
        Rope r0 = a0 instanceof Rope r ? r : new ByteRope(a0.toString());
        Rope r1 = a1 instanceof Rope r ? r : new ByteRope(a1.toString());
        return new ByteRope(r0.len() + r1.len()).append(r0).append(r1);
    }

    public static ByteRope of(Object a0, Object a1, Object a2) {
        Rope r0 = a0 instanceof Rope r ? r : new ByteRope(a0.toString());
        Rope r1 = a1 instanceof Rope r ? r : new ByteRope(a1.toString());
        Rope r2 = a2 instanceof Rope r ? r : new ByteRope(a2.toString());
        return new ByteRope(r0.len() + r1.len() + r2.len()).append(r0).append(r1).append(r2);
    }

    private static final Rope[] CHARS;

    static {
        Rope[] chars = new Rope[127];
        for (int i = 0; i < 127; i++)  //noinspection StaticInitializerReferencesSubClass
            chars[i] = new ByteRope(new byte[]{(byte)i});
        CHARS = chars;
    }

    /**
     * Equivalent to {@code new ByteRope(Stream.of(args).map(Object::toString).reduce(String::concat).orElse(""))}
     */
    public static ByteRope of(Object... args) {
        int size = 0;
        for (int i = 0; i < args.length; i++) {
            var arg = args[i];
            if (arg instanceof Character c && c < 128)
                args[i] = CHARS[c];
            else if (arg instanceof CharSequence cs && cs.length() == 1 && cs.charAt(0) < 128)
                args[i] = CHARS[cs.charAt(0)];
            else if (!(arg instanceof Rope))
                args[i] = new ByteRope(arg);
            size += ((Rope)args[i]).len();
        }
        ByteRope out = new ByteRope(size);
        for (Object a : args)
            out.append(((Rope)a));
        return out;
    }

    public static final int[] ANY = new int[] {-1, -1, -1, -1};
    /** An alphabet equivalent to the {@code [a-zA-Z0-9]} regex accepting only ASCII chars. */
    public static final int[] ALPHANUMERIC = alphabet("", Range.ALPHANUMERIC);
    /** An alphabet equivalent to the {@code [a-zA-Z]} regex accepting only ASCII chars. */
    public static final int[] LETTERS = alphabet("", Range.LETTER);
    /** An alphabet equivalent to the {@code [0-9]} regex accepting only ASCII chars. */
    public static final int[] DIGITS = alphabet("", Range.DIGIT);
    /** An alphabet for control characters and whitespace */
    public static final int[] WS = alphabet("", Range.WS);
    /** An alphabet everything that is not control characters nor whitespace */
    public static final int[] UNTIL_WS = invert(WS);
    /** An alphabet with all whitespace chars, except for {@code ' '}. */
    public static final int[] SPECIAL_WS = minus(WS, alphabet(" "));
    /** An alphabet with {@code ' '} and all non-non-whitespace chars. */
    public static final int[] UNTIL_SPECIAL_WS = invert(SPECIAL_WS);
    /** An alphabet for everything but '\n'. */
    public static final int[] UNTIL_LF = invert(alphabet("\n"));
    /** An alphabet for everything but {@code '"'}. */
    public static final int[] UNTIL_DQ = invert(alphabet("\""));
    /** An alphabet for everything but '"' and '\\'. */
    public static final int[] UNTIL_DQ_OR_BACKSLASH = invert(alphabet("\"\\"));
    /** An alphabet for ASCII upper case letters, i.e., {@code [A-Z]}*/
    public static final int[] UPPERCASE = alphabet("", Range.UPPER_LETTER);
    /** An alphabet for {@code [^A-Z]} */
    public static final int[] UNTIL_UPPERCASE = invert(UPPERCASE);
    /** An alphabet for ASCII lower case letters, i.e., {@code [a-z]} */
    public static final int[] LOWERCASE = alphabet("", Range.LOWER_LETTER);
    /** An alphabet for {@code [^a-z]} */
    public static final int[] UNTIL_LOWERCASE = invert(LOWERCASE);

    public enum Range {
        WS,
        DIGIT,
        UPPER_LETTER,
        LOWER_LETTER,
        LETTER,
        ALPHANUMERIC;

        private static final byte[][] CHARS = new byte[6][];
        static {
            byte[] ws = new byte[' '+1];
            byte[] digit = new byte[10];
            byte[] upperLetter = new byte['Z'-'A'+1];
            byte[] lowerLetter = new byte['z'-'a'+1];
            byte[] letter = new byte[lowerLetter.length + upperLetter.length];
            byte[] alphaNum = new byte[digit.length + letter.length];

            for (int i = 0; i <= ' ';      i++) ws[i] = (byte)i;
            for (int i = 0; i < 10;        i++) digit[i] = (byte)('0'+i);
            for (int i = 0; i < 'Z'-'A'+1; i++) upperLetter[i] = (byte)('A'+i);
            for (int i = 0; i < 'z'-'a'+1; i++) lowerLetter[i] = (byte)('a'+i);

            arraycopy(upperLetter, 0, letter, 0, upperLetter.length);
            arraycopy(lowerLetter, 0, letter, upperLetter.length, lowerLetter.length);

            arraycopy(digit, 0, alphaNum, 0, digit.length);
            arraycopy(letter, 0, alphaNum, digit.length, letter.length);

            CHARS[WS.ordinal()]           = ws;
            CHARS[DIGIT.ordinal()]        = digit;
            CHARS[UPPER_LETTER.ordinal()] = upperLetter;
            CHARS[LOWER_LETTER.ordinal()] = lowerLetter;
            CHARS[LETTER.ordinal()]       = letter;
            CHARS[ALPHANUMERIC.ordinal()]     = alphaNum;
        }
        /** Get an array with all bytes in this range */
        public byte[] chars() { return CHARS[ordinal()]; }
    }

    /**
     * Creates an alphabet for use with {@link Rope#skip(int, int, int[])}.
     *
     * @param ranges  for each {@link Range} add all chars in {@link Range#chars()}
     * @param chars   add all chars in the string, which must all be ASCII.
     * @return the alphabet as an array of {@code int}s.
     * @throws IllegalArgumentException if {@code chars} contains non-ASCII characters
     */
    public static int[] alphabet(String chars, Range... ranges) {
        int[] set = new int[4];
        for (Range range : ranges) {
            for (byte b : range.chars())
                set[b >> 5] |= 1 << b;
        }
        for (byte b : chars.getBytes(UTF_8)) {
            if (b < 0) throw new IllegalArgumentException("Non-ASCII char "+(0xff&b));
            if (b == 127) throw new IllegalArgumentException("DEL (127) cannot be used in this set as it signals whether non-ASCII chars are allowed");
            set[b>>5] |= (1 << b);
        }
        return set;
    }

    /**
     * Get a new alphabet that accepts anything in {@code alphabet} and also any non-ASCII char.
     *
     * @param alphabet the original alphabet
     * @return a copy of {@code alphabet} with the highest bit (in the last int) set.
     */
    public static int[] withNonAscii(int[] alphabet) {
        int[] with = Arrays.copyOf(alphabet, alphabet.length);
        with[3] |= 0x80000000;
        return with;
    }

    /** Get a new alphabet for all chars not present in {@code alphabet}. */
    public static int[] invert(int[] alphabet) {
        int[] inverted = new int[alphabet.length];
        for (int i = 0; i < alphabet.length; i++)
            inverted[i] = ~alphabet[i];
        return inverted;
    }


    /** Get a new alphabet with all chars in {@code left} that are not present in {@code right}. */
    public static int[] minus(int[] left, int[] right) {
        int[] out = new int[left.length];
        for (int i = 0; i < out.length; i++)
            out[i] = left[i] & ~right[i];
        return out;
    }

    /** Whether byte {@code c} is accepted by {@code alphabet}. */
    public static boolean contains(int[] alphabet, byte c) {
        return c > 0 ? (alphabet[c>>5] & (1<<c)) != 0 : (alphabet[3]&0x80000000) != 0;
    }

    /**
     * Find next {@code i} where {@code get(i) == '\n' || has(i, "\r\n")} and return
     * {@code n<<32 | i} where {@code n} is the number of bytes in the line-end sequence.
     *
     * @param begin where to start the search for a line break
     * @param end index where the last line may end. The line break cannot start at or
     *            after {@code end}
     * @return a long combining two ints: the number of bytes in the line break is stored in bits
     *         {@code [32,64)}, i.e., {@code retVal>>>32}, and the index where the line break
     *         starts is stored in bits {@code [0,32)}, i.e., {@code (int)retVal}.
     */
    public final long skipUntilLineBreak(int begin, int end) {
        for (int i = begin; (i = skipUntil(i, end, '\r', '\n')) != end; ++i) {
            byte c = get(i);
            if (c == '\n') return (1L<<32) | i;
            if (c == '\r' && i + 1 < end && get(i + 1) == '\n') return (2L<<32) | i;
        }
        return end;
    }

    /**
     * Equivalent to {@code skipUntil(begin, end, c0, c0)}.
     */
    public int skipUntil(int begin, int end, char c0) { return skipUntil(begin, end, c0, c0); }

    /** Whether the byte at index {@code i} is under effect of two-byte escape sequence where the
     * escape char is {@code escape} and the search for escape chars should not progress before
     * {@code begin}.
     *
     * @param i index fo the byte being tested for "is escaped"
     * @return whether {@code i} is preceeded by a odd number of escape bytes.
     */
    public boolean isEscaped(int begin, int i) {
        int not = i - 1;
        while (not >= begin && get(not) == '\\') --not;
        return ((i - not) & 1) == 0;
    }
    public boolean isEscaped(int i) { return isEscaped(0, i); }

    /**
     * Get the first {@code i >= begin} and {@code < end} where {@code get(i) == c} and
     * {@code c} is not preceded by {@code escape} or is preceded by an even number
     * of {@code escape} occurrences (cancelling themselves out).
     *
     * @param begin first possible value for {@code i} mentioned above
     * @param end non-inclusive upper bound for {@code i} mentioned above
     * @param c the ASCII char being searched for
     * @return The aforementioned {@code i} or {@code end} if no unescaped {@code c} was
     *         found in {@code [begin, end)}
     * @throws IndexOutOfBoundsException if {@code begin < 0} or {@code end > len()}
     */
    public int skipUntilUnescaped(int begin, int end, char c) {
        int i = begin;
        while ((i=skipUntil(i, end, c)) < end && (get(i) != c || isEscaped(i)))
            ++i;
        return i;
    }

    /**
     * Get the first {@code i >= begin} and {@code < end} where {@code has(i, sequence)} and
     * {@code get(i)} is not preceded by an odd number of consecutive {@code esc} bytes.
     *
     * @param begin where to start the search for {@code sequence}, this is the inclusive
     *              lower bound for {@code i}
     * @param end non-inclusive end of the range where {@code sequence} must be found. The
     *            lower bound for {@code i} is {@code end-sequence.length}.
     * @param sequence sequence of bytes to search for
     * @return The aforementioned {@code i} or {@link Rope#len()} if there is no such {@code i}.
     */
    public final int skipUntilUnescaped(int begin, int end, byte[] sequence) {
        char f = (char)(0xff&sequence[0]);
        int i = begin, last = end-sequence.length;
        while (i <= last && (i = skipUntilUnescaped(i, last, f)) <= last && !has(i, sequence))
            ++i;
        return i > last ? end : i;
    }

    /**
     * If {@code !contains(allowedFirst, get(begin))}, return {@code begin}, else
     * {@code skipUntil(begin+1, end, c)}.
     */
    public final int requireThenSkipUntil(int begin, int end, int[] allowedFirst, char c) {
        if (end <= begin) return begin;
        byte first = get(begin);
        boolean ok = first > 0 ? (allowedFirst[first >> 5] & (1 << first)) != 0
                               : (allowedFirst[3] & 0x80000000) != 0;
        return ok ? skipUntil(begin+1, end, c) : begin;
    }

    /**
     * Finds the first {@code i} in {@code [begin, end)} where {@code get(i)} is {@code c0}
     * or {@code c1}.
     *
     * <p>Implementations will will try to used SIMD instructions. Thus, on most machines this
     * will be slower than {@link Rope#skip(int, int, int[])} if {@code i < 32}.</p>
     *
     * @param begin first candidate for {@code i}
     * @param end {@link Rope#len()} or the first index after the last candidate for {@code i}
     * @param c0 One of the allowed chars for {@code get(i)}
     * @param c1 Another of the allowed chars for {@code get(i)}
     * @return The aforementioned {@code i} or {@code end} if there is no such {@code i}
     */
    public int skipUntil(int begin, int end, char c0, char c1) {
        if (end < begin || begin < 0 || end > len) raiseBadRange(begin, end);
        for (byte c; begin < end && (c=get(begin)) != c0 && c != c1;) ++begin;
        return begin;
    }

    /**
     * Finds the first {@code i} in {@code [begin, end)} where for every {@code j} in
     * {@code [0, sequence.length)} {@code get(i+j) == sequence[j]}.
     *
     * <p>Implementations will will try to used SIMD instructions. Thus, on most machines this
     * will be slower than {@link Rope#skip(int, int, int[])} if {@code i < 32}.</p>
     *
     * @param begin first candidate for {@code i}
     * @param end {@link Rope#len()} or {@code m+sequence.length} where {@code m} is the maximum
     *            allowed value for {@code i} (inclusive)
     * @param sequence Sequence of UTF-8 bytes to search for
     * @return The aforementioned {@code i} or {@code end} if there is no such {@code i}
     */
    public final int skipUntil(int begin, int end, byte[] sequence) {
        char f = (char)(0xff&sequence[0]);
        int lastBegin = end - sequence.length;
        while ((begin = skipUntil(begin, end, f)) <= lastBegin && !has(begin, sequence))
            ++begin;
        return begin > lastBegin ? end : begin;
    }

    /**
     * Finds the first {@code i} in {@code [begin, end)} where for every {@code j} in
     * {@code [0, sequence.length)} {@code get(i+j) == sequence[j]}.
     *
     * <p>Implementations will will try to used SIMD instructions. Thus, on most machines this
     * will be slower than {@link Rope#skip(int, int, int[])} if {@code i < 32}.</p>
     *
     * @param begin first candidate for {@code i}
     * @param end {@link Rope#len()} or {@code m+sequence.length} where {@code m} is the maximum
     *            allowed value for {@code i} (inclusive)
     * @param sequence Sequence of UTF-8 bytes to search for
     * @return The aforementioned {@code i} or {@code end} if there is no such {@code i}
     */
    public final int skipUntil(int begin, int end, Rope sequence) {
        char f = (char) sequence.get(0);
        int lastBegin = end-sequence.len();
        while ((begin = skipUntil(begin, end, f)) <= lastBegin && !has(begin, sequence))
            ++begin;
        return begin > lastBegin ? end : begin;
    }

    /** Equivalent to {@code skipUntilLast(begin, end, c0, c0)}. */
    public int skipUntilLast(int begin, int end, char c0) {
        return skipUntilLast(begin, end, c0, c0);
    }

    /**
     * Similar to {@link Rope#skipUntil(int, int, char, char)} but finds the
     * <strong>LAST</strong> {@code i}.
     */
    public int skipUntilLast(int begin, int end, char c0, char c1) {
        if (end < begin || begin < 0 || end > len) raiseBadRange(begin, end);
        if (!(this instanceof Term t))
            throw new UnsupportedOperationException();
        byte[] snd = t.flaggedDictId >= 0 ? t.local : RopeDict.get(t.flaggedDictId&0x7fffffff).u8();
        byte[] fst = t.flaggedDictId <  0 ? t.local
                   : (t.flaggedDictId == 0  ? EMPTY_UTF8 : RopeDict.get(t.flaggedDictId).u8());
        if (end > fst.length) {
            int i = Math.min(end, len) - 1 - fst.length;
            for (byte c; i >= 0 && (c = snd[i]) != c0 && c != c1; ) --i;
            if (i >= 0) return i + fst.length;
        }
        if (begin < fst.length) {
            int i = Math.min(end, fst.length) - 1;
            for (byte c; i >= 0 && (c = fst[i]) != c0 && c != c1; ) --i;
            if (i >= 0) return i;
        }
        return end;
    }

    /**
     * Similar to {@link Rope#skipUntil(int, int, byte[])} but finds the
     * <strong>LAST</strong> {@code i}.
     */
    public final int skipUntilLast(int begin, int end, byte[] sequence) {
        char first = (char)(sequence[0]&0xff);
        int notFound = end;
        for (int i; end-begin > sequence.length; end = i) {
            i = skipUntilLast(begin, end, first);
            if      (i == end)         return notFound;
            else if (has(i, sequence)) return i;
        }
        return notFound;
    }

    /**
     * Similar to {@link Rope#skipUntil(int, int, byte[])} but finds the
     * <strong>LAST</strong> {@code i}.
     */
    public final int skipUntilLast(int begin, int end, Rope sequence) {
        char first = (char)(sequence.get(0)&0xff);
        int notFound = end;
        for (int i, len = sequence.len(); end-begin > len; end = i) {
            i = skipUntilLast(begin, end, first);
            if      (i == end)         return notFound;
            else if (has(i, sequence)) return i;
        }
        return notFound;
    }

    /**
     * Get the first {@code i} in {@code [begin, end)} where {@code get(i)} is <strong>NOT</strong>
     * in {@code alphabet}.
     *
     * <p>To obtain behavior akin to {@link Rope#skipUntil(int, int, char)},
     * {@link Rope#invert(int[])} the alphabet.</p>
     *
     * <p>Implementations of this method shall not use vectorization and thus
     * will be faster than the {@code skip*()} counterpart when the expected result is close
     * to {@code begin}</p>
     *
     * @param begin first char to be considered for membership in {@code alphabet}.
     * @param end first char (or {@code len()}) to not test against {@code alphabet}.
     * @param alphabet set of ASCII chars to be skipped during the search. If the 127th bit in
     *                 {@code alphabet} (i.e., {@code alphabet[3] >> 31}) is set, any non-ASCII
     *                 byte will be considered member of {@code alphabet} and the search will
     *                 continue
     * @return The aforementioned {@code i}, or {@code end} if {@code begin >= end}, or no
     *         {@code i < Math.min(len(), end)} satisfying the criteria was found.
     * @throws IllegalArgumentException if {@code begin < 0}.
     */
    public int skip(int begin, int end, int[] alphabet) {
        if (!(this instanceof Term t))
            throw new UnsupportedOperationException();
        if (begin < 0 || end > len || end < begin)
            raiseBadRange(begin, end);
        byte[] fst = t.flaggedDictId > 0 ? RopeDict.get(t.flaggedDictId).u8() : t.local;
        byte[] snd = t.flaggedDictId < 0 ? RopeDict.get(t.flaggedDictId&0x7fffffff).u8()
                   : (t.flaggedDictId == 0 ? EMPTY.u8() :  t.local);
        if (begin < fst.length) {
            int physEnd = Math.min(end, fst.length);
            int i = RopeSupport.skip(fst, begin, physEnd, alphabet);
            if (i != physEnd) return i;
        }
        if (end > fst.length) {
            int physBegin = Math.max(0, begin-fst.length);
            return RopeSupport.skip(snd, physBegin, end-fst.length, alphabet)+fst.length;
        }
        return end;
    }

    /** Equivalent to {@code skip(begin, end, Rope.WS)}. */
    public int skipWS(int begin, int end) {
        for (byte c; begin < end && (c=get(begin)) <= ' ' && c >= 0; ) ++begin;
        return begin;
    }

    /** Equivalent to {@code reverseSkip(begin, end, Rope.WS)+1}. */
    public final int rightTrim(int begin, int end) {
        while (end > begin && get(end-1) <= ' ') --end;
        return end;
    }

    public static boolean in(char c, int[] alphabet) {
        return ((c & 0x7f) == c ? alphabet[c>>5] & (1 << c) : alphabet[3]&0x80000000) != 0;
    }

    /**
     * Similar to {@code get(skip(begin, end, alphabet))}, but returns '\0' if
     * {@link Rope#skip(int, int, int[])} returns {@code end}.
     */
    public final char skipAndGet(int begin, int end, int[] alphabet) {
        int i = skip(begin, end, alphabet);
        if (i == end) return '\0';
        byte c = get(i);
        return c < 0 ? '\uFFFD' : (char) c;
    }

    /**
     * Find the largest {@code i} in {@code [begin, end)} where {@code get(i)} is
     * <strong>NOT</strong> in {@code alphabet}.
     *
     * @param begin index of the lowest byte to check for membership in {@code alphabet}.
     * @param end index of the byte to right of the first to be checked for membership
     *            in {@code alphabet}
     * @param alphabet set of allowed characters that will be skipped while moving from
     *                 {@code end-1} to {@code begin}. If the 127th bit (i.e.,
     *                 {@code alphabet[3]>>31}) is set, non-ASCII bytes will be treated as
     *                 if present in {@code alphabet}.
     * @return the largest {@code i} mentioned above or {@code begin} if
     *         {@code begin >= end}, {@code begin >= len()} or if {@code get(i)} is in {@code alphabet}
     *         for all {@code i} in {@code [begin, end)}.
     */
    public final int reverseSkip(int begin, int end, int[] alphabet) {
        if (begin < 0 || end > len || end < begin)
            raiseBadRange(begin, end);
        boolean stopOnNonAscii = (alphabet[3] & 0x80000000) == 0;
        for (int i = Math.min(len, end) - 1; i >= begin; --i) {
            byte c = get(i);
            if (c > 0) { // c is ASCII
                if ((alphabet[c >> 5] & (1 << c)) == 0)
                    return i; // c is not in alphabet
            } else if (stopOnNonAscii) {
                return i; // non-ASCII  not allowed by alphabet
            }
        }
        return begin;
    }

    public int reverseSkipUntil(int begin, int end, char c) {
        if (begin < 0 || end > len || end < begin) raiseBadRange(begin, end);
        --end;
        while (end >= begin && get(end) != c) --end;
        return Math.max(begin, end);
    }

    /** Whether {@code get(i) == seq[i]} for every {@code i} in {@code [begin, end)}. */
    public boolean has(int position, byte[] seq) {
        if (position < 0) throw new IndexOutOfBoundsException(position);
        if (position+seq.length > len) return false;
        for (byte b : seq) {
            if (b != get(position++)) return false;
        }
        return true;
    }

    /** Whether {@code sub(pos, pos+(end-begin)).equals(rope.sub(begin, end))}. */
    public boolean has(int pos, Rope rope, int begin, int end) {
        int rLen = end - begin;
        if (pos < 0) throw new IndexOutOfBoundsException(pos);
        if (pos+rLen > this.len) return false;
        while (begin < end) {
            if (get(pos++) != rope.get(begin++)) return false;
        }
        return true;
    }

    /** Whether {@code sub(position, position+rope.len()).equals(rope.sub(begin, end))}. */
    public final  boolean has(int position, Rope rope) {
        return has(position, rope, 0, rope.len);
    }

    /** Similar to {@link Rope#has(int, byte[])} but */
    public boolean hasAnyCase(int position, byte[] uppercaseSequence) {
        if (position < 0) throw new IndexOutOfBoundsException(position);
        if (position+uppercaseSequence.length > len) return false;
        for (byte expected : uppercaseSequence) {
            byte actual = get(position++);
            if (actual != expected && ((actual < 'a' || actual > 'z') || actual - 32 != expected))
                return false;
        }
        return true;
    }

    private static final boolean IS_LE = ByteOrder.nativeOrder()== LITTLE_ENDIAN;
    private static final long LSB_MASK_L = 0x0101010101010101L;
    private static final int  LSB_MASK_I = 0x01010101;
    private static final ValueLayout.OfLong LONG_UNALIGNED = JAVA_LONG.withBitAlignment(8);
    private static final ValueLayout.OfInt INT_UNALIGNED = JAVA_INT.withBitAlignment(8);

    /**
     * Compute a hash value with {@code Math.min(32, end-begin)} bits where bit
     * {@code i} is {@code get(begin+i)&0x1}.
     *
     * @param begin index of the leftmost byte to include in the hash
     * @param end {@code len()} or index of first byte right of the byte sequence used to hash.
     * @return an int with the lowest {@code end-begin} bits set as described above.
     */
    public int lsbHash(int begin, int end) {
        if (end < begin || begin < 0 || end > len) raiseBadRange(begin, end);
        int h = 0, bit = 0, bits = end-begin;
        if (bits > 32)
            begin = end-(bits = 32);
        while (bit < bits)
            h |= (get(begin + bit) & 1) << bit++;
        return h;
    }

    private Rope convertCase(int[] until, int offset) {
        int len = len();
        if (skip(0, len, until) == len)
            return this;
        byte[] u8 = toArray(0, len);
        for (int b = 0; b < u8.length; ++b) {
            if ((b = RopeSupport.skip(u8, b, len, until)) != len)
                u8[b] += offset;
        }
        return new ByteRope(u8);
    }

    /**
     * Get {@code this} or a copy with all lower-case ASCII bytes replaced with their
     * ASCII upper case counterparts
     */
    public Rope toAsciiUpperCase() { return convertCase(UNTIL_LOWERCASE, 'A'-'a'); }

    /**
     * Get {@code this} or a copy with all upper-case ASCII bytes replaced with their
     * ASCII lower case counterparts
     */
    public Rope toAsciiLowerCase() { return convertCase(UNTIL_UPPERCASE, 'a'-'A'); }

    /** Analogous to {@link String#trim()} */
    public Rope trim() {
        int end = rightTrim(0, len());
        return sub(skipWS(0, end), end);
    }

    /**
     * Parse a sequence of digits at index {@code begin}, optionally prefixed by '+' or '-' as
     * a java {@code long}.
     */
    public long parseLong(int begin) {
        byte sign = get(begin);
        if      (sign == '+')                { sign =  1; ++begin; }
        else if (sign == '-')                { sign = -1; ++begin; }
        else if (sign >= '0' && sign <= '9') { sign =  1; }
        else throw badNumber(begin);

        long val = 0, power = 1;
        for (int i = skip(begin+1, len(), DIGITS)-1; i >= begin; --i, power *= 10)
            val += power*(get(i)-'0');
        return val * sign;
    }

    public int parseCodePoint(int begin) {
        if (get(begin) == '\\') ++begin;
        int digits = switch (get(begin++)) {
            case 'u' -> 4;
            case 'U' -> 8;
            default -> throw new IllegalArgumentException("No unicode escape at index");
        };
        if (begin+digits > len) throw new IllegalArgumentException("Unicode escape truncated");
        int code = 0;
        for (int weight = 1 << (4*(digits-1)), v; weight > 0; weight >>= 4) {
            byte c = get(begin++);
            if ((v = c-'0') > 10) v = 10 + ((c-'A')&31);
            if (v < 0 || v > 15)
                throw new IllegalArgumentException("Non-hex digit in unicode escape");
            code += v * weight;
        }
        return code;
    }

    private NumberFormatException badNumber(int begin) {
        return new NumberFormatException("Expected sign or integer" +": "+sub(begin, skip(begin, len(), UNTIL_WS)));
    }

    public boolean isAscii() {
        int i = 0, len = this.len;
        while (i < len && get(i) >= 0) ++i;
        return i == len;
    }

    @Override public int length() { return len; }

    @Override public char charAt(int index) {
        int i = 0, len = len();
        if (index == len-1) {
            byte c = get(index);
            if (c > 0) return (char)c;
        }
        while (i < index && i < len && get(i) > 0) ++i;
        return i == index ? (char) get(i) : toString().charAt(index);
    }

    @Override public CharSequence subSequence(int start, int end) { return sub(start, end); }

    public String toString(int begin, int end) {
        byte[] u8 = new byte[end - begin]; // manual-inlining toArray() improves escape analysis
        copy(begin, end, u8, 0);
        return new String(u8, UTF_8);
    }

    @Override public int hashCode() {
        int len = len();
        int h = 0;
        for (int i = 0; i < len; i++)
            h = 31*h + (get(i)&0xff);
        return h;
    }

    @Override public boolean equals(Object o) {
        if (o == this) return true;
        if (!(o instanceof Rope r)) {
            if (o instanceof CharSequence)
                throw new UnsupportedOperationException("equals() between Rope and CharSequence not implemented");
            return false;
        }
        return r.len() == len && has(0, r, 0, len);
    }

    @Override public int compareTo(@NonNull Rope o) {
        int common = Math.min(len, o.len), i = 0, diff = 0;
        while (i < common && (diff = get(i) - o.get(i)) == 0) ++i;
        return diff == 0 ? len - o.len : diff;
    }

    /** Equivalent to {@link #compareTo(Rope)} with {@code o.sub(begin, end}. */
    public int compareTo(Rope o, int begin, int end) {
        int oLen = end-begin, common = Math.min(len, oLen), i = 0, diff = 0;
        while (i < common && (diff = get(i)-o.get(begin++)) == 0) ++i;
        return diff == 0 ? len - oLen : diff;
    }

    @Override public @NonNull String toString() {
        return new String(toArray(0, len()), UTF_8);
    }
}
