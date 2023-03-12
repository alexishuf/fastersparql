package com.github.alexishuf.fastersparql.model.rope;

import com.github.alexishuf.fastersparql.sparql.expr.Term;
import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.Vector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorSpecies;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;

import static com.github.alexishuf.fastersparql.model.rope.ByteRope.EMPTY;
import static java.lang.System.arraycopy;
import static java.nio.charset.StandardCharsets.UTF_8;
import static jdk.incubator.vector.ByteVector.fromArray;
import static jdk.incubator.vector.ByteVector.fromByteBuffer;
import static jdk.incubator.vector.VectorOperators.LE;
import static jdk.incubator.vector.VectorOperators.MIN;

@SuppressWarnings("unused")
public abstract class Rope implements CharSequence, Comparable<Rope> {
    private static final VectorSpecies<Byte> B_SP = ByteVector.SPECIES_PREFERRED;
    private static final int B_LEN = B_SP.length();
    private static final ByteVector ZERO = ByteVector.zero(B_SP);
    private static final ByteVector IOTA = ByteVector.zero(B_SP).addIndex(1);
    private static final ByteBuffer EMPTY_BB = ByteBuffer.wrap(new byte[0]);
    private static final ByteOrder EMPTY_BO = EMPTY_BB.order();

    private int checkRangeAndGetLen(int begin, int end) {
        int len = len();
        if (begin < 0 || end > len)
            throw new IndexOutOfBoundsException(begin < 0 ? begin : end);
        if (end < begin)
            throw new IllegalArgumentException("negative range");
        return len;
    }
    private int checkIndexAndGetLen(int pos) {
        int len = len();
        if (pos < 0 || pos >= len)
            throw new IndexOutOfBoundsException(pos);
        return len;
    }
    private int laxCheckIndexAndGetLen(int pos) {
        int len = len();
        if (pos < 0 || pos > len)
            throw new IndexOutOfBoundsException(pos);
        return len;
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
            case     ByteRope r  -> r.utf8[r.offset+i];
            case   BufferRope r  -> r.buffer.get(r.buffer.position()+i);
            case         Term t  -> t.get(i);
            case CharSequence cs -> {
                char c = cs.charAt(i);
                if (c > 127) throw new IllegalArgumentException("Non-ASCII char at index"+i);
                yield (byte)c;
            }
            default -> throw staticAccessorBadType(o);
        };
    }

    private static boolean isEscaped(byte[] u8, int begin, int i) {
        int not = i-1;
        while (not >= 0 && u8[not] == '\\') --not;
        return ((i-not) & 1) == 0;
    }

    private static boolean isEscaped(ByteBuffer bb, int begin, int i) {
        int not = i-1;
        while (not >= 0 && bb.get(not) == '\\') --not;
        return ((i-not) & 1) == 0;
    }

    public static boolean isEscaped(Object o, int begin, int i) {
        int not = i-1;
        switch (o) {
            case byte[] u8 -> { while (not >= begin && u8[not] == '\\') --not; }
            case ByteRope r -> {
                byte[] u8 = r.utf8;
                not += r.offset; i += r.offset; begin += r.offset;
                while (not >= begin && u8[not] == '\\') --not;
            }
            case BufferRope r -> {
                ByteBuffer bb = r.buffer;
                int pos = bb.position();
                not += pos; i += pos; begin += pos;
                while (not >= begin && bb.get(not) == '\\') --not;
            }
            case Rope r -> { while (not >= begin && r.get(not) == '\\') --not; }
            case CharSequence cs -> { while (not >= begin && cs.charAt(not) == '\\') --not; }
            default -> throw staticAccessorBadType(o);
        }
        return ((i-not) & 1) == 0;
    }

    /** Get the number of bytes in this {@link Rope}. */
    public final int len() {
        return switch (this) {
            case ByteRope r -> r.len;
            case BufferRope r -> r.buffer.remaining();
            case Term t -> t.local.length
                        + (t.flaggedDictId==0 ? 0 : RopeDict.get(t.flaggedDictId&0x7fffffff).len);
            default -> throw new UnsupportedOperationException();
        };
    }

    /**
     *  Get the i-th UTF-8 byte in this {@link Rope}.
     *
     * @throws IndexOutOfBoundsException iff {@code i < 0} or {@code i >= len()}
     */
    public final byte get(int i) {
        checkIndexAndGetLen(i);
        return switch (this) {
            case ByteRope r -> r.utf8[r.offset+i];
            case BufferRope r -> r.buffer.get(r.buffer.position()+i);
            case Term t -> {
                int id = t.flaggedDictId;
                byte[] fst = id > 0 ? RopeDict.get(id           ).utf8 : t.local;
                byte[] snd = id < 0 ? RopeDict.get(id&0x7fffffff).utf8 : t.local;
                yield i < fst.length ? fst[i] : snd[i-fst.length];
            }
            default -> throw new UnsupportedOperationException();
        };
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
    public final byte[] copy(int begin, int end, byte[] dest, int offset) {
        checkRangeAndGetLen(begin, end);
        int len = end - begin;
        switch (this) {
            case   ByteRope r -> arraycopy(r.utf8, r.offset+begin, dest, offset, len);
            case BufferRope r -> r.buffer.get(r.buffer.position()+begin, dest, offset, len);
            case Term t -> {
                int fId = t.flaggedDictId;
                if (fId == 0) {
                    arraycopy(t.local, begin, dest, offset, len);
                } else {
                    byte[] f = fId < 0 ? t.local : RopeDict.get(fId&0x7fffffff).utf8;
                    int written = 0;
                    if (begin < f.length)
                        arraycopy(f, begin, dest, offset, written += Math.min(end, f.length)-begin);
                    if (end > f.length) {
                        byte[] s = fId < 0 ? RopeDict.get(fId&0x7fffffff).utf8 : t.local;
                        int srcPos = Math.max(begin, f.length) - f.length;
                        arraycopy(s, srcPos, dest, offset+written, len - written);
                    }
                }
            }
            default           -> {while (begin < end) dest[offset++] = get(begin++); }
        }
        return dest;
    }

    /** Equivalent to {@code out.write(toArray(0, len())); return len}. */
    public final int write(OutputStream out) throws IOException {
        return switch (this) {
            case ByteRope r -> { out.write(r.utf8, r.offset, r.len); yield r.len; }
            case BufferRope r -> {
                ByteBuffer b = r.buffer;
                int len = b.remaining();
                if (b.hasArray()) out.write(b.array(), b.arrayOffset()+b.position(), len);
                else              out.write(toArray(0, len));
                yield len;
            }
            case Term t -> {
                int fId = t.flaggedDictId;
                byte[] lc = t.local, sh = fId==0 ? EMPTY.utf8 : RopeDict.get(fId&0x7fffffff).utf8;
                out.write(fId > 0 ? sh : lc);
                out.write(fId > 0 ? lc : sh);
                yield sh.length + lc.length;
            }
            default -> throw new UnsupportedOperationException();
        };
    }

    /**
     * Get a {@link Rope} with only the chars in the {@code [begin, end)} range.
     *
     * @param begin index of the first char to include in the sub-{@link Rope}
     * @param end {@link Rope#len()} or first index to NOT include in the sub-{@link Rope}
     * @return a {@link Rope} {@code sub} with {@code len()==end-begin} where
     *         {@code sub.get(i) == this.get(begin+i)}.
     */
    public final Rope sub(int begin, int end) {
        if (begin == 0 && end == checkRangeAndGetLen(begin, end)) return this;
        int len = end-begin;
        return switch (this) {
            case ByteRope   r -> new   ByteRope(r.utf8, r.offset+begin, len);
            case BufferRope r -> new BufferRope(r.buffer.slice(r.buffer.position()+begin, len));
            case Term       t -> subTerm(begin, end, t);
            default -> throw new UnsupportedOperationException();
        };
    }

    private ByteRope subTerm(int begin, int end, Term t) {
        int len = end-begin;
        if (t.flaggedDictId == 0) {
            return new ByteRope(t.local, begin, len);
        } else if (t.flaggedDictId > 0) {
            byte[] prefix = RopeDict.get(t.flaggedDictId).utf8;
            if (begin >= prefix.length)
                return new ByteRope(t.local, begin - prefix.length, len);
            else if (end <= prefix.length)
                return new ByteRope(prefix, begin, len);
        } else {
            byte[] suffix = RopeDict.get(t.flaggedDictId & 0x7fffffff).utf8;
            if (end <= t.local.length)
                return new ByteRope(t.local, begin, len);
            else if (begin >= t.local.length)
                return new ByteRope(suffix, begin - t.local.length, len);
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
    public final int skipUntil(int begin, int end, char c0) {
        int i, lane;
        checkRangeAndGetLen(begin, end);
        Vector<Byte> c0Vec = B_SP.broadcast(c0);
        switch (this) {
            case   ByteRope r -> {
                byte[] u8 =  r.utf8;
                end += (i = r.offset);
                i = begin += i;
                for (int b = begin+B_SP.loopBound(end-begin); i < b; i += B_SP.length()) {
                    if ((lane = fromArray(B_SP, u8, i).eq(c0Vec).firstTrue()) < B_LEN)
                        return i+lane-r.offset;
//                    ByteVector vec = fromArray(B_SP, u8, i);
//                    byte candidate = IOTA.reduceLanes(MIN, vec.eq(c0Vec));
//                    if (candidate != Byte.MAX_VALUE)
//                        return i+candidate-r.offset;
                }
                while (i < end && u8[i] != c0) ++i;
                return i-r.offset;
            }
            case BufferRope r -> {
                ByteBuffer bb = r.buffer;
                ByteOrder bo = bb.order();
                end += (i = bb.position());
                i = begin += i;
                for (int b = begin+B_SP.loopBound(end-begin); i < b; i += B_SP.length()) {
                    if ((lane = fromByteBuffer(B_SP, bb, i, bo).eq(c0Vec).firstTrue()) < B_LEN)
                        return i+lane-bb.position();
//                    ByteVector vec = fromByteBuffer(B_SP, bb, i, bo);
//                    byte candidate = IOTA.reduceLanes(MIN, vec.eq(c0Vec));
//                    if (candidate != Byte.MAX_VALUE)
//                        return i+candidate-bb.position();
                }
                while (i < end && bb.get(i) != c0) ++i;
                return i-bb.position();
            }
            default -> {
                while (begin < end && get(begin) != c0) ++begin;
                return begin;
            }
        }
    }

    /** Whether the byte at index {@code i} is under effect of two-byte escape sequence where the
     * escape char is {@code escape} and the search for escape chars should not progress before
     * {@code begin}.
     *
     * @param i index fo the byte being tested for "is escaped"
     * @return whether {@code i} is preceeded by a odd number of escape bytes.
     */
    public boolean isEscaped(int i) {
        return isEscaped(this, 0, i);
    }

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
    public final int skipUntilUnescaped(int begin, int end, char c) {
        checkRangeAndGetLen(begin, end);
        int i = begin, offset;
        Vector<Byte> c0Vec = B_SP.broadcast(c);
        switch (this) {
            case   ByteRope r -> {
                byte[] u8 = r.utf8;
                end += offset = r.offset;
                i = begin += offset;
                for (int b = begin+B_SP.loopBound(end-begin); i < b; i += B_SP.length()) {
                    VectorMask<Byte> matches = fromArray(B_SP, u8, i).eq(c0Vec);
                    ByteVector iota = IOTA;
                    while (true) {
                        byte m = iota.reduceLanes(MIN, matches);
                        if      (m == 127)                     break;
                        else if (!isEscaped(u8, begin, i+m)) return i+m-offset;
                        iota = iota.withLane(m, Byte.MAX_VALUE);
                    }
                }
                while (i < end && (u8[i] != c || isEscaped(u8, offset, i))) ++i;
                return i-offset;
            }
            case BufferRope r -> {
                var bb = r.buffer;
                var bo = bb.order();
                end += offset  = bb.position();
                i = begin += offset;
                for (int b = begin + B_SP.loopBound(end - begin); i < b; i += B_SP.length()) {
                    VectorMask<Byte> matches = fromByteBuffer(B_SP, bb, i, bo).eq(c0Vec);
                    ByteVector iota = IOTA;
                    while (true) {
                        byte m = iota.reduceLanes(MIN, matches);
                        if (m == 127) break;
                        else if (!isEscaped(bb, offset, i+m)) return i+m-offset;
                        iota = iota.withLane(m, Byte.MAX_VALUE);
                    }
                }
                while (i < end && (bb.get(i) != c || isEscaped(bb, offset, i))) ++i;
                return i-offset;
            }
            default -> {
                while ((i=skipUntil(i, end, c)) < end && (get(i) != c || isEscaped(i)))
                    ++i;
                return i;
            }
        }
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
        while ((i = skipUntilUnescaped(i, end, f)) <= last && !has(i, sequence))
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
    public final int skipUntil(int begin, int end, char c0, char c1) {
        int i, lane;
        checkRangeAndGetLen(begin, end);
        Vector<Byte> c0Vec = B_SP.broadcast(c0), c1Vec = B_SP.broadcast(c1);
        switch (this) {
            case   ByteRope r -> {
                byte[] u8 =  r.utf8;
                end += (i = r.offset);
                i = begin += i;
                for (int b = begin+B_SP.loopBound(end-begin); i < b; i += B_SP.length()) {
                    ByteVector vec = fromArray(B_SP, u8, i);
                    if ((lane = vec.eq(c0Vec).or(vec.eq(c1Vec)).firstTrue()) < B_LEN)
                        return i+lane-r.offset;
//                    byte candidate = IOTA.reduceLanes(MIN, vec.eq(c0Vec).or(vec.eq(c1Vec)));
//                    if (candidate != Byte.MAX_VALUE)
//                        return i+candidate-r.offset;
                }
                for (byte c; i != end && (c = u8[i]) != c0 && c != c1; ) ++i;
                return i-r.offset;
            }
            case BufferRope r -> {
                ByteBuffer bb = r.buffer;
                ByteOrder bo = bb.order();
                end += (i = bb.position());
                i = begin += i;
                for (int b = begin+B_SP.loopBound(end-begin); i < b; i += B_SP.length()) {
                    ByteVector vec = fromByteBuffer(B_SP, bb, i, bo);
                    if ((lane = vec.eq(c0Vec).or(vec.eq(c1Vec)).firstTrue()) < B_LEN)
                        return i+lane-bb.position();
//                    byte candidate = IOTA.reduceLanes(MIN, vec.eq(c0Vec).or(vec.eq(c1Vec)));
//                    if (candidate != Byte.MAX_VALUE)
//                        return i+candidate-bb.position();
                }
                for (byte c; i != end && (c = bb.get(i)) != c0 && c != c1; ) ++i;
                return i-bb.position();
            }
            default -> {
                for (i = begin; i < end; i++) {
                    byte c = get(i);
                    if (c == c0 || c == c1) return i;
                }
                return end;
            }
        }
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
    public final int skipUntilLast(int begin, int end, char c0) {
        int i, lane, len = checkRangeAndGetLen(begin, end);
        Vector<Byte> c0Vec = B_SP.broadcast(c0);
        switch (this) {
            case ByteRope r -> {
                byte[] u8 = r.utf8;
                begin += (i = r.offset);
                i = end += i;
                while ((i -= B_SP.length()) >= begin) {
                    if ((lane = fromArray(B_SP, u8, i).eq(c0Vec).lastTrue()) < B_LEN)
                        return i+lane-r.offset;
//                    ByteVector vec = fromArray(B_SP, u8, i);
//                    byte candidate = IOTA.reduceLanes(MAX, vec.eq(c0Vec));
//                    if (candidate != Byte.MIN_VALUE)
//                        return i+candidate- r.offset;
                }
                i += B_SP.length()-1; // while always overdraws from i, revert that
                for (; i >= begin; --i) {
                    byte c = u8[i];
                    if (c == c0) return i-r.offset;
                }
                return end- r.offset;
            }
            case BufferRope r -> {
                ByteBuffer buf = r.buffer;
                ByteOrder bo = buf.order();
                begin += i = buf.position();
                i = end += i;
                while ((i -= B_SP.length()) >= begin) {
                    if ((lane = fromByteBuffer(B_SP, buf, i, bo).eq(c0Vec).lastTrue()) < B_LEN)
                        return i+lane-buf.position();
//                    ByteVector vec = fromByteBuffer(B_SP, buf, i, bo);
//                    byte candidate = IOTA.reduceLanes(MAX, vec.eq(c0Vec));
//                    if (candidate != Byte.MIN_VALUE)
//                        return i+candidate-buf.position();
                }
                i += B_SP.length()-1; // while always overdraws from i, revert that
                for (; i >= begin; --i) {
                    byte c = buf.get(i);
                    if (c == c0) return i-buf.position();
                }
                return end-buf.position();
            }
            default -> {
                for (i = end-1; i >= begin; --i) {
                    byte c = get(i);
                    if (c == c0) return i;
                }
                return end;
            }
        }
    }

    /**
     * Similar to {@link Rope#skipUntil(int, int, char, char)} but finds the
     * <strong>LAST</strong> {@code i}.
     */
    public final int skipUntilLast(int begin, int end, char c0, char c1) {
        int i, lane, len = checkRangeAndGetLen(begin, end);
        Vector<Byte> c0Vec = B_SP.broadcast(c0), c1Vec = B_SP.broadcast(c1);
        switch (this) {
            case ByteRope r -> {
                byte[] u8 = r.utf8;
                begin += (i = r.offset);
                i = end += i;
                while ((i -= B_SP.length()) >= begin) {
                    ByteVector vec = fromArray(B_SP, u8, i);
                    if ((lane = vec.eq(c0Vec).or(vec.eq(c1Vec)).lastTrue()) > -1)
                        return i+lane-r.offset;
//                    byte candidate = IOTA.reduceLanes(MAX, vec.eq(c0Vec).or(vec.eq(c1Vec)));
//                    if (candidate != Byte.MIN_VALUE)
//                        return i+candidate- r.offset;
                }
                i += B_LEN-1; // while always overdraws from i, revert that
                for (; i >= begin; --i) {
                    byte c = u8[i];
                    if (c == c0 || c == c1) return i- r.offset;
                }
                return end-r.offset;
            }
            case BufferRope r -> {
                ByteBuffer buf = r.buffer;
                ByteOrder bo = buf.order();
                begin += i = buf.position();
                i = end += i;
                while ((i -= B_SP.length()) >= begin) {
                    ByteVector vec = fromByteBuffer(B_SP, buf, i, bo);
                    if ((lane = vec.eq(c0Vec).or(vec.eq(c1Vec)).lastTrue()) > -1)
                        return i+lane-buf.position();
//                    byte candidate = IOTA.reduceLanes(MAX, vec.eq(c0Vec).or(vec.eq(c1Vec)));
//                    if (candidate != Byte.MIN_VALUE)
//                        return i+candidate-buf.position();
                }
                i += B_LEN-1; // while always overdraws from i, revert that
                for (; i >= begin; --i) {
                    byte c = buf.get(i);
                    if (c == c0 || c == c1) return i-buf.position();
                }
                return end-buf.position();
            }
            default -> {
                for (i = end-1; i >= begin; --i) {
                    byte c = get(i);
                    if (c == c0 || c == c1) return i;
                }
                return end;
            }
        }
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
    public final int skip(int begin, int end, int[] alphabet) {
        int len = checkRangeAndGetLen(begin, end);
        if (this instanceof ByteRope b) {
            end += b.offset;
            return RopeSupport.skip(b.utf8, b.offset+begin, end, alphabet)-b.offset;
        } else {
            boolean stopOnNonAscii = (alphabet[3] & 0x80000000) == 0;
            for (int i = begin, eEnd = Math.min(len(), end); i < eEnd; ++i) {
                byte c = get(i);
                if (c >= 0) { // c is ASCII
                    if ((alphabet[c >> 5] & (1 << c)) == 0)
                        return i; // c is not in alphabet
                } else if (stopOnNonAscii) {
                    return i; // non-ASCII  not allowed by alphabet
                }
            }
            return end;
        }
    }

    /** Equivalent to {@code skip(begin, end, Rope.WS)}. */
    public final int skipWS(int begin, int end) {
        while (begin < end && get(begin) <= ' ') ++begin;
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
        int len = checkRangeAndGetLen(begin, end);
        if (this instanceof ByteRope b) {
            begin += b.offset;
            end = b.offset+Math.min(end, len);
            return RopeSupport.reverseSkip(b.utf8, begin, end, alphabet)-b.offset;
        } else {
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
    }

    public final int reverseSkipUntil(int begin, int end, char c) {
        int offset;
        switch (this) {
            case ByteRope r -> {
                byte[] u8 = r.utf8;
                begin += (offset = r.offset);
                end += offset - 1;
                while (end >= begin && u8[end] != c) --end;
                return Math.max(end, begin)-r.offset;
            }
            case BufferRope r -> {
                ByteBuffer bb = r.buffer;
                begin += (offset = bb.position());
                end += offset - 1;
                while (end >= begin && bb.get(end) != c) --end;
            }
            default -> {
                end += (offset = 0) - 1;
                while (end >= begin && get(end) != c) --end;
            }
        }
        return Math.max(end, begin)-offset;
    }

    /** Whether {@code get(i) == sequence[i]} for every {@code i} in {@code [begin, end)}. */
    public final boolean has(int position, byte[] sequence) {
        if (position+sequence.length > laxCheckIndexAndGetLen(position)) return false;
        return switch (this) {
            case ByteRope r ->
                RopeSupport.rangesEqual(r.utf8, r.offset+position, sequence, 0, sequence.length);
            case BufferRope r -> {
                int i = 0;
                position += r.buffer.position();
                if (sequence.length > 32) {
                    ByteOrder bo = r.buffer.order();
                    for (int e = B_SP.loopBound(sequence.length); i < e; i += B_SP.length()) {
                        ByteVector sVec = fromArray(B_SP, sequence, i);
                        ByteVector bVec = fromByteBuffer(B_SP, r.buffer, position+i, bo);
                        if (!sVec.eq(bVec).allTrue()) yield false;
                    }
                }
                while (i < sequence.length && r.buffer.get(position+i) == sequence[i]) ++i;
                yield i == sequence.length;
            }
            default -> {
                for (byte b : sequence) {
                    if (b != get(position++)) yield false;
                }
                yield true;
            }
        };
    }

    /** Whether {@code sub(pos, pos+(end-begin)).equals(rope.sub(begin, end))}. */
    public final boolean has(int pos, Rope rope, int begin, int end) {
        int len = end - begin;
        if (pos+len > laxCheckIndexAndGetLen(pos)) return false;
        return switch (this) {
            case ByteRope l -> {
                if (len > 32 && rope instanceof ByteRope r)
                    yield RopeSupport.rangesEqual(l.utf8, l.offset + pos, r.utf8, r.offset+begin, len);
                for (pos += l.offset; begin < end && l.utf8[pos] == rope.get(begin); ++pos)
                    ++begin;
                yield begin == end;
            }
            case BufferRope l -> {
                pos += l.buffer.position();
                if (len > 32 && rope instanceof ByteRope r) {
                    ByteOrder lo = l.buffer.order();
                    begin += r.offset;
                    for (int e = pos+B_SP.loopBound(len); pos < e; pos += B_SP.length(), begin += B_SP.length()) {
                        ByteVector lv = fromByteBuffer(B_SP, l.buffer, pos, lo);
                        if (!lv.eq(fromArray(B_SP, r.utf8, begin)).allTrue()) yield false;
                    }
                    begin -= r.offset;
                }
                while (begin < end) {
                    if (l.buffer.get(pos++) != rope.get(begin++)) yield false;
                }
                yield true;
            }
            default -> {
                while (begin < end) {
                    if (get(pos++) != rope.get(begin++)) yield false;
                }
                yield true;
            }
        };
    }

    /** Whether {@code sub(position, position+rope.len()).equals(rope.sub(begin, end))}. */
    public final  boolean has(int position, Rope rope) {
        int len = rope.len();
        if (position+len > checkIndexAndGetLen(position)) return false;
        return switch (this) {
            case ByteRope l -> {
                if (len > 32 && rope instanceof ByteRope r)
                    yield RopeSupport.rangesEqual(l.utf8, l.offset+position, r.utf8, r.offset, len);
                position += l.offset;
                for (int i = 0; i < len; i++, position++) {
                    if (l.utf8[position] != rope.get(i)) yield false;
                }
                yield true;
            }
            case BufferRope l -> {
                int i = 0;
                if (len > 32) {
                    ByteOrder lo = l.buffer.order();
                    if (rope instanceof ByteRope r) {
                        for (int e = B_SP.loopBound(len), ro = r.offset; i < e; i += B_SP.length()) {
                            ByteVector lv = fromByteBuffer(B_SP, l.buffer, position+i, lo);
                            ByteVector rv = fromArray(B_SP, r.utf8, ro+i);
                            if (!lv.eq(rv).allTrue()) yield false;
                        }
                    } else if (rope instanceof BufferRope r) {
                        ByteOrder ro = r.buffer.order();
                        for (int e = B_SP.loopBound(len), rOff = r.buffer.position(); i < e; i += B_SP.length()) {
                            ByteVector lv = fromByteBuffer(B_SP, l.buffer, position+i, lo);
                            ByteVector rv = fromByteBuffer(B_SP, r.buffer, rOff+i, ro);
                            if (!lv.eq(rv).allTrue()) yield false;
                        }
                    }
                    position += i;
                }
                for (position += l.buffer.position(); i < len; ++position, ++i) {
                    if (l.buffer.get(position++) != rope.get(i++)) yield false;
                }
                yield true;
            }
            default -> {
                for (int i = 0; i < len; i++, position++) {
                    if (get(position) != rope.get(i)) yield false;
                }
                yield true;
            }
        };
    }

    /** Similar to {@link Rope#has(int, byte[])} but */
    public final boolean hasAnyCase(int position, byte[] uppercaseSequence) {
        int end = position +uppercaseSequence.length;
        if (end > laxCheckIndexAndGetLen(position)) return false;
        for (byte expected : uppercaseSequence) {
            byte actual = get(position++);
            if (actual != expected && ((actual < 'a' || actual > 'z') || actual - 32 != expected))
                return false;
        }
        return true;
    }

    /**
     * Compute a hash value where bit {@code i} is {@code get(end-1-i)&0x1}.
     *
     * @param begin index of the leftmost byte to include in the hash
     * @param end {@code len()} or index of first byte right of the byte sequence used to hash.
     * @return a int with the lowest {@code end-begin} bits set as described above.
     */
    public int lsbHash(int begin, int end) {
        int h = 0, bit = end-begin-1;
        switch (this) {
            case ByteRope r -> {
                byte[] u8 = r.utf8;
                begin += r.offset;
                while (bit >= 0)
                    h |= (u8[begin++]&1) << bit--;
            }
            case BufferRope r -> {
                var bb = r.buffer;
                begin += bb.position();
                while (bit >= 0)
                    h |= (bb.get(begin++)&1) << bit--;
            }
            default -> {
                while (begin < end)
                    h |= (get(begin++)&1) << bit--;
            }
        }
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

    private NumberFormatException badNumber(int begin) {
        return new NumberFormatException("Expected sign or integer" +": "+sub(begin, skip(begin, len(), UNTIL_WS)));
    }

    public boolean isAscii() {
        int i = 0, end = len(), vecEnd = B_SP.loopBound(end);
        switch (this) {
            case ByteRope r -> {
                byte[] u8 = r.utf8;
                end += i = r.offset;
                for (; i < vecEnd; i+= B_SP.length()) {
                    if (!ZERO.compare(LE, fromArray(B_SP, u8, i)).allTrue())
                        return false;
                }
                while (i < end && u8[i] >= 0) ++i;
            }
            case BufferRope r -> {
                ByteBuffer bb = r.buffer;
                ByteOrder bo = bb.order();
                end += i = bb.position();
                for (; i < vecEnd; i+= B_SP.length()) {
                    if (!ZERO.compare(LE, fromByteBuffer(B_SP, bb, i, bo)).allTrue())
                        return false;
                }
                while (i < end && bb.get(i) >= 0) ++i;
            }
            default -> {
                while (i < end && get(i) >= 0) ++i;
            }
        }
        return i == end;
    }

    @Override public int length() { return len(); }

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

    public final String toString(int begin, int end) {
        if (this instanceof ByteRope r)
            return new String(r.utf8, r.offset+begin, end-begin, UTF_8);
        else if (this instanceof BufferRope r && r.buffer.hasArray())
            return new String(r.buffer.array(), r.buffer.position()+begin, end-begin, UTF_8);
        else
            return new String(toArray(begin, end), 0, end-begin, UTF_8);
    }

    @Override public int hashCode() {
        int len = len();
        if (this instanceof ByteRope r)
            return RopeSupport.hash(r.utf8, r.offset, len);
        if (this instanceof BufferRope r && r.buffer.hasArray()) {
            int offset = r.buffer.arrayOffset() + r.buffer.position();
            return RopeSupport.hash(r.buffer.array(), offset, len);
        }
        int h = 0;
        for (int i = 0; i < len; i++)
            h = 31*h + (get(i)&0xff);
        return h;
    }

    @Override public boolean equals(Object o) {
        int len = len();
        return o == this || o instanceof Rope r && r.len() == len && has(0, r, 0, len);
    }

    @Override public int compareTo(@NonNull Rope o) {
        int len = len(), shared = Math.min(len, o.len());
        for (int i = 0; i < shared; i++) {
            if (get(i) != o.get(i)) return get(i) - o.get(i);
        }
        return len - o.len();
    }

    @Override public final @NonNull String toString() {
        if (this instanceof ByteRope r)
            return new String(r.utf8, r.offset, r.len, UTF_8);
        return new String(toArray(0, len()), UTF_8);
    }
}
