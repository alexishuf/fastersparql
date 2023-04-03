package com.github.alexishuf.fastersparql.model.rope;

import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.Vector;
import jdk.incubator.vector.VectorSpecies;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.lang.foreign.MemorySegment;
import java.util.Arrays;
import java.util.Collection;

import static java.lang.Math.max;
import static java.lang.System.arraycopy;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.copyOf;
import static jdk.incubator.vector.ByteVector.fromArray;

@SuppressWarnings("UnusedReturnValue")
public final class ByteRope extends Rope {
    private static final VectorSpecies<Byte> B_SP = ByteVector.SPECIES_PREFERRED;
    private static final int B_LEN = B_SP.length();
    private static final int READLINE_CHUNK = 128;

    public static final ByteRope EMPTY = new ByteRope(new byte[0]);
    public static final ByteRope A = new ByteRope(new byte[]{'a'});
    public static final ByteRope DQ = new ByteRope(new byte[]{'"'});
    public static final ByteRope GT = new ByteRope(new byte[]{'>'});
    public static final ByteRope DT_MID = new ByteRope(new byte[]{'"', '^', '^'});
    public static final ByteRope DT_MID_LT = new ByteRope(new byte[]{'"', '^', '^', '<'});

    private static final MemorySegment EMPTY_SEGMENT = MemorySegment.ofArray(EMPTY.utf8);

    public byte[] utf8;
    public final int offset;
    private @Nullable MemorySegment segment, slice;

    /** Create an empty {@link ByteRope} */
    public ByteRope() {
        super(0);
        segment = slice = EMPTY_SEGMENT;
        utf8 = EMPTY.utf8;
        offset = 0;
    }

    /** Create an empty {@link ByteRope} backed by an array of the given capacity */
    public ByteRope(int capacity) {
        super(0);
        utf8 = new byte[capacity];
        slice = EMPTY_SEGMENT;
        offset = 0;
    }

    /** Copy the UTF-8 of {@code r}. */
    public ByteRope(Rope r) {
        super(r.len);
        utf8 = r.toArray(0, r.len());
        offset = 0;
    }

    /** Copy the UTF-8 of {@code o}. */
    public ByteRope(Object o) {
        super(0);
        utf8 = switch (o) {
            case null     ->  EMPTY.utf8;
            case Rope r   ->  r.toArray(0, r.len());
            case byte[] a ->  a;
            default       ->  o.toString().getBytes(UTF_8);
        };
        offset = 0;
        len = utf8.length;
    }

    /** Equivalent to {@code ByteRope(utf8, 0, utf8.length)} */
    public ByteRope(byte[] utf8) {
        super(utf8.length);
        this.utf8 = utf8;
        this.offset = 0;
    }

    /**
     * Create a ByteRope backed by {@code len} bytes starting at {@code utf8[offset]}.
     *
     * <p>{@code utf8} is kept by reference. Therefore, changes to its content will be
     * visible in the {@link ByteRope} instance. Use {@link ByteRope(Object)}
     * for a copying constructor.</p>
     *
     * @param utf8 backing array (kept by reference) with the UTF-8 string
     * @param offset start of the UTF-8 string in {@code utf8}
     * @param len length (in bytes) of the UTF-8 string in {@code utf8}.
     */
    public ByteRope(byte[] utf8, int offset, int len) {
        super(len);
        if (offset < 0 || offset+len > utf8.length)
            throw new IndexOutOfBoundsException(offset < 0 ? offset : offset+len);
        this.utf8 = utf8;
        this.offset = offset;
    }

    @Override public MemorySegment segment() {
        var s = slice;
        if (s == null) {
            if ((s = segment) == null)
                segment = s = MemorySegment.ofArray(utf8);
            if (offset != 0 || len != utf8.length)
                s = s.asSlice(offset, len);
            slice = s;
        }
        return s;
    }

    @Override public byte get(int i) {
        if (i < 0 || i >= len) throw new IndexOutOfBoundsException(i);
        return utf8[offset+i];
    }

    private void raiseImmutable() {
        throw new UnsupportedOperationException("Mutating ByteRope over shared byte[] utf8");
    }

    private int postIncLen(int increment) {
        if (offset != 0 || this == EMPTY) raiseImmutable();
        int len = this.len, required = len+increment;
        if (required > utf8.length) {
            utf8 = copyOf(utf8, max(utf8.length+32, required));
            segment = null;
        }
        this.len += increment;
        slice = null;
        return len;
    }

    public @This ByteRope ensureFreeCapacity(int increment) {
        if (offset != 0 || this == EMPTY) raiseImmutable();
        int len = this.len, required = len+increment;
        if (required > utf8.length) {
            utf8 = copyOf(utf8, max(utf8.length + 32, required));
            segment = null;
            slice = null;

        }
        return this;
    }

    @SuppressWarnings("unused") public @This ByteRope ensureCapacity(int capacity) {
        ensureFreeCapacity(capacity-len);
        return this;
    }

    /** Get {@code this.utf8} or a copy with {@code length == this.len}. */
    public byte[] fitBytes() {
        return utf8.length == len ? utf8 : copyOf(utf8, len);
    }

    public @This ByteRope clear() {
        if (offset != 0 || this == EMPTY) raiseImmutable();
        len = 0;
        slice = null;
        return this;
    }

    public @This ByteRope replace(char c, char r) {
        if (offset != 0 || this == EMPTY) raiseImmutable();
        int i = 0;
        Vector<Byte> cVec = B_SP.broadcast(c);
        for (int j, e = B_SP.loopBound(len); i < e; i += B_SP.length()) {
            for (var vec = fromArray(B_SP, utf8, i); (j = vec.eq(cVec).firstTrue()) != B_LEN; )
                vec = vec.withLane(j, utf8[i+j] = (byte) r);
        }
        for (int e = len; i < e; ++i)
            if (utf8[i] == c) utf8[i] = (byte) r;
        return this;
    }

    public @This ByteRope erase(int begin, int end) {
        if (offset != 0 || this == EMPTY) raiseImmutable();
        if (end < len)
            arraycopy(utf8, end, utf8, begin, len-end);
        len -= Math.min(len, end-begin);
        slice = null;
        return this;
    }

//    /**
//     * Read as many bytes as possible from {@code in} and stop if there is at least one
//     * {@code mark} within the read bytes. If EOF is reached for {@code in}, insert a
//     * {@code mark} if it did not end in {@code mark}.
//     *
//     * @return the number of bytes read.
//     */
//    public int eagerReadUntilEnforced(InputStream in, char mark) throws IOException {
//        int sum = 0, n;
//        while ((n = in.read(utf8, len, utf8.length-len)) != -1) {
//            sum += n;
//            int i = len, e = len += n;
//            if (skipUntil(i, e, mark) < e) break;
//            if (e == utf8.length) ensureFreeCapacity(Math.min(4096, e));
//        }
//        if (n == -1 && utf8[len-1] != mark)
//            append(mark);
//        return sum;
//    }

    /** Analogous to {@link BufferedReader#readLine()}. */
    public boolean readLine(InputStream in) throws IOException {
        boolean got = false;
        ensureFreeCapacity(READLINE_CHUNK);
        if (in instanceof BufferedInputStream) {
            in.mark(READLINE_CHUNK);
            for (int n; (n = in.read(utf8, len, READLINE_CHUNK)) != -1; ) {
                got = true;
                int begin = len, eol;
                long breakLenAndIndex = skipUntilLineBreak(begin, len += n);
                if ((eol = (int)breakLenAndIndex) < len) {
                    in.reset();
                    //noinspection ResultOfMethodCallIgnored
                    in.read(utf8, begin, (len = eol) + (int)(breakLenAndIndex>>>32) - begin);
                    break;
                } else {
                    in.mark(READLINE_CHUNK);
                }
            }

        } else {
            int sum = 0, c;
            for (int free = utf8.length-len; (c = in.read()) != -1 && c != '\n'; --free, ++sum) {
                if (free-- == 0) {
                    int newLength = utf8.length + max(32, utf8.length >> 1);
                    free = (utf8 = copyOf(utf8, newLength)).length - len;
                    segment = null;
                }
                utf8[len++] = (byte) c;
            }
            if (sum > 0 && utf8[len-1] == '\r')
                --len; // unget() '\r'
            got = sum > 0 || c == '\n';
        }
        slice = null;
        return got;
    }

    public @This ByteRope fill(int value) {
        if (offset != 0 || this == EMPTY) raiseImmutable();
        Arrays.fill(utf8, offset, offset+len, (byte)value);
        return this;
    }

    public @This ByteRope append(byte[] utf8, int offset, int len) {
        int out = postIncLen(len);
        arraycopy(utf8, offset, this.utf8, out, len);
        return this;
    }

    public @This ByteRope append(byte[] utf8) { return append(utf8, 0, utf8.length); }

    public @This ByteRope append(Rope r, int begin, int end) {
        int out = postIncLen(max(0, end-begin));
        r.copy(begin, end, utf8, out);
        return this;
    }

    public @This ByteRope append(Rope r) {
        int len = r.len();
        int out = postIncLen(len);
        r.copy(0, len, utf8, out);
        return this;
    }

    public @This ByteRope append(char c) {
        ensureFreeCapacity(1);
        utf8[len++] = (byte)c;
        slice = null;
        return this;
    }
    public @This ByteRope append(byte c) {
        ensureFreeCapacity(1);
        utf8[len++] = c;
        slice = null;
        return this; }

    public @This ByteRope append(CharSequence o) {
        return o instanceof Rope r ? append(r) : append(o.toString().getBytes(UTF_8));
    }

    public @This ByteRope append(Collection<?> coll) {
        append('[');
        boolean first = true;
        for (Object o : coll) {
            if (first) first = false;
            else       append(',').append(' ');
            append(o);
        }
        append(']');
        return this;
    }

    public @This ByteRope append(Object o) {
        switch (o) {
            case Rope          r -> append(r);
            case byte[]        a -> append(a, 0, a.length);
            case Collection<?> c -> append(c);
            case Object[]      a -> append(Arrays.asList(a));
            case    int[]      a -> append(Arrays.toString(a));
            case   long[]      a -> append(Arrays.toString(a));
            case double[]      a -> append(Arrays.toString(a));
            case  float[]      a -> append(Arrays.toString(a));
            case  short[]      a -> append(Arrays.toString(a));
            case null            -> {}
            default              -> append(o.toString().getBytes(UTF_8));
        }
        return this;
    }

    public @This ByteRope append(long n) {
        if (n == 0)
            return append('0');
        else if (n == Long.MIN_VALUE)
            return append(Long.toString(n));
        int sign = (int) (n >>> 63);
        n = Math.abs(n);
        postIncLen(sign + (int) Math.floor(Math.log10(n) + 1));
        int i = len;
        for (long rem = n%10; n  > 0; n = n/10, rem = n%10)
            utf8[--i] = (byte)('0' + rem);
        if (sign == 1)
            utf8[--i] = '-';
        return this;
    }

    public @This ByteRope repeat(byte c, int n) {
        for (int i = 0, out = postIncLen(n); i < n; i++) utf8[out++] = c;
        return this;
    }

    public @This ByteRope newline(int spaces) {
        int o = postIncLen(spaces + 1);
        utf8[o++] = '\n';
        while (spaces-- > 0) utf8[o++] = ' ';
        return this;
    }

    public @This ByteRope escapingLF(Object o) {
        Rope r = Rope.of(o);
        ensureFreeCapacity(r.len()+8);
        for (int consumed = 0, i, end = r.len(); consumed < end; consumed = i+1) {
            i = r.skipUntil(consumed, end, '\n');
            append(r, consumed, i);
            if (i < end)
                append('\\').append('n');
        }
        return this;
    }

    public @This ByteRope indented(int spaces, Object o) {
        Rope r = Rope.of(o);
        int end = r.len();
        ensureFreeCapacity(end+(spaces+1)<<3);
        repeat((byte) ' ', spaces);
        for (int i = 0, eol; i < end; i = eol+1) {
            if (i > 0) newline(spaces);
            append(r, i, eol = r.skipUntil(i, end, '\n'));
        }
        if (end > 0 && r.get(end-1) == '\n') append('\n');
        return this;
    }

    public @This ByteRope unAppend(int n) {
        if (n > len) throw new IllegalArgumentException("n ("+n+") > size ("+ len +")");
        len -= n;
        slice = null;
        return this;
    }

}