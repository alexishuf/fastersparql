package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.model.rope.*;
import com.github.alexishuf.fastersparql.sparql.PrefixAssigner;
import com.github.alexishuf.fastersparql.sparql.expr.InvalidTermException;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import com.github.alexishuf.fastersparql.sparql.expr.TermParser;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import static com.github.alexishuf.fastersparql.model.rope.ByteRope.EMPTY;

@SuppressWarnings("BooleanMethodIsAlwaysInverted")
public abstract class Batch<B extends Batch<B>> {
    public static final TermBatchType TERM = TermBatchType.INSTANCE;
    public static final CompressedBatchType COMPRESSED = CompressedBatchType.INSTANCE;

    public int rows, cols;

    protected Batch(int rows, int cols) {
        this.rows = rows;
        this.cols = cols;
    }

    /* --- --- --- batch-level accessors --- --- --- */

    /** Number of rows in this batch. This is not capacity, rather number of actual rows. */
    public final int rows() { return rows; }

    /** Number of columns in rows of this batch */
    public final int cols() { return cols; }

    /** Get a {@link Batch} with same rows as this. The copy is deep,so that changing a row
     *  in {@code this} will have no effect on the row at the returned batch. */
    public abstract Batch<B> copy();

    /**
     * How many bytes should be given to {@link #reserve(int, int)} {@code bytes}
     * parameter so that {@code putRows(this, 0, rows)} does not trigger an allocation.
     *
     * @return how many bytes are being currently used.
     */
    public int bytesUsed() { return (rows*cols)<<3; }

    public abstract int rowsCapacity();

    /** Whether this batch can reach {@code rowsCapacity} and  a {@link #bytesUsed()} value
     *  of {@code bytes} before requiring a new allocation. */
    public abstract boolean hasCapacity(int rowsCapacity, int bytesCapacity);

    /** Whether this batch has more capacity than {@code other}. */
    public abstract boolean hasMoreCapacity(B other);

    /**
     * <strong>USE ONLY FOR TESTING</strong>
     * @return list or all rows, with each row represented as a {@code List<Term>}
     */
    public List<List<Term>> asList() {
        var list = new ArrayList<List<Term>>();
        for (int r = 0; r < rows; r++)
            list.add(asList(r));
        return list;
    }

    @Override public int hashCode() {
        int h = cols;
        for (int r = 0, n = rows; r < n; r++) h ^= hash(r);
        return h;
    }

    @Override public boolean equals(Object obj) {
        if (!(obj instanceof Batch<?> wild) || wild.cols != cols || wild.rows != rows)
            return false;
        //noinspection unchecked
        B rhs = (B)wild;
        for (int r = 0; r < rows; r++) { if (!equals(r, rhs, r)) return false; }
        return true;
    }

    @Override public String toString() {
        int rows = this.rows, cols = this.cols;
        if (rows == 0)
            return "[]";
        if (cols == 0)
            return rows == 1 ? "[[]]" : "[... "+rows+" zero-column rows ...]";
        try {
            var sb = new ByteRope().append(rows == 1 ? "[[" : "[\n  [");
            for (int r = 0; r < rows; r++) {
                for (int c = 0; c < cols; c++) {
                    var t = get(r, c);
                    sb.append(t == null ? "null" : t.toSparql()).append(", ");
                }
                sb.unAppend(2);
                sb.append("]\n  [");
            }
            sb.unAppend(4);
            return sb.append(rows == 1 ? "]" : "\n]").toString();
        } catch (IndexOutOfBoundsException e)  {
            return "[<batch clear()ed concurrently with toString()>]";
        }
    }

    /* --- --- --- row-level accessors --- --- --- */

    /**
     * <strong>USE ONLY FOR TESTING</strong>
     * @return The terms for the {@code r}-th row as a {@code List<Term>}
     */
    public List<Term> asList(int r) {
        ArrayList<Term> list = new ArrayList<>(cols);
        for (int c = 0; c < cols; c++)
            list.add(get(r, c));
        return list;
    }

    /**
     * Compute a hash value for the row with given index.
     *
     * @param row index of the row
     * @return a hash code
     * @throws IndexOutOfBoundsException if {@code row} not in {@code [0, rows)}.
     */
    public int hash(int row) {
        int acc = 0;
        for (int c = 0, cols = this.cols; c < cols; c++)
            acc ^= hash(row, c);
        return acc;
    }

    /**
     * How many bytes should be given to {@link #reserve(int, int)} {@code bytes}
     * parameter so that {@code putRows(this, row, row+1)} does not trigger an allocation.
     *
     * @param row the row index
     */
    public int bytesUsed(int row) { return cols<<3; }

    /** Whether rows {@code row} in {@code this} and {@code oRow} in {@code other} have the
     *  same number of columns with equal {@link Term}s in them. */
    public boolean equals(int row, B other, int oRow) {
        int cols = this.cols;
        if (other.cols != cols) return false;
        for (int c = 0; c < cols; c++) { if (!equals(row, c, other, oRow, c)) return false; }
        return true;
    }

    /** Whether {@code cols == other.length} and {@code equals(get(row, i), other[i])}
     * for every column {@code i}. */
    public boolean equals(int row, Term[] other) {
        int cols = this.cols;
        if (other.length != cols) return false;
        for (int c = 0; c < cols; c++) { if (!equals(row, c, other[c])) return false; }
        return true;
    }

    /** Whether {@code cols == other.size()} and {@code equals(get(row, i), other.get(i))} for
     *  every column {@code i}. */
    public boolean equals(int row, List<Term> other) {
        int cols = this.cols;
        if (other.size() != cols) return false;
        for (int c = 0; c < cols; c++) { if (!equals(row, c, other.get(c))) return false; }
        return true;
    }

    /** Get a string representation of the row at the given index. */
    public String toString(int row) {
        if (cols == 0) return "[]";
        var sb = new ByteRope().append('[');
        for (int i = 0, cols = this.cols; i < cols; i++) {
            var t = get(row, i);
            sb.append(t == null ? "null" : t.toSparql()).append(", ");
        }
        sb.unAppend(2);
        return sb.append(']').toString();
    }

    protected String mkOutOfBoundsMsg(int row) {
        return "row "+row+" is out of bounds for cols="+cols+")";
    }

    /* --- --- --- term-level accessors --- --- --- */

    /**
     * Get the {@link Term} set at column {@code col} or row {@code row}.
     *
     * @param row row index
     * @param col columns index
     * @throws IndexOutOfBoundsException if {@code row} is not in {@code [0, rows)}
     *                                   or {@code col} is not in {@code [0, cols)}
     */
    public abstract @Nullable Term get(@NonNegative int row, @NonNegative int col);

    /**
     * Analogous to {@link #get(int, int)}, but gets a {@link SegmentRope} or
     * {@link TwoSegmentRope} rope instance, which are cheaper to produce than {@link Term}.
     *
     * @param row the row index
     * @param col the column index
     * @return A {@link PlainRope} with the same bytes as a {@link Term}
     * @throws IndexOutOfBoundsException if {@code row} is not in {@code [0, rows)} or
     *                                   {@code col} is not  in {@code [0, cols)}
     */
    public @Nullable PlainRope getRope(@NonNegative int row, @NonNegative int col) {
        Term t = get(row, col);
        return t == null ? null : new TwoSegmentRope(t.first(), t.second());
    }

    /**
     * Get the {@link Term} at column {@code col} of row {@code row} into {@code dest}.
     *
     * <p><strong>WARNING:</strong> If the batch is mutated after this method returns,
     * the contents of {@code dest.local()} MAY change. Use {@link #get(int, int)} to avoid
     * this.</p>
     *
     * @param row the row of the desired term.
     * @param col the column of the desired term
     * @param dest where the term will be mapped to there will be no copy of UTF-8 bytes, only
     *             pointers to the term stored in this batch will be copied into {@code dest}.
     *             <strong>Warning:</strong> if the batch is mutated/recycled, {@code dest}
     *             might then be pointing to garbage.
     * @return {@code true} if there is a term set at the requested row and column. {@code false}
     *         if the row and column where within bounds but there was no value set (e.g., an
     *         unbound variable)
     * @throws IndexOutOfBoundsException if {@code row} or {@code col} are negative or above the
     *                                   number of rows (or columns) in this batch.
     */
    public boolean getView(@NonNegative int row, @NonNegative int col, Term dest)  {
        Term t = get(row, col);
        if (t == null) return false;
        dest.set(t.shared(), t.local(), t.sharedSuffixed());
        return true;
    }

    /**
     * Analogous to {@link #getRope(int, int)}, but modifies {@code dest}
     * instead of spawning a new {@link SegmentRope} or {@link TwoSegmentRope} instance.
     *
     * <p><strong>Warning:</strong>If the batch is mutated after this method returns, the contents
     * of {@code dest.local()} MAY change. To avoid this behavior, use
     * {@link #getRope(int, int)}.</p>
     *
     * @param row the row index
     * @param col the column index
     * @return {@code true} iff there was a term defined at {@code (row, col)} and therefore
     *         {@code dest} was mutated.
     * @throws IndexOutOfBoundsException if {@code row} is not in {@code [0, rows)} or
     *                                   {@code col} is not  in {@code [0, cols)}
     */
    public boolean getRopeView(@NonNegative int row, @NonNegative int col, TwoSegmentRope dest) {
        PlainRope r = getRope(row, col);
        if (r == null) return false;
        if (r instanceof SegmentRope s) {
            dest.wrapFirst(s);
            dest.wrapSecond(EMPTY);
        } else {
            TwoSegmentRope t = (TwoSegmentRope) r;
            dest.wrapFirst(t.fst, t.fstU8, t.fstOff, t.fstLen);
            dest.wrapSecond(t.snd, t.sndU8, t.sndOff, t.sndLen);
        }
        return true;
    }

    /** Null-safe equivalent to {@code get(row, col).shared()}. */
    public @NonNull SegmentRope shared(@NonNegative int row, @NonNegative int col) {
        Term term = get(row, col);
        return term == null ? EMPTY : term.shared();
    }

    /** Null-safe equivalent to {@code get(row, col).shared()}. */
    public boolean sharedSuffixed(@NonNegative int row, @NonNegative int col) {
        Term term = get(row, col);
        return term != null && term.sharedSuffixed();
    }

    /** Null-safe equivalent to {@code get(row, col).len}. */
    public int len(@NonNegative int row, @NonNegative int col) {
        Term t = get(row, col);
        return t == null ? 0 : t.len;
    }

    /** If the term at {@code (row, col)} is a literal, return the index of the
     * closing {@code "}-quote. Else, return zero. */
    public int lexEnd(@NonNegative int row, @NonNegative int col) {
        Term t = get(row, col);
        return Math.max(t == null ? 0 : t.endLex(), 0);
    }

    public int localLen(@NonNegative int row, @NonNegative int col) {
        Term t = get(row, col);
        return t == null ? 0 : t.local().len;
    }

    /** Null-safe equivalent to {@code get(row, col).type()}. */
    public Term.@Nullable Type termType(int row, int col) {
        var t = get(row, col);
        return t == null ? null : t.type();
    }

    /** Null-safe equivalent to {@code get(row, col).asDatatypeId()}. */
    public @Nullable SegmentRope asDatatypeSuff(int row, int col) {
        var t = get(row, col);
        return t == null ? null : t.asDatatypeSuff();
    }

    /** Null-safe equivalent to {@code get(row, col).datatypeTerm()}. */
    public @Nullable Term datatypeTerm(int row, int col) {
        var t = get(row, col);
        return t == null ? null : t.datatypeTerm();
    }

    /**
     * Writes the RDF value at {@code column} out to {@code dest} in SPARQL syntax using
     * {@code prefixAssigner} to find (or assign) a prefix name to likely shared IRIs.
     *
     * <p>This is equivalent to (but maybe faster than):</p>
     * <pre>
     *     Term term = get(row, column);
     *     if (term != null)
     *        term.toSparql(dest, prefixAssigner);
     * </pre>
     *
     * @param dest where to write the SPARQL representation to
     * @param row the row from where to get the RDF value. if null, the RDF value will also
     *            be null and nothing will be written
     * @param column The column from where to get the RDF value.
     * @param prefixAssigner Used to get prefix names for an IRI prefix that appears in an IRI
     *                       or on a datatype IRI and is likely to be shared.
     * @return the number of bytes written
     */
    public int writeSparql(ByteSink<?> dest, int row, int column, PrefixAssigner prefixAssigner) {
        Term term = get(row, column);
        if (term != null)
            return term.toSparql(dest, prefixAssigner);
        return 0;
    }

    /**
     * Appends {@code get(row, col)} to {@code dest} if the {@link Term} is not null.
     * @param dest destination where the term will be appended, in NT-syntax
     * @param row see {@link #get(int, int)}
     * @param col see {@link #get(int, int)}
     */
    public void writeNT(ByteSink<?> dest, int row, int col) {
        TwoSegmentRope tmp = new TwoSegmentRope();
        if (getRopeView(row, col, tmp)) {
            dest.append(tmp.fst, tmp.fstOff, tmp.fstLen);
            dest.append(tmp.snd, tmp.sndOff, tmp.sndLen);
        }
    }

    /**
     * Null-safe equivalent to {@code dest.append(get(row, col), begin, end)}.
     */
    public void write(ByteSink<?> dest, int row, int col, int begin, int end) {
        Term t = get(row, col);
        if (t != null) dest.append(t, begin, end);
    }

    /** Get a hash code for the term at column {@code col} of row {@code row}. */
    public int hash(int row, int col) {
        Term t = get(row, col);
        return t == null ? Rope.FNV_BASIS : t.hashCode();
    }

    /** Equivalent to {@code Objects.equals(get(row, col), other)} */
    public boolean equals(@NonNegative int row, @NonNegative int col, @Nullable Term other) {
        return Objects.equals(get(row, col), other);
    }

    /** Equivalent to {@code Objects.equals(get(row, col), other)} */
    public boolean equals(@NonNegative int row, @NonNegative int col,
                          B other, int oRow, int oCol) {
        return Objects.equals(get(row, col), other.get(oRow, oCol));
    }

    protected String mkOutOfBoundsMsg(int row, int col) {
        return "("+row+", "+col+") is out of bounds for batch of size ("+rows+", "+cols+")";
    }

    /* --- --- --- mutators --- --- --- */

    /**
     * Hints that an implementation SHOULD perform allocation for an incoming sequence of
     * {@code rows} row offers/puts (see {@link #beginOffer()} and {@link #beginPut()}).
     *
     * <p>This method is a hint and thus subsequent {@link #beginOffer()} and related
     * methods may still reject a row by returning {@code false}.</p>
     *
     * @param additionalRows expected number of subsequent row offers/puts. Implementations may
     *                       ignore this in favor of {@code additionalBytes}.
     * @param additionalBytes expected number of additional bytes to be used by
     *                        subsequent offers/puts. Implementations may ignore this in favor of
     *                        {@code additionalRows}.
     */
    public abstract void reserve(int additionalRows, int additionalBytes);

    /** Remove all rows from this batch.*/
    public void clear() { clear(cols); }

    /**
     * Remove all rows from this batch and sets columns to {@code newColumns}
     * (performing required adjustments on the backing storage.;
     *
     * @param newColumns new number of columns
     */
    public abstract void clear(int newColumns);

    /**
     * Starts adding a new row to this batch.
     *
     * <p>Columns for the new row will be set through {@link #offerTerm(int, Term)} calls
     * following this call, with the {@code i-th} {@link #offerTerm(int, Term)} call setting
     * the {@code i}-th column of the new row. The row will only be visible to other methods
     * of this batch once {@link #commitOffer()} returns {@code true}./p>
     *
     * <p>This method, {@link #offerTerm(int, Term)} and {@link #commitOffer()}
     * return whether the operation may continue ({@code true}) or if the row will not be
     * accepted ({@code false}). Addition of a row will be rejected if memory allocation and/or
     * extensive copying would be required.</p>
     *
     * <p>The row offer methods are not thread-safe: For a given {@link Batch}, there MUST be
     * only one thread invoking any of said methods. Concurrent offers are not detected and thus
     * resulting behaviour is undefined.</p>
     *
     * <p>Example with cols==2:</p>
     *
     * <pre>{@code
     * boolean ok = batch.beginRowOffer();
     * if (ok) {
     *     for (int c = 0; c < row.length; c++) ok = batch.offer(row[c]);
     *     ok = ok && batch.commitRowOffer();
     * }
     * if (ok) {
     *     //row was added
     * } else {
     *     //row not added, wait or try another batch
     * }
     * }</pre>
     *
     * @return {@code true} if, maybe, this batch can accept a new row, {@code false} otherwise.
     */
    public abstract boolean beginOffer();

    /**
     * Offer {@code t} as the value of the next column after a {@link #beginOffer()}.
     *
     * @return {@code true} iff the row offer may continue.
     * @throws IllegalStateException if there is no previously uncommitted {@code true}
     *                               {@link #beginOffer()} call.
     */
    public abstract boolean offerTerm(int col, Term t);

    /** Equivalent to {@code offer(destCol, batch.get(row, col))}. */
    public boolean offerTerm(int destCol, B batch, int row, int col) {
        return offerTerm(destCol, batch.get(row, col));
    }

    /**
     * Equivalent to {@code offerTerm(col, termParser.asTerm())}, but faster.
     *
     * @param col destination column of {@link TermParser#asTerm()}
     * @param termParser A {@link TermParser} that just parsed some input.
     * @return {@code true} iff the row offer may continue.
     */
    public boolean offerTerm(int col, TermParser termParser) {
        var local = termParser.localBuf();
        int begin = termParser.localBegin;
        return offerTerm(col, termParser.shared(), local.segment, local.offset+begin,
                         termParser.localEnd - begin, termParser.sharedSuffixed());
    }

    /**
     * Equivalent building a term with the given {@code shared*} and {@code local*} arguments
     * and then calling {@link #offerTerm(int, Term)}.
     *
     * @param col destination column of the term
     * @param shared A prefix or suffix to be kept by reference
     * @param local Where the local (i.e., non-shared) prefix/suffix of this term is stored.
     * @param localOff Index of first byte in {@code local} that is part of the term
     * @param localLen number of bytes in {@code local} that constitute the local segment.
     * @param sharedSuffix Whether the shared segment is a suffix
     * @return {@code true} iff the row offer may continue
     */
    public boolean offerTerm(int col, SegmentRope shared, MemorySegment local,
                             long localOff, int localLen, boolean sharedSuffix) {
        return offerTerm(col, makeTerm(shared, local, localOff, localLen, sharedSuffix));
    }

    /** Analogous to {@link #offerTerm(int, SegmentRope, MemorySegment, long, int, boolean)}. */
    public boolean offerTerm(int col, SegmentRope shared, byte[] local, int localOff,
                             int localLen, boolean sharedSuffix) {
        return offerTerm(col, makeTerm(shared, local, localOff, localLen, sharedSuffix));
    }

    /** Analogous to {@link #offerTerm(int, SegmentRope, MemorySegment, long, int, boolean)}. */
    public boolean offerTerm(int col, SegmentRope shared, TwoSegmentRope local, int localOff,
                             int localLen, boolean sharedSuffix) {
        return offerTerm(col, makeTerm(shared, local, localOff, localLen, sharedSuffix));
    }

    /**
     * Try to commit the current {@link #beginOffer()}.
     *
     * @return {@code true} iff the row was added. If {@code false}, there will be no trace of
     *         the attempted row offer.
     * @throws IllegalStateException if there is no uncommitted and {@code true}
     *                               {@link #beginOffer()} call or if any
     *                               {@link #offerTerm(int, Term)} after
     *                               {@link #beginOffer()} returned {@code false}.
     */
    public abstract boolean commitOffer();

    /** Equivalent to
     * <pre>{@code
     * if (!beginOffer()) return false;
     * for (int c = 0; c < cols; c++) { if (!offerTerm(other, row, c)) return false; }
     * return commitOffer();
     * }</pre>
     *
     * @param other source of the row to be added
     * @param row index of row to be added
     * @return whether the row was added in full ({@code true}) or if it could not be added and no
     * side effects remain ({@code false}
     * @throws IndexOutOfBoundsException if row is not in {@code [0, other.rows)}
     * @throws IllegalArgumentException If {@code other.cols != this.cols}
     */
    public boolean offerRow(B other, int row) {
        int cols = this.cols;
        if (other.cols != cols) throw new IllegalArgumentException();
        if (beginOffer()) {
            for (int c = 0; c < cols; c++) { if (!offerTerm(c, other, row, c)) return false; }
            return commitOffer();
        }
        return false;
    }

    /**
     * Version of {@link #beginOffer()} that never rejects addition.
     *
     * <p>Example usage:</p>
     *
     * <pre>{@code
     * batch.beginRowPut();
     * for (int c = 0; c < row.length; c++) batch.offer(row[c]);
     * // row is not visible yet
     * batch.commitRowPut();
     * // batch.rows incremented and row is now visible.
     * }</pre>
     */
    public abstract void beginPut();

    /** Version of {@link #offerTerm(int, Term)} that never rejects. For use with
     *  {@link #beginPut()} and {@link #commitPut()} */
    public abstract void putTerm(int col, Term t);

    /** Equivalent to {@code putTerm(destCol, batch.get(row, col))}. */
    public void putTerm(int destCol, B batch, int row, int col) {
        putTerm(destCol, batch.get(row, col));
    }

    /**
     * Equivalent to {@code putTerm(col, termParser.asTerm())}, but faster.
     *
     * @param col destination column of {@link TermParser#asTerm()}
     * @param termParser A {@link TermParser} that just parsed some input.
     */
    public void putTerm(int col, TermParser termParser) {
        var local = termParser.localBuf();
        int begin = termParser.localBegin;
        putTerm(col, termParser.shared(), local.segment, local.offset+begin,
                  termParser.localEnd - begin, termParser.sharedSuffixed());
    }

    /**
     * Equivalent building a term with the given {@code shared*} and {@code local*} arguments
     * and then calling {@link #putTerm(int, Term)}.
     *
     * @param col destination column of the term
     * @param shared A prefix or suffix to be kept by reference
     * @param local Where the local (i.e., non-shared) prefix/suffix of this term is stored.
     * @param localOff Index of first byte in {@code local} that is part of the term
     * @param localLen number of bytes in {@code local} that constitute the local segment.
     * @param sharedSuffix Whether the shared segment is a suffix
     */
    public void putTerm(int col, SegmentRope shared, MemorySegment local,
                        long localOff, int localLen, boolean sharedSuffix) {
        putTerm(col, makeTerm(shared, local, localOff, localLen, sharedSuffix));
    }

    /** Analogous to {@link #putTerm(int, SegmentRope, MemorySegment, long, int, boolean)} */
    public void putTerm(int col, SegmentRope shared, TwoSegmentRope local,
                        int localOff, int localLen, boolean sharedSuffix) {
        putTerm(col, makeTerm(shared, local, localOff, localLen, sharedSuffix));
    }

    /** Analogous to {@link #putTerm(int, SegmentRope, MemorySegment, long, int, boolean)}. */
    public void putTerm(int col, SegmentRope shared, byte[] local, int localOff, int localLen,
                        boolean sharedSuffix) {
        putTerm(col, makeTerm(shared, local, localOff, localLen, sharedSuffix));
    }

    private Term makeTerm(SegmentRope shared, MemorySegment local, long localOff,
                          int localLen, boolean sharedSuffix) {
        if ((shared == null || shared.len == 0) && localLen == 0)
            return null;
        ByteRope localRope = new ByteRope(localLen).append(local, localOff, localLen);
        SegmentRope fst, snd;
        if (sharedSuffix) { fst = localRope; snd =    shared; }
        else              { fst =    shared; snd = localRope; }
        return Term.wrap(fst, snd);
    }

    private Term makeTerm(SegmentRope shared, byte[] localU8, int localOff,
                          int localLen, boolean sharedSuffix) {
        if ((shared == null || shared.len == 0) && localLen == 0)
            return null;
        ByteRope localRope = new ByteRope(localLen).append(localU8, localOff, localLen);
        SegmentRope fst, snd;
        if (sharedSuffix) { fst = localRope; snd =    shared; }
        else              { fst =    shared; snd = localRope; }
        return Term.wrap(fst, snd);
    }

    private Term makeTerm(SegmentRope shared, TwoSegmentRope local, int localOff,
                          int localLen, boolean sharedSuffix) {
        if ((shared == null || shared.len == 0) && localLen == 0)
            return null;
        ByteRope localRope = new ByteRope(localLen).append(local, localOff, localOff+localLen);
        SegmentRope fst, snd;
        if (sharedSuffix) { fst = localRope; snd =    shared; }
        else              { fst =    shared; snd = localRope; }
        return Term.wrap(fst, snd);
    }

    /** Version of {@link #commitOffer()} that never rejects. For use with
     *  {@link #beginPut()} and {@link #putTerm(int, Term)} */
    public abstract void commitPut();

    /** Version of {@link #offerRow(Batch, int)} that always add the row. */
    public void putRow(B other, int row) {
        int cols = this.cols;
        if (other.cols != cols) throw new IllegalArgumentException();
        reserve(1, other.bytesUsed(row));
        beginPut();
        for (int c = 0; c < cols; c++) putTerm(c, other, row, c);
        commitPut();
    }

    /**
     * Equivalent to:
     *
     * <pre>{@code
     * beginPut();
     * for (Term t : row) putTerm(t);
     * commitPut();
     * }</pre>
     *
     * @param row array with terms for the new row
     * @throws IllegalArgumentException if {@code row.length != this.cols}
     */
    public void putRow(Term[] row) {
        if (row.length != cols) throw new IllegalArgumentException();
        int bytes = 0;
        for (Term t : row)
            bytes += t == null ? 0 : t.local().len;
        reserve(1, bytes);
        beginPut();
        for (int c = 0; c < row.length; c++)
            putTerm(c, row[c]);
        commitPut();
    }

    /**
     * Equivalent to {@link #putRow(Term[])}, but with a {@link Collection}
     *
     * <p>If collection items are non-null and non-{@link Term}, they will be converted using
     * {@link Rope#of(Object)} and {@link Term#valueOf(CharSequence)}.</p>
     *
     * @param row single row to be added
     * @throws IllegalArgumentException if {@code row.size() != this.cols}
     * @throws InvalidTermException if {@link Term#valueOf(CharSequence)} fails to convert a non-null,
     *                              non-Term row item
     */
    public void putRow(Collection<?> row) {
        if (row.size() != cols)
            throw new IllegalArgumentException();
        reserve(1, 0);
        beginPut();
        int c = 0;
        for (Object o : row)
            putTerm(c++, o instanceof Term t ? t : Term.valueOf(Rope.of(o)));
        commitPut();
    }

    /**
     * Tries to add all rows of {@code other} into {@code this}. Either no rows will be
     * added or all rows will be added.
     *
     * @param other source of rows to add. Must have same number of columns.
     * @return whether all rows of {@code other} were added to {@code this}
     * @throws IllegalArgumentException if {@code other.cols != this.cols}.
     */
    public abstract boolean offer(B other) ;

    /**
     * Equivalent to {@code range(0, other.rows).forEach(r -> putRow(other, r))}.
     *
     * @param other batch that will have all its rows added to {@code this}.
     */
    public abstract void put(B other) ;

    /**
     * Equivalent to {@link #put(Batch)} but accepts {@link Batch} implementations
     * other than this.
     *
     * @param other source of rows
     * @param <O> {@link Batch} concrete class
     */
    public <O extends Batch<O>> @This B putConverting(O other) {
        int cols = other.cols;
        if (cols != this.cols) throw new IllegalArgumentException();
        if (other.getClass() == getClass()) {//noinspection unchecked
            put((B) other);
        } else {
            int rows = other.rows;
            reserve(rows, other.bytesUsed());
            for (int r = 0; r < rows; r++) {
                beginPut();
                for (int c = 0; c < cols; c++) putTerm(c, other.get(r, c));
                commitPut();
            }
        }
        //noinspection unchecked
        return (B)this;
    }
}
