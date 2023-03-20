package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.model.rope.ByteRope;
import com.github.alexishuf.fastersparql.model.rope.Rope;
import com.github.alexishuf.fastersparql.sparql.PrefixAssigner;
import com.github.alexishuf.fastersparql.sparql.expr.InvalidTermException;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

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
     * How many bytes should be given to {@link Batch#reserve(int, int)} {@code bytes}
     * parameter so that {@code putRows(this, 0, rows)} does not trigger an allocation.
     *
     * @return how many bytes are being currently used.
     */
    public abstract int bytesUsed();

    public abstract int rowsCapacity();

    /** Whether this batch can reach {@code rowsCapacity} and  a {@link Batch#bytesUsed()} value
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
            var sb = new StringBuilder().append(rows == 1 ? "[[" : "[\n  [");
            for (int r = 0; r < rows; r++) {
                for (int c = 0; c < cols; c++) {
                    var t = get(r, c);
                    sb.append(t == null ? "null" : t.toSparql()).append(", ");
                }
                sb.setLength(sb.length() - 2);
                sb.append("]\n  [");
            }
            sb.setLength(sb.length() - 4);
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
     * How many bytes should be given to {@link Batch#reserve(int, int)} {@code bytes}
     * parameter so that {@code putRows(this, row, row+1)} does not trigger an allocation.
     *
     * @param row the row index
     */
    public abstract int bytesUsed(int row);


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
        var sb = new StringBuilder().append('[');
        for (int i = 0, cols = this.cols; i < cols; i++) {
            var t = get(row, i);
            sb.append(t == null ? "null" : t.toSparql()).append(", ");
        }
        sb.setLength(sb.length()-2);
        return sb.append(']').toString();
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

    /** Null-safe equivalent to {@code get(row, col).flaggedDictId}. */
    public int flaggedId(@NonNegative int row, @NonNegative int col) {
        Term term = get(row, col);
        return term == null ? 0 : term.flaggedDictId;
    }

    public int localLen(@NonNegative int row, @NonNegative int col) {
        Term t = get(row, col);
        return t == null ? 0 : t.local.length;
    }

    /** Null-safe equivalent to {@code get(row, col).type()}. */
    public Term.@Nullable Type termType(int row, int col) {
        var t = get(row, col);
        return t == null ? null : t.type();
    }

    /** Null-safe equivalent to {@code get(row, col).asDatatypeId()}. */
    public @NonNegative int asDatatypeId(int row, int col) {
        var t = get(row, col);
        return t == null ? 0 : t.asDatatypeId();
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
     */
    public void writeSparql(ByteRope dest, int row, int column, PrefixAssigner prefixAssigner) {
        Term term = get(row, column);
        if (term != null)
            term.toSparql(dest, prefixAssigner);
    }

    /**
     * Appends {@code get(row, col)} to {@code dest} if the {@link Term} is not null.
     * @param dest destination where the term will be appended, in NT-syntax
     * @param row see {@link Batch#get(int, int)}
     * @param col see {@link Batch#get(int, int)}
     */
    public void writeNT(ByteRope dest, int row, int col) {
        Term t = get(row, col);
        if (t != null) dest.append(t);
    }

    /** Get a hash code for the term at column {@code col} of row {@code row}. */
    public int hash(int row, int col) {
        Term t = get(row, col);
        return t == null ? 0 : t.hashCode();
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


    /* --- --- --- mutators --- --- --- */

    /**
     * Hints that an implementation SHOULD perform allocation for an incoming sequence of
     * {@code rows} row offers/puts (see {@link Batch#beginOffer()} and {@link Batch#beginPut()}).
     *
     * <p>This method is a hint and thus subsequent {@link Batch#beginOffer()} and related
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
     * <p>Columns for the new row will be set through {@link Batch#offerTerm(Term)} calls
     * following this call, with the {@code i-th} {@link Batch#offerTerm(Term)} call setting
     * the {@code i}-th column of the new row. The row will only be visible to other methods
     * of this batch once {@link Batch#commitOffer()} returns {@code true}./p>
     *
     * <p>This method, {@link Batch#offerTerm(Term)} and {@link Batch#commitOffer()}
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
     * Offer {@code t} as the value of the next column after a {@link Batch#beginOffer()}.
     *
     * @return {@code true} iff the row offer may continue.
     * @throws IllegalStateException if there is no previously uncommitted {@code true}
     *                               {@link Batch#beginOffer()} call.
     */
    public abstract boolean offerTerm(Term t);

    /** Equivalent to {@code offer(batch.get(row, col))}. */
    public boolean offerTerm(B batch, int row, int col) {
        return offerTerm(batch.get(row, col));
    }

    /**
     * Try to commit the current {@link Batch#beginOffer()}.
     *
     * @return {@code true} iff the row was added. If {@code false}, there will be no trace of
     *         the attempted row offer.
     * @throws IllegalStateException if there is no uncommitted and {@code true}
     *                               {@link Batch#beginOffer()} call or if any
     *                               {@link Batch#offerTerm(Term)} after
     *                               {@link Batch#beginOffer()} returned {@code false}.
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
        reserve(1, other.bytesUsed(row));
        if (beginOffer()) {
            for (int c = 0; c < cols; c++) { if (!offerTerm(other, row, c)) return false; }
            return commitOffer();
        }
        return false;
    }

    /**
     * Version of {@link Batch#beginOffer()} that never rejects addition.
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

    /** Version of {@link Batch#offerTerm(Term)} that never rejects. For use with
     *  {@link Batch#beginPut()} and {@link Batch#commitPut()} */
    public abstract void putTerm(Term t);

    /** Equivalent to {@code putTerm(batch.get(row, col))}. */
    public void putTerm(B batch, int row, int col) { putTerm(batch.get(row, col)); }

    /** Version of {@link Batch#commitOffer()} that never rejects. For use with
     *  {@link Batch#beginPut()} and {@link Batch#putTerm(Term)} */
    public abstract void commitPut();

    /** Version of {@link Batch#offerRow(Batch, int)} that always add the row. */
    public void putRow(B other, int row) {
        int cols = this.cols;
        if (other.cols != cols) throw new IllegalArgumentException();
        reserve(1, other.bytesUsed(row));
        beginPut();
        for (int c = 0; c < cols; c++) putTerm(other, row, c);
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
            bytes += t == null ? 0 : t.local.length;
        reserve(1, bytes);
        beginPut();
        for (Term t : row) putTerm(t);
        commitPut();
    }

    /**
     * Equivalent to {@link Batch#putRow(Term[])}, but with a {@link Collection}
     *
     * <p>If collection items are non-null and non-{@link Term}, they will be converted using
     * {@link Rope#of(Object)} and {@link Term#valueOf(Rope)}.</p>
     *
     * @param row single row to be added
     * @throws IllegalArgumentException if {@code row.size() != this.cols}
     * @throws InvalidTermException if {@link Term#valueOf(Rope)} fails to convert a non-null,
     *                              non-Term row item
     */
    public void putRow(Collection<?> row) {
        if (row.size() != cols)
            throw new IllegalArgumentException();
        reserve(1, 0);
        beginPut();
        for (Object o : row)  putTerm(o instanceof Term t ? t : Term.valueOf(Rope.of(o)));
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
     * Equivalent to {@link Batch#put(Batch)} but accepts {@link Batch} implementations
     * other than this.
     *
     * @param other source of rows
     * @param <O> {@link Batch} concrete class
     */
    public final <O extends Batch<O>> @This B putConverting(O other) {
        int cols = other.cols;
        if (cols != this.cols) throw new IllegalArgumentException();
        if (other.getClass() == getClass()) {//noinspection unchecked
            put((B) other);
        } else {
            int rows = other.rows;
            reserve(rows, other.bytesUsed());
            for (int r = 0; r < rows; r++) {
                beginPut();
                for (int c = 0; c < cols; c++) putTerm(other.get(r, c));
                commitPut();
            }
        }
        //noinspection unchecked
        return (B)this;
    }
}
