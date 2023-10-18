package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.FSProperties;
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
import java.lang.invoke.VarHandle;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import static com.github.alexishuf.fastersparql.model.rope.ByteRope.EMPTY;
import static java.lang.invoke.MethodHandles.lookup;

@SuppressWarnings("BooleanMethodIsAlwaysInverted")
public abstract class Batch<B extends Batch<B>> {
    protected static final VarHandle P;
    protected static final byte P_UNPOOLED = 0;
    protected static final byte P_POOLED   = 1;
    protected static final byte P_GARBAGE  = 2;
    public static boolean SELF_VALIDATE = FSProperties.batchSelfValidate();

    static {
        try {
            P = lookup().findVarHandle(Batch.class, "plainPooled", byte.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    protected static final boolean MARK_POOLED = FSProperties.batchPooledMark();
    protected static final boolean TRACE_POOLED = FSProperties.batchPooledTrace();

    public static final TermBatchType TERM = TermBatchType.INSTANCE;
    public static final CompressedBatchType COMPRESSED = CompressedBatchType.INSTANCE;

    public int rows, cols;
    @SuppressWarnings("unused") private byte plainPooled;
    private PoolEvent[] poolTraces;

    protected Batch(int rows, int cols) {
        this.rows = rows;
        this.cols = cols;
        if (TRACE_POOLED)
            poolTraces = new PoolEvent[] {null, new UnpooledEvent(null)};
    }

    /* --- --- --- lifecycle --- --- ---  */

    protected static sealed class PoolEvent extends Exception {
        public PoolEvent(String message, PoolEvent cause) {super(message, cause);}
    }
    protected static final class PooledEvent extends PoolEvent {
        public PooledEvent(PoolEvent cause) {super("pooled here", cause);}
    }
    protected static final class UnpooledEvent extends PoolEvent {
        public UnpooledEvent(PoolEvent cause) {super("unpooled here", cause);}
    }

    /**
     * Marks the batch as pooled. After this, any interaction that is not
     * {@link #markUnpooled()}, {@link #markUnpooledNoTrace()} or {@code markGarbage()} is an error.
     * @return {@code this}
     * @throws IllegalStateException if this batch is pooled or marked as garbage
     */
    public final @This B markPooled() {
        if (MARK_POOLED) {
            if ((byte)P.compareAndExchangeRelease(this, P_UNPOOLED, P_POOLED) != P_UNPOOLED) {
                throw new IllegalStateException("pooling batch that is not unpooled",
                                                poolTraces == null ? null : poolTraces[0]);
            }
            if (TRACE_POOLED) {
                if (poolTraces == null) poolTraces = new PoolEvent[2];
                PoolEvent cause = poolTraces[1];
                if (cause != null) {
                    var acyclic = new UnpooledEvent(null);
                    acyclic.setStackTrace(cause.getStackTrace());
                    cause = acyclic;
                }
                poolTraces[0] = new PooledEvent(cause);
            }
        }
        //noinspection unchecked
        return (B)this;
    }

    /**
     * Undoes a {@link #markPooled()} without storing a stack trace as {@link #markUnpooled()}
     * could.
     * @return {@code this}
     * @throws IllegalStateException if this batch is not pooled.
     */
    public final B markUnpooledNoTrace() {
        if (MARK_POOLED && (byte)P.compareAndExchangeRelease(this, P_POOLED, P_UNPOOLED) != P_POOLED) {
            throw new IllegalStateException("un-pooling batch that is not pooled",
                                            poolTraces == null ? null : poolTraces[1]);
        }
        //noinspection unchecked
        return (B)this;
    }

    /**
     * Marks the batch as not pooled.
     * @return {@code this}
     * @throws IllegalStateException if this batch is not marked as pooled
     */
    public final @This B markUnpooled() {
        if (MARK_POOLED) {
            if ((byte)P.compareAndExchangeRelease(this, P_POOLED, P_UNPOOLED) != P_POOLED)
                throw new IllegalStateException("un-pooling batch that is not pooled",
                        poolTraces == null ? null : poolTraces[1]);
            if (TRACE_POOLED) {
                if (poolTraces == null) poolTraces = new PoolEvent[2];
                PoolEvent cause = poolTraces[0];
                if (cause != null) {
                    var acyclic = new PooledEvent(null);
                    acyclic.setStackTrace(cause.getStackTrace());
                    cause = acyclic;
                }
                poolTraces[1] = new UnpooledEvent(cause);
            }
        }
        //noinspection unchecked
        return (B)this;
    }

    /** Equivalent to {@code b == null ? null : b.}{@link #markUnpooled()}. */
    public static <B extends Batch<B>> @Nullable B asUnpooled(@Nullable B b) {
        return b == null ? null : b.markUnpooled();
    }

    /** Equivalent to {@code b == null ? null : b.}{@link #markPooled()}. */
    public static <B extends Batch<B>> @Nullable B asPooled(@Nullable B b) {
        return b == null ? null : b.markPooled();
    }

    /** Equivalent to {@code b == null ? null : b.}{@link #markUnpooledNoTrace()}{@code .}{@link #recycle()}. */
    public static <B extends Batch<B>> @Nullable B recyclePooled(@Nullable Batch<?> b) {
        //noinspection unchecked
        return b == null ? null : (B)b.markUnpooledNoTrace().recycle();
    }

    /** Throws {@link IllegalStateException} if this batch is not pooled. */
    @SuppressWarnings("unused") public void requirePooled() {
        if (MARK_POOLED && (byte)P.getOpaque(this) != P_POOLED) {
            throw new IllegalStateException("batch is not pooled",
                                            poolTraces == null ? null : poolTraces[1]);
        }
    }


    /** Throws {@link IllegalStateException} if this batch is pooled or marked garbage. */
    public void requireUnpooled() {
        if (MARK_POOLED && (byte)P.getOpaque(this) != P_UNPOOLED) {
            throw new IllegalStateException("batch is pooled",
                                            poolTraces == null ? null : poolTraces[0]);
        }
    }

//    @SuppressWarnings("removal") @Override protected void finalize() throws Throwable {
//        if (MARK_POOLED) {
//            if ((byte)P.getOpaque(this) == P_UNPOOLED) {
//                BatchEvent.Leaked.record(this);
//                PoolEvent e;
//                if (poolTraces != null && (e = poolTraces[1]) != null)
//                    //noinspection CallToPrintStackTrace
//                    new Exception("Leaked &batch="+ identityHashCode(this), e).printStackTrace();
//            }
//        }
//    }
//
    /**
     * Equivalent to {@link BatchType#recycle(Batch)} for the {@link BatchType} that created
     * this instance. MAy be a no-op for some implementations.
     *
     * @return {@code null}, for conveniently clearing references:
     *         {@code b = b.recycle();}
     */
    public abstract @Nullable B recycle();

    /**
     * Perform self-test to verify if implementation-specific invariants are valid for
     * this Batch instance.
     *
     * <p> This operation is expensive and should only be done inside {@code assert} statements.
     * Implementations of this method must honor {@link #SELF_VALIDATE}, which is
     * initialized with {@link FSProperties#batchSelfValidate()}.</p>
     *
     * @return {@code true} iff this {@link Batch} instance has not been corrupted.
     */
    public boolean validate() {
        if (!SELF_VALIDATE)
            return true;
        if (rows < 0 || cols < 0)
            return false; // negative dimensions
        if (rows*cols > termsCapacity())
            return false;
        if (!hasCapacity(rows*cols, localBytesUsed()))
            return false;
        if ((byte)P.getOpaque(this) != P_UNPOOLED)
            return false; //pooled or garbage is not valid
        //noinspection unchecked
        B self = (B)this;
        var ropeView = TwoSegmentRope.pooled();
        var termView = Term.pooledMutable();
        try {
            for (int r = 0; r < rows; r++) {
                for (int c = 0; c < cols; c++) {
                    boolean hasTerm = getView(r, c, termView);
                    boolean hasRope = getRopeView(r, c, ropeView);
                    if (hasTerm != hasRope) {
                        return false;
                    } else if (hasTerm) {
                        if (!equals(r, c, termView))
                            return false; // failed to compare with term view
                        //noinspection EqualsBetweenInconvertibleTypes
                        if (!termView.equals(ropeView))
                            return false; // string-equality failed
                        if (termView.hashCode() != hash(r, c))
                            return false; // inconsistent hash
                    } else if (!equals(r, c, self, r, c)) {
                        return false;
                    } else if (!equals(r, c, null)) {
                        return false;
                    }
                }
                if (!equals(r, self, r))
                    return false; // reflexive equality failed for row
            }
        } finally {
            ropeView.recycle();
            termView.recycle();
        }
        return true;
    }

    /* --- --- --- batch-level accessors --- --- --- */

    public abstract BatchType<B> type();

    /** Number of rows in this batch. This is not capacity, rather number of actual rows. */
    public final int rows() { return rows; }

    /** Number of columns in rows of this batch */
    public final int cols() { return cols; }

    /**
     * {@link #clear(int)}s {@code offer} or allocates a new batch and copy all contents of this
     * into {@code offer} or the new batch and return such destination batch.
     */
    public abstract B copy(@Nullable B offer);

    /**
     * Get a batch with same contents as {@code this} that can hold at least more {@code addRows}
     * rows.
     *
     * <p>If possible, this method will return {@code this}. If {@code this} does not have
     * sufficient internal capacity and allocating a new batch is more efficient, a new batch
     * containing a copy of {@code this} will be returned and {@code this} will be
     * {@link #recycle()}d. Therefore {@code this} <strong>MUST NOT be used after this method
     * returns.</strong>. Instead do:</p>
     *
     * <pre>{@code
     *   batch = batch.withCapacity(addRows, addLocal);
     *   // use possibly new batch
     * }</pre>
     *
     * @param addRows additional number of rows that may be added
     * @return If possible, {@code this}, else a new batch with same contents as {@code this}
     *         but with the requested <strong>free</strong> capacity
     */
    public abstract B withCapacity(int addRows);

    /**
     * The total number of bytes being actively used by this batch instance to store local
     * segments of terms.
     *
     * <p>Batches that do not directly store local segments will return 0. {@link CompressedBatch}
     * may return a value larger than the sum of all local segments if it has undergone an
     * in-place projection.</p>
     *
     * @return the number of bytes being used to store local segments of terms in this batch.
     */
    public int localBytesUsed() { return 0; }

    /**
     * How many bytes are being used by the {@code row}-th row of this batch to store local
     * segments of terms.
     *
     * <p>For implementations that do not store directly such segments, this will return 0. For
     * {@link CompressedBatch}, this may return a value larger than the sum of
     * {@link #localLen(int, int)} of all terms in case the local segments are not contiguous
     * (which may happen due to an in-place projection).</p>
     *
     * @param row the row to compute the number of bytes reserved for
     * @return the number of bytes reserved for locals segments of the given row.
     *         Will return zero if {@code row} is out of bounds
     */
    public int localBytesUsed(int row) {  return 0; }

    public abstract int rowsCapacity();

    /**
     * How many terms (rows*cols) can this batch hold after a {@link #clear(int)} without
     * requiring re-allocation of internal data structures.
     *
     * @return capacity of this batch in terms.
     */
    public abstract int termsCapacity();

    /**
     * How many bytes are held by this batch directly.
     *
     * <p>The value reported by this method is independent from {@link #rows} and {@link #cols},
     * since it counts memory held by the batch and {@link #clear()} does not effectively
     * release memory.</p>
     *
     * <p>Objects that can be shared by several batches are not included in this count, only
     * 4 bytes (corresponding to the references) will be counted. {@link Term} and
     * {@link SegmentRope}s are the main examples of this rule. However batch metadata and
     * UTF-8 bytes managed by the batch are counted.</p>
     *
     * @return number of bytes held exclusively by this batch.
     */
    public abstract int totalBytesCapacity();

    /**
     * Whether this batch, can hold {@code terms=rows*cols} terms summing {@code localBytes}
     * UTF-8 bytes of local segments, including new rows and rows already in this batch.
     *
     * @param terms total number of terms: {@code rows*cols}
     * @param localBytes sum of UTF-8 bytes of all local segments of all terms.
     * @return true iff this batch can hold that many terms after a {@link #clear(int)}.
     */
    public abstract boolean hasCapacity(int terms, int localBytes);

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
        requireUnpooled();
        int acc = 0;
        for (int c = 0, cols = this.cols; c < cols; c++)
            acc ^= hash(row, c);
        return acc;
    }

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

    public abstract B copyRow(int row, @Nullable B offer);

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
    public abstract @Nullable PlainRope getRope(@NonNegative int row, @NonNegative int col);

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
    public abstract boolean getView(@NonNegative int row, @NonNegative int col, Term dest);

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
    public abstract boolean getRopeView(@NonNegative int row, @NonNegative int col, TwoSegmentRope dest);

    /**
     * Change {@code dest} so that it wraps the underlying storage for the local segment of the
     * value at column {@code col} of row {@code row}in this batch.
     *
     * <p>If this batch is mutated, including appending rows, {@code dest} MUST be considered
     * invalid as its contents may have changed or will change, likely to invalid garbage.</p>
     *
     * @param row the row of the term to access
     * @param col the column containing the term of which the local segment is desired
     * @param dest A {@link SegmentRope} that will wrap the underlying storage corresponding
     *             to the local segment of the term at {@code (row, col)}.
     * @return {@code true} iff there is a term at {@code (row, col)}, otherwise {@code false} is
     *         returned and {@code dest} is unchanged.
     */
    public boolean localView(@NonNegative int row, @NonNegative int col, SegmentRope dest) {
        Term t = get(row, col);
        if (t == null) return false;
        dest.wrap(t.local());
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

    public int uncheckedLocalLen(@NonNegative int row, @NonNegative int col) {
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
    public int writeSparql(ByteSink<?, ?> dest, int row, int column, PrefixAssigner prefixAssigner) {
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
    public void writeNT(ByteSink<?, ?> dest, int row, int col) {
        TwoSegmentRope tmp = new TwoSegmentRope();
        if (getRopeView(row, col, tmp)) {
            dest.append(tmp.fst, tmp.fstU8, tmp.fstOff, tmp.fstLen);
            dest.append(tmp.snd, tmp.sndU8, tmp.sndOff, tmp.sndLen);
        }
    }

    /**
     * Null-safe equivalent to {@code dest.append(get(row, col), begin, end)}.
     */
    public void write(ByteSink<?, ?> dest, int row, int col, int begin, int end) {
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

    /** Remove all rows from this batch.*/
    public abstract void clear();

    /**
     * Remove all rows from this batch and sets columns to {@code newColumns}
     * (performing required adjustments on the backing storage.;
     *
     * @param newColumns new number of columns
     */
    public abstract @This B clear(int newColumns);

    /**
     * Ensure that this batch can receive new terms whose local segments sum to {@code addBytes}
     * without triggering an internal reallocation.
     *
     * @param addBytes expected sum of local segments UTF-8 bytes in new additions to this
     *                 batch via future {@code put*()} or {@link #beginPut()} calls
     */
    public void reserveAddLocals(int addBytes) {}

    /**
     * Starts adding a new row, whose terms will be added via {@code putTerm()} and the row will
     * become visible upon {@link #commitPut()} on the batch returned by this method.
     *
     * <p>Since {@code this} may lack capacity to store an additional row, implementations of
     * this method internally invoke {@link #withCapacity(int)} and the result of that call
     * (a batch with same contents as this, but with sufficient capacity) is returned to the
     * caller of this method. In case {@code this} did not have enough capacity, it will be
     * {@link #recycle()} internally. Therefore, callers <strong>MUST ALWAYS reassign the
     * variable holding the reference to {@code this}</strong>.</p>
     *
     * <p>Example usage:</p>
     *
     * <pre>{@code
     * batch = batch.beginPut(expectedLocalBytes);
     * for (int c = 0; c < row.length; c++) batch.put(row[c]);
     * // row is not visible yet in batch
     * batch.commitPut();
     * // batch.rows incremented and row is now visible.
     * }</pre>
     */
    public abstract B beginPut();

    /**
     * Writes {@code t} to the {@code col}-th column of the row being filled.
     *
     * <p>This must be called after {@link #beginPut()} and before {@link #commitPut()}. The
     * same column cannot be set more than once per filling row, but some {@link Batch}
     * implementations may silently allow that.</p>
     *
     * @param col column where {@code t} will be put
     * @param t the term to write
     */
    public abstract void putTerm(int col, Term t);

    /** Efficient alternative to {@link #putTerm(int, Term)} using {@code batch.get(row, col)} */
    public void putTerm(int destCol, B batch, int row, int col) {
        putTerm(destCol, batch.get(row, col));
    }

    /** Efficient alternative to {@link #putTerm(int, Term)} using {@link TermParser#asTerm()} */
    public void putTerm(int col, TermParser termParser) {
        var local = termParser.localBuf();
        int begin = termParser.localBegin;
        putTerm(col, termParser.shared(), local, begin,
                  termParser.localEnd-begin, termParser.sharedSuffixed());
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
    public void putTerm(int col, SegmentRope shared, SegmentRope local, int localOff,
                        int localLen, boolean sharedSuffix) {
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
        var localRope = new ByteRope(localLen);
        localRope.append(local, (byte[]) local.array().orElse(null), localOff, localLen);
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

    private Term makeTerm(SegmentRope shared, PlainRope local, int localOff,
                          int localLen, boolean sharedSuffix) {
        if ((shared == null || shared.len == 0) && localLen == 0)
            return null;
        ByteRope localRope = new ByteRope(localLen).append(local, localOff, localOff+localLen);
        SegmentRope fst, snd;
        if (sharedSuffix) { fst = localRope; snd =    shared; }
        else              { fst =    shared; snd = localRope; }
        return Term.wrap(fst, snd);
    }

    /**
     * Publishes the row started by the last uncommitted {@link #beginPut()}.
     */
    public abstract void commitPut();

    /**
     * Aborts the previous {@link #beginPut()} if {@link #commitPut()} has not yet been called.
     */
    public abstract void abortPut() throws IllegalStateException;

    /**
     * Equivalent to {@link #beginPut()}, followed by a sequence of
     * {@link #putTerm(int, Batch, int, int)} and a {@link #commitPut()} that would append
     * the {@code row}-th row of {@code other} to the end of {@code this} batch.
     *
     * @param other source of a row to copy
     * @param row index of the row to copy from {@code other}
     */
    public abstract B putRow(B other, int row);

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
    public final B putRow(Term[] row) {
        if (row.length != cols) throw new IllegalArgumentException();
        B dst = beginPut();
        for (int c = 0; c < row.length; c++)
            dst.putTerm(c, row[c]);
        dst.commitPut();
        return dst;
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
    public final B putRow(Collection<?> row) {
        if (row.size() != cols)
            throw new IllegalArgumentException();
        B dst = beginPut();
        int c = 0;
        for (Object o : row)
            dst.putTerm(c++, o instanceof Term t ? t : Term.valueOf(Rope.of(o)));
        dst.commitPut();
        return dst;
    }

    /**
     * Return a batch that contains all rows in {@code this}, followed by all rows in {@code other}.
     *
     * <p>If possible, {@code this} will be returned. If {@code this} cannot receive the rows
     * from {@code other} without a re-alloc of internal structures, a batch that can hold all
     * rows may be fetched from the global batch pool. When a pooled batch is used,
     * {@code this} will be marked as pooled  ({@link #markPooled()} and first offered to the
     * {@code recycled} field of {@code receiver} via
     * {@link VarHandle#compareAndExchangeRelease(Object...)} before being {@link #recycle()}d
     * in case the field already holds a batch.</p>
     *
     *
     * @param other A batch with rows that should be appended to this {@code this}
     * @param recycled A {@link VarHandle} for a field in {@code receiver} that may receive
     *                 {@code asPolled(this)} (if the field is null). If the {@link VarHandle}
     *                 itself is null, this will not be attempted before {@link #recycle()}.
     * @param receiver An object that contains a field accessed by {@code recycled} can be
     *                 null if {@code recycled} is null or refers to a static field.
     * @return {@code this} or a new batch. In both cases the returned batch will contain
     *         all rows in {@code this} and rows {@code [begin, end)} of {@code other}. If the
     *         return is not {@code this}, the caller has lost ownership over {@code this},
     *         which will now be pooled in {@code receiver} or in {@link #type()}.
     */
    public abstract B put(B other, @Nullable VarHandle recycled, Object receiver);

    /** Equivalent to {@link #put(Batch, VarHandle, Object)} without a recycled {@link VarHandle}.*/
    public abstract B put(B other);

    /**
     * Equivalent to {@link #put(Batch, VarHandle, Object)} but accepts {@link Batch} implementations
     * other than this.
     */
    public abstract B putConverting(Batch<?> other, @Nullable VarHandle recycled,
                                    @Nullable Object receiver);

    /**
     * Equivalent to {@link #putConverting(Batch, VarHandle, Object)} without a
     * {@code recycled} {@link VarHandle}
     */
    public final B putConverting(Batch<?> other) {
        return putConverting(other, null, null);
    }

    /**
     * Equivalent to {@link #putRow(Batch, int)}, but accepts a batch of another type.
     *
     * @param other a batch of which terms in the {@code row}-th row will be copied as a new row
     *              in {@code this}
     * @param row   index of the source row in {@code other}
     * @throws IllegalArgumentException  if {@code other.cols != cols}
     * @throws IndexOutOfBoundsException if {@code row < 0 || row >= other.rows}
     */
    public abstract B putRowConverting(Batch<?> other, int row);
}
