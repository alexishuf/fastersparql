package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.FSProperties;
import com.github.alexishuf.fastersparql.batch.BatchEvent;
import com.github.alexishuf.fastersparql.model.rope.*;
import com.github.alexishuf.fastersparql.sparql.PrefixAssigner;
import com.github.alexishuf.fastersparql.sparql.expr.*;
import com.github.alexishuf.fastersparql.util.owned.*;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import static com.github.alexishuf.fastersparql.FSProperties.batchSelfValidate;
import static com.github.alexishuf.fastersparql.batch.type.Batch.Validation.EXPENSIVE;
import static com.github.alexishuf.fastersparql.batch.type.Batch.Validation.NONE;
import static com.github.alexishuf.fastersparql.util.owned.SpecialOwner.HANGMAN;

@SuppressWarnings("BooleanMethodIsAlwaysInverted")
public abstract class Batch<B extends Batch<B>> extends AbstractOwned<B> {
    private static final Logger log = LoggerFactory.getLogger(Batch.class);
    protected static final boolean SELF_VALIDATE           = batchSelfValidate().ordinal() >  NONE.ordinal();
    protected static       boolean SELF_VALIDATE_EXPENSIVE = batchSelfValidate().ordinal() >= EXPENSIVE.ordinal();

    /**
     * Disable expensive checks in {@link #validate()}. This will trigger de-optimization of
     * methods that are very likely to be hot.
     *
     * <p><strong>Use only for testing purposes</strong> and ensure
     * {@link #restoreValidationCheaper()} gets called, eventually</p>
     */
    public static void makeValidationCheaper() {
        SELF_VALIDATE_EXPENSIVE = false;
    }

    /**
     * Undoes the effect of a previous {@link #makeValidationCheaper()} call
     */
    public static void restoreValidationCheaper() {
        SELF_VALIDATE_EXPENSIVE = batchSelfValidate().ordinal() >= EXPENSIVE.ordinal();
    }


    public @NonNegative short rows, cols;
    public @Nullable B next;
    protected B tail;

    protected Batch(short rows, short cols) {
        this.rows = rows;
        this.cols = cols;
        //noinspection unchecked
        this.tail = (B)this;
    }

    public enum Validation {
        NONE,
        CHEAP,
        EXPENSIVE
    }

    /* --- --- --- lifecycle --- --- ---  */

    /** Equivalent to {@link Owned#recycle(Owned, Object)}. */
    public static <B extends Batch<B>> B recycle(@Nullable Batch<?> b, Object currentOwner) {
        try {
            if (b != null)
                b.recycle(currentOwner);
        } catch (Throwable t) {
            log.error("Error recycling {}", b, t);
        }
        return null;
    }

    /** Equivalent to {@link Owned#safeRecycle(Owned, Object)}. */
    public static <B extends Batch<B>> B safeRecycle(@Nullable Batch<?> b, Object currentOwner) {
        try {
            if (b != null)
                b.recycle(currentOwner);
        } catch (Throwable t) {
            log.error("Error recycling {}", b, t);
        }
        return null;
    }

    @Override protected @Nullable B internalMarkGarbage(Object currentOwner) {
        super.internalMarkGarbage(currentOwner);
        rows = 0;
        cols = 0;
        BatchEvent.Garbage.record(this);
        return null;
    }

    @Override protected BatchLeakState makeLeakState(@Nullable OwnershipHistory history) {
        return new BatchLeakState(this, history);
    }

    protected final void updateLeakDetectorRefCapacity() {
        BatchLeakState leakState = (BatchLeakState)this.leakState;
        if (leakState != null)
            leakState.updateCapacity(this);
    }

    protected static final class BatchLeakState extends LeakDetector.LeakState {
        private int termsCapacity;
        private int bytesCapacity;

        private BatchLeakState(Batch<?> referent, @Nullable OwnershipHistory history) {
            super(referent, history);
        }

        public void updateCapacity(Batch<?> batch) {
            this.termsCapacity = batch.termsCapacity();
            this.bytesCapacity = batch.totalBytesCapacity();
        }

        @Override protected void singleThreadFillAndCommitJFR() {
            LEAKED.fillAndCommit(termsCapacity, bytesCapacity);
        }
        private static final BatchEvent.Leaked LEAKED = new BatchEvent.Leaked();

        @Override protected void appendShortProperties(StringBuilder out) {
            out.append(" termsCap=").append(termsCapacity);
            out.append(" bytesCap=").append(bytesCapacity);
        }
    }

    /**
     * Perform self-test to verify if implementation-specific invariants are valid for
     * this Batch instance.
     *
     * <p> This operation is expensive and should only be done inside {@code assert} statements.
     * Implementations of this method must honor {@link #SELF_VALIDATE}, which is
     * initialized with {@link FSProperties#batchSelfValidate()}.</p>
     *
     * @param validation maximum level of validation to execute. This will be ignored if above
     *                   what would be determined by, {@link FSProperties#batchSelfValidate()},
     *                   {@link #makeValidationCheaper()} and {@link #restoreValidationCheaper()}
     * @return {@code true} iff this {@link Batch} instance has not been corrupted.
     */
    @SuppressWarnings("unchecked")
    public final boolean validate(Validation validation) {
        if (!SELF_VALIDATE || validation == NONE)
            return true;
        if (next == this)
            return false; // cycle
        for (B prev = (B)this, b = next; b != null; b = (prev = b).next) {
            if (b.next == b || b.next == prev || b.next == this)
                return false; // cycle
        }
        requireAlive();
        if (rows == 0 && next != null)
            return false; // empty batch cannot have successor node
        var actualTail = this;
        for (B b = next; b != null; b = b.next) {
            b.requireAlive();
            actualTail = b;
            if (b.cols != cols)
                return false; // mismatching cols
            if (b.rows == 0 && b.next != null)
                return false;  // intermediary nodes should not be empty
            if (b.next == null && b.tail != b)
                return false; // actual tail has tail field not set to self
        }
        if (actualTail != this.tail)
            return false; // tail field at head must be the actual tail node
        for (var b = this; b != null; b = b.next) {
            if (!b.validateNode(validation))
                return false;
        }
        return true;
    }

    /**
     * Equivalent to {@link #validate(Validation)} with {@link Validation#EXPENSIVE}.
     * Note that {@link FSProperties#batchSelfValidate()}, and
     * {@link #makeValidationCheaper()} will disable expensive checks even though this
     * method allows them
     *
     * @return see {@link #validate(Validation)}
     */
    public final boolean validate() { return validate(EXPENSIVE); }

    protected boolean validateNode(Validation validation) {
        if (!SELF_VALIDATE || validation == NONE)
            return true;
        //noinspection ConstantValue
        if (rows < 0 || cols < 0)
            return false; // negative dimensions
        if (rows*cols > termsCapacity())
            return false;
        if (!hasCapacity(rows*cols, localBytesUsed()))
            return false;
        if (!isAlive())
            return false; //pooled or garbage is not valid
        if (tail == this && next != null)
            return false; // tail has successors
        if (tail != this && next == null)
            return false; // tail is not reachable
        if (!SELF_VALIDATE_EXPENSIVE || validation.compareTo(EXPENSIVE) < 0)
            return true;
        //noinspection unchecked
        B self = (B)this;
        try (var termView = PooledTermView.ofEmptyString();
             var ropeView = PooledTwoSegmentRope.ofEmpty()) {
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
        }
        return true;
    }

    /* --- --- --- linked list management --- --- --- */

    /**
     * The batch whose contents succeed the contents of {@code this} batch.
     *
     * @return the successor batch or {@code null} if this is the last batch in the linked list
     */
    public final @Nullable B next() { return next; }

    /**
     * Get the last batch in the linked list that starts or ends at this batch.
     *
     * @return the tail batch or {@code this} if this is the last or the only batch.
     * @throws UnsupportedOperationException() if {@code this} is neither the first nor the last
     *                                  batch in a linked list
     */
    public final B tail() {
        requireAlive();
        B tail = this.tail;
        if (tail == this) {
            if (next == null) return tail;
            for (B next = this.next; next != null; next = next.next)
                tail = next;
        }
        return tail;
    }

    protected B setTail(Orphan<B> orphanNewTail) {
        B oldTail    = this.tail;
        B newTail    = orphanNewTail.takeOwnership(oldTail);
        oldTail.tail = null;
        oldTail.next = newTail;
        this.tail    = newTail;
        newTail.tail = newTail;
        return newTail;
    }

    /**
     * Null-safe equivalent to {@code before.append(after)}.
     *
     * @param before {@code null} or a batch to which {@code after} will be
         *               {@link #append(Orphan)}'ed
     * @param owner if {@code before != null}, the current owner of {@code before}.
     *              Else, the new owner for {@code after}
     * @param after a batch to append to {@code before}, the nodes in {@code after} will be
     *              linked into {@code before} or will be recycled.
     * @return a batch owned by {@code owner} with the contents of {@code before}
     *         followed by the contents of {@code after}
     */
    public static <B extends Batch<B>>
    B append(@Nullable B before, Object owner, Orphan<B> after) {
        if (before == null)
            return after.takeOwnership(owner);
        before.append(after);
        return before;
    }

    /**
     * Appends {@code after}, BY REFERENCE, to the linked list that starts at {@code before}.
     *
     * @param before a batch that will have {@code b} to its linked list.
     *          If {@code null} or if empty and {@code b} is not null and not empty, this method
     *          will return {@code b}.
     * @param owner current owner of {@code before} and also the owner of the batch to be returned
     *              by this method (if {@code before} is null or empty).
     * @param after an orphan batch to be appended to the linked list that starts at
     *              {@code before}, if {@code before} is not null and not empty.
     * @return {@code before} if not null and not empty, else {@code after}.
     */
    public static <B extends Batch<B>> B
    quickAppend(@Nullable B before, Object owner, Orphan<B> after) {
        if (before == null)
            return after.takeOwnership(owner);
        before.requireOwner(owner);
        @SuppressWarnings("unchecked") B peek = (B)after;
        if (before.cols != peek.cols) {
            throw new IllegalArgumentException("other.cols != cols");
        } else if (before.rows == 0) {
            before.recycle(owner);
            return after.takeOwnership(owner);
        } else if (peek.tail == before.tail) {
            throw new IllegalArgumentException("cyclic quickAppend()");
        } else if (peek.rows == 0
                || (peek.rows < 8 && peek.next == null && before.tail != before
                                  && before.copySingleNodeIfFast(peek))) {
            peek.recycle(null);
        } else {
            before.quickAppend0(after);
        }
        assert before.validate();
        return before;
    }

    protected void quickAppend0(Orphan<B> nonEmpty) {
        B oldTail = this.tail, head = nonEmpty.takeOwnership(oldTail), newTail = head.tail;
        oldTail.next = head;
        oldTail.tail = newTail;
        this.tail = newTail;
    }

    protected @Nullable B quickAppend0(B nonEmpty) {
        B oldTail = this.tail, newTail = nonEmpty.tail;
        nonEmpty.requireOwner(oldTail);
        oldTail.next = nonEmpty;
        oldTail.tail = newTail;
        this.tail = newTail;
        return null;
    }

    /**
     * Equivalent to {@code this.copy(nonEmpty)} if doing so is certain to be a fast operation.
     * Else, do nothing and return {@code false}.
     *
     * @param nonEmpty A non-empty single-node batch ({@code rows > 0 && next == null})
     * @return {@code true} iff rows of {@code nonEmpty} were copied into {@code this}
     */
    protected abstract boolean copySingleNodeIfFast(B nonEmpty);

    /**
     * Detaches the first node of {@code other} and copy its contents to {@code this},
     * which MUST have:
     * <ul>
     *     <li>{@link #next()}{@code == null}</li>
     *     <li>{@link #tail()}{@code == this}</li>
     *     <li>{@link #rows}{@code == 0}</li>
     * </ul>
     *
     * <p>The first node of {@code other} will be detached from the remainder of its linked list,
     * with {@code other.next} becoming the new head of the linked list. The original {@code other}
     * will be {@link #recycle(Object)}d</p>
     *
     * @param otherOrphan the head of a linked list of batches, whose head node will be detached,
     *                    copied into this and recycled.
     * @return the linked list that starts on {@code otherOrphan.}{@link #next()}, or {@code null}
     */
    protected @Nullable Orphan<B> copyFirstNodeToEmpty(Orphan<B> otherOrphan) {
        B other = otherOrphan.takeOwnership(this);
        Orphan<B> remainder = other.detachHead();
        copy(other);
        other.recycle(this);
        return remainder;
    }


    /* --- --- --- batch-level accessors --- --- --- */

    public abstract BatchType<B> type();

    /** Number of rows in this batch. This is not capacity, rather number of actual rows. */
    public final short rows() { return rows; }

    /**
     * Get the sum of all rows in {@code this} and all subsequent batches in the
     * linked list ({@link #next()}) that starts at {@code this}.
     *
     * @return total number of rows across the whole linked list of batches.
     */
    public final int totalRows() {
        int sum = rows;
        for (B b = next; b != null; b=b.next) sum += b.rows;
        return sum;
    }

    public static int peekRows(Orphan<? extends Batch<?>> orphan) {
        return orphan == null ? 0 : ((Batch<?>)orphan).rows;
    }

    public static int peekTotalRows(Orphan<? extends Batch<?>> orphan) {
        return orphan == null ? 0 : ((Batch<?>)orphan).totalRows();
    }

    public static short peekColumns(Orphan<? extends Batch<?>> orphan) {
        return orphan == null ? 0 : ((Batch<?>)orphan).cols;
    }

    @SuppressWarnings("unchecked")
    public static <B extends Batch<B>> BatchType<B> peekType(Orphan<? extends Batch<B>> orphan) {
        return ((Batch<B>)orphan).type();
    }

    /** Number of columns in rows of this batch */
    public final short cols() { return cols; }


    /**
     * Get a new batch that contains a copy of the contents in {@code this} batch and all
     * successor nodes ({@link #next()}).
     */
    public abstract Orphan<B> dup();

    /** Equivalent to {@link #dup()} but uses {@code threadId} as a surrogate to
     * {@code Thread.currentThread().threadId()} */
    public abstract Orphan<B> dup(int threadId);

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

    /** The average {@link #localBytesUsed(int)} across all rows in this batch and its
     *  successors ({@link #next()}). */
    public int avgLocalBytesUsed() { return 0; }

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
        ArrayList<List<Term>> list = new ArrayList<>();
        for (var b = this; b != null; b = b.next) {
            for (int r = 0; r < b.rows; r++)
                list.add(b.asList(r));
        }
        return list;
    }

    @Override public int hashCode() {
        int h = cols;
        for (var b = this; b != null; b = b.next) {
            for (int r = 0, n = b.rows; r < n; r++) h ^= b.hash(r);
        }
        return h;
    }

    @Override public boolean equals(Object obj) {
        if (!(obj instanceof Batch<?> wild) || wild.cols != cols || wild.totalRows() != totalRows())
            return false;
        //noinspection unchecked
        B rhs = (B)wild, ln = (B)this;
        int lRows = ln.rows, lRow = 0;
        for (var rn = rhs; rn != null; rn = rn.next) {
            for (int r = 0; r < rn.rows; r++, ++lRow) {
                while (lRow == lRows) {
                    if ((ln = ln.next) == null) return false;
                    lRow = 0;
                    lRows = ln.rows;
                }
                if (!ln.equals(lRow, rn, r)) return false;
            }
        }
        return true;
    }

    private MutableRope appendRow(MutableRope sb, int r) {
        sb.append('[');
        for (int c = 0; c < cols; c++) {
            var t = get(r, c);
            if (t == null)
                sb.append("null");
            else
                t.toSparql(sb, PrefixAssigner.CANON);
            sb.append(", ");
        }
        return sb.unAppend(2).append(']');
    }

    @Override public String toString() {
        short rows = this.rows, cols = this.cols;
        if (rows == 0)
            return "[]";
        if (cols == 0)
            return rows == 1 ? "[[]]" : "[... "+rows+" zero-column rows ...]";
        try (var sb = PooledMutableRope.get()) {
            sb.append('[');
            if (rows == 1 && next == null) {
                appendRow(sb, 0);
            } else {
                sb.append('\n');
                for (var b = this; b != null; b = b.next) {
                    rows = b.rows;
                    for (int r = 0; r < rows; r++)
                        b.appendRow(sb, r).append('\n');
                }
            }
            return sb.append(']').toString();
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
        requireAlive();
        int acc = 0;
        for (int c = 0, cols = this.cols; c < cols; c++)
            acc ^= hash(row, c);
        return acc;
    }


    /** Analogous to {@link #hash(int)}, but row is an index relative to the linked
     *  list formed by {@link #next()} that starts at {@code this}*/
    public final int linkedHash(int row) {
        @SuppressWarnings("unchecked") B node = (B)this;
        int rel = row;
        for (; node != null && rel >= node.rows; node = node.next) rel -= node.rows;
        if (node == null) throw new IndexOutOfBoundsException(row);

        return node.hash(rel);
    }

    /** Whether rows {@code row} in {@code this} and {@code oRow} in {@code other} have the
     *  same number of columns with equal {@link Term}s in them. */
    public boolean equals(int row, B other, int oRow) {
        int cols = this.cols;
        if (other.cols != cols) return false;
        for (int c = 0; c < cols; c++) { if (!equals(row, c, other, oRow, c)) return false; }
        return true;
    }

    /** Analogous to {@link #equals(int, Batch, int)} but both {@code row} and {@code oRow} are
     *  indexes into the whole linked lists that start at {@code this} and {@code other}. */
    public final boolean linkedEquals(int row, B other, int oRow) {
        int rel = row, oRel = oRow;
        @SuppressWarnings("unchecked") B node = (B)this;
        B oNode = other;
        for (;  node != null &&  rel >=  node.rows;  node =  node.next)  rel -=  node.rows;
        for (; oNode != null && oRel >= oNode.rows; oNode = oNode.next) oRel -= oNode.rows;
        if ( node == null) throw new IndexOutOfBoundsException( "row is out of bounds");
        if (oNode == null) throw new IndexOutOfBoundsException("oRow is out of bounds");

        return node.equals(rel, oNode, oRel);
    }

    /** Whether {@code cols == other.length} and {@code equals(get(row, i), other[i])}
     * for every column {@code i}. */
    public boolean equals(int row, Term[] other) {
        int cols = this.cols;
        if (other.length != cols) return false;
        for (int c = 0; c < cols; c++) { if (!equals(row, c, other[c])) return false; }
        return true;
    }

    /** Analogous to {@link #equals(int, Term[])} but {@code row} is relative to the whole
     *  linked list that starts at {@code this} */
    public final boolean linkedEquals(int row, Term[] other) {
        @SuppressWarnings("unchecked") B node = (B)this;
        int rel = row;
        for (; node != null && rel >= node.rows; node = node.next) rel -= node.rows;
        if (node == null) throw new IndexOutOfBoundsException(row);

        return node.equals(rel, other);
    }

    /** Whether {@code cols == other.size()} and {@code equals(get(row, i), other.get(i))} for
     *  every column {@code i}. */
    public boolean equals(int row, List<Term> other) {
        int cols = this.cols;
        if (other.size() != cols) return false;
        for (int c = 0; c < cols; c++) { if (!equals(row, c, other.get(c))) return false; }
        return true;
    }

    /** Analogous to {@link #equals(int, List)} but {@code row} is relative to the whole
     *  linked list that starts at {@code this} */
    @SuppressWarnings("unused") public final boolean linkedEquals(int row, List<Term> other) {
        @SuppressWarnings("unchecked") B node = (B)this;
        int rel = row;
        for (; node != null && rel >= node.rows; node = node.next) rel -= node.rows;
        if (node == null) throw new IndexOutOfBoundsException(row);

        return node.equals(rel, other);
    }

    /** Get a string representation of the row at the given index. */
    public String toString(int row) {
        if (cols == 0) return "[]";
        try (var sb = PooledMutableRope.get().append('[')) {
            for (int i = 0, cols = this.cols; i < cols; i++) {
                var t = get(row, i);
                if (t == null)
                    sb.append("null");
                else
                    t.toSparql(sb, PrefixAssigner.CANON);
                sb.append(", ");
            }
            sb.unAppend(2);
            return sb.append(']').toString();
        }
    }

    /** Analogous to {@link #toString(int)} but {@code row} is relative to the whole
     *  linked list that starts at {@code this} */
    @SuppressWarnings("unused") public String linkedToString(int row) {
        @SuppressWarnings("unchecked") B node = (B)this;
        int rel = row;
        for (; node != null && rel >= node.rows; node = node.next) rel -= node.rows;
        if (node == null) throw new IndexOutOfBoundsException(row);

        return node.toString(row);
    }

    /***
     * Get a new batch containing only the {@code row}-th row of {@code this} batch.
     *
     * @param row the row to copy into the new batch
     * @return a new batch containing only a copy of the row.
     */
    public abstract Orphan<B> dupRow(int row);

    /** Analogous to {@link #dupRow(int)} but {@code row} is relative to the whole
     *  linked list that starts at {@code this} */
    public final Orphan<B> linkedDupRow(int row) {
        @SuppressWarnings("unchecked") B node = (B)this;
        int rel = row;
        for (; node != null && rel >= node.rows; node = node.next) rel -= node.rows;
        if (node == null) throw new IndexOutOfBoundsException(row);
        return node.dupRow(rel);
    }

    /** Equivalent to {@link #dupRow(int)} but uses {@code threadId} as a surrogate for
     *  {@code Thread.currentThread().threadId()}. */
    public abstract Orphan<B> dupRow(int row, int threadId);

    /** Analogous to {@link #dupRow(int, int)} but {@code row} is relative to the whole
     *  linked list that starts at {@code this} */
    @SuppressWarnings("unused") public final Orphan<B> linkedDupRow(int row, int threadId){
        @SuppressWarnings("unchecked") B node = (B)this;
        int rel = row;
        for (; node != null && rel >= node.rows; node = node.next) rel -= node.rows;
        if (node == null) throw new IndexOutOfBoundsException(row);

        return node.dupRow(rel, threadId);
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
    public abstract @Nullable FinalTerm get(@NonNegative int row, @NonNegative int col);

    /** Analogous to {@link #get(int, int)} but {@code row} is relative to the whole
     *  linked list that starts at {@code this} */
    public final @Nullable FinalTerm linkedGet(@NonNegative int row, @NonNegative int col) {
        @SuppressWarnings("unchecked") B node = (B)this;
        int rel = row;
        for (; node != null && rel >= node.rows; node = node.next) rel -= node.rows;
        if (node == null) throw new IndexOutOfBoundsException(row);

        return node.get(rel, col);
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
    public abstract boolean getView(@NonNegative int row, @NonNegative int col, TermView dest);

    /** Analogous to {@link #getView(int, int, TermView)} but {@code row} is relative to the whole
     * linked list that starts at {@code this}. */
    public final boolean linkedGetView(@NonNegative int row, @NonNegative int col, TermView dest) {
        @SuppressWarnings("unchecked") B node = (B)this;
        int rel = row;
        for (; node != null && rel >= node.rows; node = node.next) rel -= node.rows;
        if (node == null) throw new IndexOutOfBoundsException(row);

        return node.getView(rel, col, dest);
    }

    /**
     * If there is a value at the given {@code row} and {@code col}, modify {@code dest} to wrap
     * the NT representation of the value and return {@code true}.
     *
     * <p><strong>Warning:</strong>If the batch is mutated after this method returns, the contents
     * of {@code dest.local()} MAY change.</p>
     *
     * @param row the row index
     * @param col the column index
     * @return {@code true} iff there was a term defined at {@code (row, col)} and therefore
     *         {@code dest} was mutated.
     * @throws IndexOutOfBoundsException if {@code row} is not in {@code [0, rows)} or
     *                                   {@code col} is not  in {@code [0, cols)}
     */
    public abstract boolean getRopeView(@NonNegative int row, @NonNegative int col, TwoSegmentRope dest);

    /** Analogous to {@link #getRopeView(int, int, TwoSegmentRope)} but {@code row} is
     *  relative to the whole linked list that starts at {@code this}; */
    public final boolean linkedGetRopeView(@NonNegative int row, @NonNegative int col, TwoSegmentRope dest) {
        @SuppressWarnings("unchecked") B node = (B)this;
        int rel = row;
        for (; node != null && rel >= node.rows; node = node.next) rel -= node.rows;
        if (node == null) throw new IndexOutOfBoundsException(row);

        return node.getRopeView(rel, col, dest);
    }

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
    public boolean localView(@NonNegative int row, @NonNegative int col, SegmentRopeView dest) {
        Term t = get(row, col);
        if (t == null) return false;
        dest.wrap(t.local());
        return true;
    }

    /**
     * Analogous to  {@link #localView(int, int, SegmentRopeView)} but {@code row} is relative
     * to the whole linked list that starts at {@code this}
     */
    public final boolean linkedLocalView(@NonNegative int row, @NonNegative int col,
                                         SegmentRopeView dest) {
        @SuppressWarnings("unchecked") B node = (B)this;
        int rel = row;
        for (; node != null && rel >= node.rows; node = node.next) rel -= node.rows;
        if (node == null) throw new IndexOutOfBoundsException(row);

        return node.localView(rel, col, dest);
    }

    /** Null-safe equivalent to {@code get(row, col).shared()}. */
    public @NonNull FinalSegmentRope shared(@NonNegative int row, @NonNegative int col) {
        var term = get(row, col);
        return term == null ? FinalSegmentRope.EMPTY : term.finalShared();
    }

    /**
     * Analogous to {@link #shared(int, int)} but {@code row} is relative to the whole
     * linked list that starts at {@code this}
     */
    public final @NonNull FinalSegmentRope linkedShared(@NonNegative int row, @NonNegative int col) {
        @SuppressWarnings("unchecked") B node = (B)this;
        int rel = row;
        for (; node != null && rel >= node.rows; node = node.next) rel -= node.rows;
        if (node == null) throw new IndexOutOfBoundsException(row);

        return node.shared(rel, col);
    }

    /** Null-safe equivalent to {@code get(row, col).shared()}. */
    public boolean sharedSuffixed(@NonNegative int row, @NonNegative int col) {
        Term term = get(row, col);
        return term != null && term.sharedSuffixed();
    }

    /** Analogous to {@link #sharedSuffixed(int, int)} but {@code row} is relative to the
     *  whole linked list that starts at {@code this} */
    public boolean linkedSharedSuffixed(@NonNegative int row, @NonNegative int col) {
        @SuppressWarnings("unchecked") B node = (B)this;
        int rel = row;
        for (; node != null && rel >= node.rows; node = node.next) rel -= node.rows;
        if (node == null) throw new IndexOutOfBoundsException(row);

        return node.sharedSuffixed(rel, col);
    }

    /** Null-safe equivalent to {@code get(row, col).len}. */
    public int len(@NonNegative int row, @NonNegative int col) {
        Term t = get(row, col);
        return t == null ? 0 : t.len;
    }

    /** Analogous to {@link #len(int, int)} but {@code row} is relative to the whole
     *  linked list that starts at {@code this}. */
    public int linkedLen(@NonNegative int row, @NonNegative int col) {
        @SuppressWarnings("unchecked") B node = (B)this;
        int rel = row;
        for (; node != null && rel >= node.rows; node = node.next) rel -= node.rows;
        if (node == null) throw new IndexOutOfBoundsException(row);

        return node.len(rel, col);
    }


    /** If the term at {@code (row, col)} is a literal, return the index of the
     * closing {@code "}-quote. Else, return zero. */
    public int lexEnd(@NonNegative int row, @NonNegative int col) {
        Term t = get(row, col);
        return Math.max(t == null ? 0 : t.endLex(), 0);
    }

    /**
     * Analogous to {@link #lexEnd(int, int)} but {@code row} is relative to the whole linked
     * list that starts at {@code this}.
     */
    public int linkedLexEnd(@NonNegative int row, @NonNegative int col) {
        @SuppressWarnings("unchecked") B node = (B)this;
        int rel = row;
        for (; node != null && rel >= node.rows; node = node.next) rel -= node.rows;
        if (node == null) throw new IndexOutOfBoundsException(row);

        return node.lexEnd(rel, col);
    }

    public int localLen(@NonNegative int row, @NonNegative int col) {
        Term t = get(row, col);
        return t == null ? 0 : t.local().len;
    }

    /** Analogous to {@link #localLen(int, int)} but {@code row} is relative to the whole
     *  linked list that starts at {@code this} */
    public int linkedLocalLen(@NonNegative int row, @NonNegative int col) {
        @SuppressWarnings("unchecked") B node = (B)this;
        int rel = row;
        for (; node != null && rel >= node.rows; node = node.next) rel -= node.rows;
        if (node == null) throw new IndexOutOfBoundsException(row);

        return node.localLen(rel, col);
    }

    @SuppressWarnings("unused") public int uncheckedLocalLen(@NonNegative int row, @NonNegative int col) {
        Term t = get(row, col);
        return t == null ? 0 : t.local().len;
    }

    /** Null-safe equivalent to {@code get(row, col).type()}. */
    public Term.@Nullable Type termType(int row, int col) {
        var t = get(row, col);
        return t == null ? null : t.type();
    }

    /** Analogous to {@link #termType(int, int)} but {@code row} is relative to the whole
     *  linked list that starts at {@code this} */
    public Term.@Nullable Type linkedTermType(int row, int col) {
        @SuppressWarnings("unchecked") B node = (B)this;
        int rel = row;
        for (; node != null && rel >= node.rows; node = node.next) rel -= node.rows;
        if (node == null) throw new IndexOutOfBoundsException(row);

        return node.termType(rel, col);
    }

    /** Null-safe equivalent to {@code get(row, col).asDatatypeId()}. */
    public @Nullable SegmentRope asDatatypeSuff(int row, int col) {
        var t = get(row, col);
        return t == null ? null : t.asDatatypeSuff();
    }

    /** Analogous to {@link #asDatatypeSuff(int, int)} but {@code row} is relative to the
     *  whole linked list that starts at {@code this} */
    public @Nullable SegmentRope linkedAsDatatypeSuff(int row, int col) {
        @SuppressWarnings("unchecked") B node = (B)this;
        int rel = row;
        for (; node != null && rel >= node.rows; node = node.next) rel -= node.rows;
        if (node == null) throw new IndexOutOfBoundsException(row);

        return node.asDatatypeSuff(rel, col);
    }


    /** Null-safe equivalent to {@code get(row, col).datatypeTerm()}. */
    public @Nullable Term datatypeTerm(int row, int col) {
        var t = get(row, col);
        return t == null ? null : t.datatypeTerm();
    }


    /** Analogous to {@link #datatypeTerm(int, int)} but {@code row} is relative to the
     *  whole linked list that starts at {@code this} */
    public @Nullable Term linkedDatatypeTerm(int row, int col) {
        @SuppressWarnings("unchecked") B node = (B)this;
        int rel = row;
        for (; node != null && rel >= node.rows; node = node.next) rel -= node.rows;
        if (node == null) throw new IndexOutOfBoundsException(row);

        return node.datatypeTerm(rel, col);
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

    public int linkedWriteSparql(ByteSink<?, ?> dest, int row, int column,
                                 PrefixAssigner prefixAssigner) {
        @SuppressWarnings("unchecked") B node = (B)this;
        int rel = row;
        for (; node != null && rel >= node.rows; node = node.next) rel -= node.rows;
        if (node == null) throw new IndexOutOfBoundsException(row);

        return node.writeSparql(dest, rel, column, prefixAssigner);
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

    public void linkedWriteNT(ByteSink<?, ?> dest, int row, int col) {
        @SuppressWarnings("unchecked") B node = (B)this;
        int rel = row;
        for (; node != null && rel >= node.rows; node = node.next) rel -= node.rows;
        if (node == null) throw new IndexOutOfBoundsException(row);

        node.writeNT(dest, rel, col);
    }

    /**
     * Null-safe equivalent to {@code dest.append(get(row, col), begin, end)}.
     */
    public void write(ByteSink<?, ?> dest, int row, int col, int begin, int end) {
        Term t = get(row, col);
        if (t != null) dest.append(t, begin, end);
    }

    public void linkedWrite(ByteSink<?, ?> dest, int row, int col, int begin, int end) {
        @SuppressWarnings("unchecked") B node = (B)this;
        int rel = row;
        for (; node != null && rel >= node.rows; node = node.next) rel -= node.rows;
        if (node == null) throw new IndexOutOfBoundsException(row);

        node.write(dest, rel, col, begin, end);
    }

    /** Get a hash code for the term at column {@code col} of row {@code row}. */
    public int hash(int row, int col) {
        Term t = get(row, col);
        return t == null ? Rope.FNV_BASIS : t.hashCode();
    }

    /** Analogous to {@link #hash(int, int)} but {@code row} is relative to the whole
     *  linked list that starts at {@code this} */
    public int linkedHash(int row, int col) {
        @SuppressWarnings("unchecked") B node = (B)this;
        int rel = row;
        for (; node != null && rel >= node.rows; node = node.next) rel -= node.rows;
        if (node == null) throw new IndexOutOfBoundsException(row);

        return node.hash(rel, col);
    }


    /** Equivalent to {@code Objects.equals(get(row, col), other)} */
    public boolean equals(@NonNegative int row, @NonNegative int col, @Nullable Term other) {
        return Objects.equals(get(row, col), other);
    }

    /**
     * Analogous to {@link #equals(int, int, Term)} but {@code row} is relative to the
     * whole linked list that starts at {@code this}
     */
    public boolean linkedEquals(@NonNegative int row, @NonNegative int col, @Nullable Term other) {
        @SuppressWarnings("unchecked") B node = (B)this;
        int rel = row;
        for (; node != null && rel >= node.rows; node = node.next) rel -= node.rows;
        if (node == null) throw new IndexOutOfBoundsException(row);

        return node.equals(rel, col, other);
    }

    /** Equivalent to {@code Objects.equals(get(row, col), other)} */
    public boolean equals(@NonNegative int row, @NonNegative int col,
                          B other, int oRow, int oCol) {
        return Objects.equals(get(row, col), other.get(oRow, oCol));
    }

    /** Analogous to {@link #equals(int, int, Batch, int, int)}, but {@code row} and
     *  {@code oRow} are relative to the whole linked list that starts at {@code this} and to
     *  the whole linked list that starts at {@code other} */
    public boolean linkedEquals(@NonNegative int row, @NonNegative int col,
                                B other, int oRow, int oCol) {
        int rel = row, oRel = oRow;
        @SuppressWarnings("unchecked") B node = (B)this;
        B oNode = other;
        for (;  node != null &&  rel >=  node.rows;  node =  node.next)  rel -=  node.rows;
        for (; oNode != null && oRel >= oNode.rows; oNode = oNode.next) oRel -= oNode.rows;
        if ( node == null) throw new IndexOutOfBoundsException( "row is out of bounds");
        if (oNode == null) throw new IndexOutOfBoundsException("oRow is out of bounds");

        return node.equals(rel, col, oNode, oRel, oCol);
    }

    protected String mkOutOfBoundsMsg(int row, int col) {
        return "("+row+", "+col+") is out of bounds for batch of size ("+rows+", "+cols+")";
    }

    /* --- --- --- mutators --- --- --- */

    /**
     * Locate the first non-empty batch that succeeds {@code this} via {@link #next()} and recycle
     * all batches (including {@code this}) that precede such batch. The returned batch will be
     * made the head of what remains of the linked list.
     *
     * @param owner current owner of {@code this}
     * @return the aforementioned batch or {@code null} if there is no such batch. In any case
     *         {@code this} and any empty sucessors will be recycled.
     */
    @SuppressWarnings("unchecked") public final @Nullable B dropHead(Object owner) {
        requireOwner(owner);
        B next = this.next;
        if (next != null) {
            next.tail = this.tail;
            next.transferOwnership(this, owner);
            this.next = null;
            this.tail = (B)this;
            if (next.rows == 0) // branch should be dead
                next = next.dropEmptyHeads(owner);
        }
        recycle(owner);
        return next;
    }

    /**
     * Detach {@code this} batch, which must be the root of a {@link #next()} linked-list,
     * from the remainder of the list. Unlike {@link #dropHead(Object)}, {@code this} will not
     * be recycled.
     *
     * @return the new head of the remainder of the list, a.k.a. {@link #next()}.
     */
    public final @Nullable Orphan<B> detachHead() {
        var next = this.next;
        if (next != null) {
            next.tail = this.tail;
            //noinspection unchecked
            this.tail = (B)this;
            this.next = null;
            return next.releaseOwnership(this);
        }
        return null;
    }

    @SuppressWarnings("unchecked") protected @Nullable B dropEmptyHeads(Object owner) {
        B head = (B)this;
        while (head != null && head.rows == 0) {
            Orphan<B> remainder = detachHead();
            head.recycle(owner);
            head = remainder == null ? null : remainder.takeOwnership(owner);
        }
        return head;
    }

    /**
     * Removes {@code this.tail} from the linked list that starts at this, updating {@code this.tail} and returns the original this.tail.
     *
     * <p><strong>Important:</strong> this MAY return {@code this}, in which case
     * {@code this.tail} remains being {@code this}. Callers must check if the returned
     * tail is {@code this} and modify their behavior in order to not create references to
     * the same {@link Batch}.</p>
     *
     * @param headOwner owner of the head of the Batch linked list, i.e.,
     *                  {@code this.isOwner(headOwner) == true}
     * @return the original {@link #tail()} of {@code this}, which MAY be {@code this}.
     */
    public @NonNull Orphan<B> detachTail(Object headOwner) {
        if (this.tail == this)
            return releaseOwnership(headOwner);
        return detachDistinctTail0();
    }

    /**
     * Removes the last node in the linked-list that starts at {@code this}, <strong>if such
     * node is NOT</strong> {@code this}.
     *
     * @return The tail node as an {@link Orphan} if it is not {@code this}, else {@code null}.
     */
    public @Nullable Orphan<B> detachDistinctTail() {
        return this.tail == this ? null : detachDistinctTail0();
    }

    public static <B extends Batch<B>>
    @Nullable B detachDistinctTail(@Nullable Batch<B> head, Object newOwner) {
        if (head == null || head.tail == head)
            return null;
        return head.detachDistinctTail0().takeOwnership(newOwner);
    }
    public static <B extends Batch<B>>
    @Nullable Orphan<B> detachDistinctTail(@Nullable Batch<B> head) {
        return head == null || head.tail == head ? null : head.detachDistinctTail0();
    }

    @SuppressWarnings("unchecked") private Orphan<B> detachDistinctTail0() {
        B oldTail = this.tail;
        B nTail = (B)this;
        for (B n = next; n != null && n != oldTail; n = nTail.next)
            nTail = n;
        oldTail    = nTail.next; // defense against corruption or concurrent append
        this.tail  = nTail;
        nTail.tail = nTail;
        nTail.next = null;
        return oldTail.releaseOwnership(nTail);
    }

    /** Remove if {@code this.tail} is not {@code this}, detach it and recycle it. */
    protected void dropEmptyTail() {
        B tail = this.tail;
        if (tail != this && tail != null && tail.rows == 0)
            dropEmptyTail0();
    }

    private void dropEmptyTail0() {
        detachDistinctTail0().takeOwnership(HANGMAN).releaseOwnership(HANGMAN);
    }

    void addRowsToZeroColumns(int rows) {
        B tail = this.tail;
        while (tail.rows+rows > Short.MAX_VALUE) {
            rows -= (Short.MAX_VALUE-tail.rows);
            tail.rows = Short.MAX_VALUE;
            beginPut();
            commitPut();
            tail = this.tail;
        }
        tail.rows += (short)rows;
    }

    /** Remove all rows from this batch.*/
    public abstract void clear();

    /**
     * Remove all rows from this batch and sets columns to {@code newColumns}
     * (performing required adjustments on the backing storage.;
     *
     * @param newColumns new number of columns
     */
    public abstract B clear(int newColumns);

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
     * <p>Example usage:</p>
     *
     * <pre>{@code
     * batch.beginPut(expectedLocalBytes);
     * for (int c = 0; c < row.length; c++) batch.put(row[c]);
     * // row is not visible yet in batch
     * batch.commitPut();
     * // batch.rows incremented and row is now visible.
     * }</pre>
     */
    public abstract void beginPut();

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

    /** Analogous to {@link #putTerm(int, Batch, int, int)}, but {@code row} is relative to
     *  the whole linked list that starts at {@code batch} */
    public final void linkedPutTerm(int destCol, B batch, int row, int col) {
        B node = batch;
        int rel = row;
        for (; node != null && rel >= node.rows; node = node.next) rel -= node.rows;
        if (node == null) throw new IndexOutOfBoundsException(row);
        else              putTerm(destCol, node, rel, col);
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
    public void putTerm(int col, FinalSegmentRope shared, MemorySegment local,
                        long localOff, int localLen, boolean sharedSuffix) {
        putTerm(col, makeTerm(shared, local, localOff, localLen, sharedSuffix));
    }

    /** Analogous to {@link #putTerm(int, FinalSegmentRope, MemorySegment, long, int, boolean)} */
    public void putTerm(int col, FinalSegmentRope shared, SegmentRope local, int localOff,
                        int localLen, boolean sharedSuffix) {
        putTerm(col, makeTerm(shared, local, localOff, localLen, sharedSuffix));
    }

    /** Analogous to {@link #putTerm(int, FinalSegmentRope, MemorySegment, long, int, boolean)} */
    public void putTerm(int col, FinalSegmentRope shared, TwoSegmentRope local,
                        int localOff, int localLen, boolean sharedSuffix) {
        putTerm(col, makeTerm(shared, local, localOff, localLen, sharedSuffix));
    }

    /** Analogous to {@link #putTerm(int, FinalSegmentRope, MemorySegment, long, int, boolean)}. */
    public void putTerm(int col, FinalSegmentRope shared, byte[] local, int localOff, int localLen,
                        boolean sharedSuffix) {
        putTerm(col, makeTerm(shared, local, localOff, localLen, sharedSuffix));
    }

    private Term makeTerm(FinalSegmentRope shared, MemorySegment local, long localOff,
                          int localLen, boolean sharedSuffix) {
        if ((shared == null || shared.len == 0) && localLen == 0)
            return null;
        var localRope = RopeFactory.make(localLen).add(local, localOff, localLen).take();
        SegmentRope fst, snd;
        if (sharedSuffix) { fst = localRope; snd =    shared; }
        else              { fst =    shared; snd = localRope; }
        return Term.wrap(fst, snd);
    }

    private Term makeTerm(FinalSegmentRope shared, byte[] localU8, int localOff,
                          int localLen, boolean sharedSuffix) {
        if ((shared == null || shared.len == 0) && localLen == 0)
            return null;
        var localRope = RopeFactory.make(localLen)
                                   .add(localU8, localOff, localOff+localLen).take();
        SegmentRope fst, snd;
        if (sharedSuffix) { fst = localRope; snd =    shared; }
        else              { fst =    shared; snd = localRope; }
        return Term.wrap(fst, snd);
    }

    private Term makeTerm(SegmentRope shared, PlainRope local, int localOff,
                          int localLen, boolean sharedSuffix) {
        if ((shared == null || shared.len == 0) && localLen == 0)
            return null;
        var localRope = RopeFactory.make(localLen)
                                   .add(local, localOff, localOff+localLen).take();
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
    public abstract void putRow(B other, int row);

    /** Analogous to {@link #putRow(B, int)} but {@code row} is relative to the
     *  whole linked list that starts at {@code this} */
    public final void linkedPutRow(B other, int row) {
        int rel = row;
        for (; other != null && rel >= other.rows; other = other.next) rel -= other.rows;
        if (other == null) throw new IndexOutOfBoundsException(row);
        else               putRow(other, rel);
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
    public final B putRow(Term[] row) {
        if (row.length != cols)
            throw new IllegalArgumentException("cols mismatch");
        beginPut();
        for (int c = 0; c < row.length; c++)
            putTerm(c, row[c]);
        commitPut();
        //noinspection unchecked
        return (B)this;
    }

    public final B putRow(Term[] terms, int offset) {
        beginPut();
        for (short c = 0, cols = this.cols; c < cols; c++)
            putTerm(c, terms[offset+c]);
        commitPut();
        //noinspection unchecked
        return (B)this;
    }

    /**
     * Equivalent to {@link #putRow(Term[])}, but with a {@link Collection}
     *
     * <p>If collection items are non-null and non-{@link Term}, they will be converted using
     * {@link Term#valueOf(CharSequence)}.</p>
     *
     * @param row single row to be added
     * @throws IllegalArgumentException if {@code row.size() != this.cols}
     * @throws InvalidTermException if {@link Term#valueOf(CharSequence)} fails to convert a non-null,
     *                              non-Term row item
     */
    public final B putRow(Collection<?> row) {
        if (row.size() != cols)
            throw new IllegalArgumentException();
        beginPut();
        int c = 0;
        for (Object o : row)
            putTerm(c++, o instanceof Term t ? t : Term.valueOf(FinalSegmentRope.asFinal(o)));
        commitPut();
        //noinspection unchecked
        return (B)this;
    }

    /**
     * Copy all contents of {@code other} to the end of {@code this} batch.
     *
     * <p>Ownership of {@code other} is ALWAYS retained by the caller.</p>
     *
     * @param other A batch with rows that should be appended to this {@code this}
     */
    public abstract void copy(B other);

    /**
     * Tries to put contents of {@code other} into the end of {@code this} batch. If there is
     * not enough free capacity in {@code this}, {@code other} will be appended
     * <strong>BY REFERENCE</strong> to the linked-list pointed by {@link #next}.
     *
     * <p><strong>Important:</strong> ownership of {@code other} is always transferred from
     * the caller {@code this}. If contents of {@code other} (or of a subset of the linked list
     * starting with it) are copied, {@code other} (or a subset of the linked list) will be
     * recycled. </p>
     *
     * @param other a batch whose contents may be copied or whose reference will be appended to
     *              {@code this}. Even in case of a copy, the caller looses ownership
     */
    public abstract void append(Orphan<B> other);


    /**
     * Equivalent to {@link #copy(Batch)} but accepts {@link Batch} implementations other than this.
     */
    public abstract void putConverting(Batch<?> other);

    /**
     * Equivalent to {@link #putRow(Batch, int)}, but accepts a batch of another type.
     *
     * @param other a batch of which terms in the {@code row}-th row will be copied as a new row
     *              in {@code this}
     * @param row   index of the source row in {@code other}
     * @throws IllegalArgumentException  if {@code other.cols != cols}
     * @throws IndexOutOfBoundsException if {@code row < 0 || row >= other.rows}
     */
    public abstract void putRowConverting(Batch<?> other, int row);
}
