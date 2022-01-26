package com.github.alexishuf.fastersparql.operators.row;

import com.github.alexishuf.fastersparql.client.model.Results;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

public interface RowOperations {
    /**
     * Set the value of the {@code idx}-th {@code var} in {@code row} to {@code object}.
     *
     * @param row the row object.
     * @param idx the index of {@code var} in the list of {@link Results#vars()} from where
     *            the row originated.
     * @param var the name of the variable to be set.
     * @param object the value to set.
     * @return the old value for {@code var} (i.e., {@link RowOperations#get(Object, int, String)}
     *         before this call.
     * @throws IndexOutOfBoundsException if {@code idx} is out of bounds or if {@code var} is
     *         not known to this row and the row implementation does not allow setting a unknown
     *         variable.
     */
    @Nullable Object set(Object row, int idx, String var, @Nullable Object object);

    /**
     * Gets the value for the {@code idx}-th variable ({@code var}) at {@code row}.
     *
     * @param row the row from where to get the value. If {@code null}, must return null.
     * @param idx the index of the variable to read in the {@link Results#vars()} from which
     *            the row originated.
     * @param var the name of the variable to be read
     * @return the value of the variable in the given row, which may be null.
     * @throws IndexOutOfBoundsException if {@code idx} is out of bounds or if var is not
     *         expected for this row (e.g., it was not present in
     *         {@link RowOperations#createEmpty(List)}).
     */
    @Nullable Object get(@Nullable Object row, int idx, String var);

    /**
     * Create a new row with {@code null} set for each variable in {@code vars}.
     *
     * @param vars the variables of the row. The order of this list determines the ordering
     *             for positional row implementations. See {@link Results}.
     * @return a new, non-null row object.
     */
    Object createEmpty(List<String> vars);

    /**
     * Tests whether two rows which share the same variable lists are equals.
     *
     * @param left one row to compare
     * @param right another row to compare.
     * @return {@code} true iff {@code left} and {@code right} are null or both have
     *         the same values for all variables.
     */
    boolean equalsSameVars(@Nullable Object left, @Nullable Object right);

    /**
     * Compute a hash code for the given row.
     *
     * @param row the row
     * @return an integer representing the hash code.
     */
    int hash(@Nullable Object row);

    /**
     * Whether {@link RowOperations#hash(Object)} is expected to be different from
     * {@link java.util.Objects#hashCode(Object)}
     *
     * @return {@code false} iff {@code hash(o) == Objects.hashCode(o)} for every {@code o}.
     */
    boolean needsCustomHash();

    /**
     * Create a {@link RowMatcher} for this row type where the rows it receives have the
     * given variable lists.
     *
     * @param leftVars the variables of the {@code left} arguments to be given to
     *                 {@link RowMatcher#matches(Object, Object)}.
     * @param rightVars the variables of the {@code right} arguments to be given to
     *                  {@link RowMatcher#matches(Object, Object)}.
     * @return a new, non-null {@link RowMatcher}.
     */
    RowMatcher createMatcher(List<String> leftVars, List<String> rightVars);
}
