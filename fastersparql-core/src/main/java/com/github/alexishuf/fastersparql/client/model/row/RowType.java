package com.github.alexishuf.fastersparql.client.model.row;

import com.github.alexishuf.fastersparql.batch.operators.FilteringTransformBIt;
import com.github.alexishuf.fastersparql.client.model.Vars;
import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.Batch;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Objects;
import java.util.function.Function;

@SuppressWarnings("UnusedReturnValue")
public abstract class RowType<R, I> {
    public final Class<R> rowClass;
    public final Class<I> itemClass;

    public RowType(Class<R> rowClass, Class<I> itemClass) {
        this.rowClass = rowClass;
        this.itemClass = itemClass;
    }

    /** The {@link Class} of rows */
    public final Class<R> rowClass() { return rowClass; }

    /** The clas of items within a row. */
    @SuppressWarnings("unused") public final Class<I> itemClass() { return itemClass; }

    /**
     * Set the value of the {@code idx}-th {@code var} in {@code row} to {@code object}.
     *
     * @param row the row object.
     * @param column the index of the term within {@code row} to set
     * @param object the value to set.
     * @return the old value for {@code var} (i.e., {@link RowType#get(Object, int)})
     *         before this call.
     * @throws IndexOutOfBoundsException if {@code idx} is out of bounds or if {@code var} is
     *         not known to this row and the row implementation does not allow setting an unknown
     *         variable.
     */
    public abstract @Nullable I set(R row, int column, @Nullable I object);

    /**
     * Set the {@code i}-th item in {@code row}, which corresponds to the variable
     * {@code var} to a {@code R} instance that corresponds to the RDF term in N-Triple syntax
     * {@code nt}.
     *
     * @param row the Row where a term will be set
     * @param column the index of the term within {@code row} to set
     * @param nt an RDF value in N-Triples syntax to be set
     * @return the previous value for the set term in N-Triples syntax.
     */
    public @Nullable String setNT(R row, int column, @Nullable String nt) {
        I old = set(row, column, fromNT(nt, 0, nt == null ? 0 : nt.length()));
        return old == null ? null : old.toString();
    }

    /**
     * Convert the N-Triples representation of an RDF term in {@code source.substring(begin, end)}
     * into a term which can be put into a row using {@link RowType#set(Object, int, Object)}.
     *
     * @param source where there is a N-Triples representation of a term
     * @param begin index of the first char of the N-Triples representation in {@code source}
     * @param end index of the first char after the N-Triples representation in {@code source}
     * @return an RDF term to be used with {@link RowType#set(Object, int, Object)}.
     */
    public @Nullable I fromNT(@Nullable String source, int begin, int end) {
        if (source == null || end <= begin) return null;
        //noinspection unchecked
        return (I)source.substring(begin, end);
    }

    /**
     * Convert an RDF term into a N-Triples representation.
     *
     * @param term the term to convert, can be null
     * @return Equivalent N-Triples syntax for {@code term} or {@code null} if {@code term} is null.
     */
    public @Nullable String toNT(@Nullable I term) {
        return term == null ? null : term.toString();
    }

    /**
     * Convert an RDF term into a SPARQL representation. Equivalent to
     * {@link RowType#toNT(Object)} in the general case. For rdf:type returns {@code "a"}, and for
     * xsd:decimal/xsd:double/xsd:integer/xsd:boolean return their unquoted lexical forms.
     *
     * @param term the term to be converted into SPARQL syntax.
     * @return a String with the SPARQL representation of {@code term}.
     */
    public @Nullable String toSparql(@Nullable I term) {
        String s = toNT(term);
        if (s == null || s.isEmpty()) return null;
        if (s.length() == 49  &&  s.endsWith("-ns#type>")) return "a";
        if (s.charAt(0) == '"' && (   s.endsWith("#integer>") || s.endsWith("#decimal>")
                                   || s.endsWith("#double>")  || s.endsWith("#boolean>"))) {
            return s.substring(1, s.lastIndexOf('"'));
        }
        return s;
    }

    /**
     * Gets the value for the {@code idx}-th variable ({@code var}) at {@code row}.
     *
     * @param row the row from where to get the value. If {@code null}, must return null.
     * @param column the index of the term within {@code row} to get
     * @return the value of the variable in the given row, which may be null.
     * @throws IndexOutOfBoundsException if {@code idx} is out of bounds or if var is not
     *         expected for this row (e.g., it was not present in
     *         {@link RowType#createEmpty(Vars)}).
     */
    public abstract @Nullable I get(@Nullable R row, int column);

    /**
     * Same as {@link RowType#get(Object, int)}, but converts the returned object
     * to a String representing the RDF term in N-Triples syntax.
     *
     * @param row the row from where to get the value. If {@code null}, must return {@code null}
     * @param column the index of the term within {@code row} to get
     * @return the value of the variable in the given row as an RDF term in N-Triples syntax,
     *         which may be null.
     * @throws IndexOutOfBoundsException if {@code idx} is out of bounds or if var is not
     *         expected for this row (e.g., it was not present in
     *         {@link RowType#createEmpty(Vars)}).
     */
    public @Nullable String getNT(@Nullable R row, int column) {
        return toNT(get(row, column));
    }

    /** If necessary, convert a {@link BIt} of {@code S} rows into one of {@code R} rows. */
    public final <S> BIt<R> convert(RowType<S, ?> inType, BIt<S> in) {
        if (equals(inType)) //noinspection unchecked
            return (BIt<R>) in;
        return new ConvertingBIt<>(in, in.vars(), inType);
    }

    private final class ConvertingBIt<S> extends FilteringTransformBIt<R, S> {
        private final RowType<S, ?> inType;

        public ConvertingBIt(BIt<S> delegate, Vars vars, RowType<S, ?> inType) {
            super(delegate, rowClass, vars);
            this.inType = inType;
        }

        @Override protected Batch<R> process(Batch<S> input) {
            int n = input.size, nVars = vars.size();
            Batch<R> output = recycleOrAlloc(n);
            S[] in = input.array;
            R[] out = output.array;
            for (int i = 0; i < n; i++) {
                S inRow = in[i];
                R outRow = createEmpty(vars);
                for (int j = 0; j < nVars; j++)
                    setNT(outRow, j, inType.getNT(inRow, j));
                out[i] = outRow;
            }
            output.size = n;
            return output;
        }
    }

    /** Convert a row of type {@code S} into an instance of {@code R}. */
    public <S> R convert(RowType<S, ?> inType, Vars vars, S in) {
        R out = createEmpty(vars);
        for (int i = 0, n = vars.size(); i < n; i++)
            setNT(out, i, inType.getNT(in, i));
        return out;
    }

    private class Converter<S> implements Function<S, R> {
        private final Vars vars;
        private final RowType<S, ?> inType;

        public Converter(RowType<S, ?> inType, Vars vars) {
            this.vars = vars;
            this.inType = inType;
        }

        @Override public R apply(S inRow) {
            R out = createEmpty(vars);
            for (int i = 0, n = vars.size(); i < n; i++)
                setNT(out, i, inType.getNT(inRow, i));
            return out;
        }
    }

    /** Get a {@link Function} that converts from {@code S} to {@code R}, if required */
    public <S> Function<S, R> converter(RowType<S, ?> inType, Vars vars) {
        //noinspection unchecked
        return equals(inType) ? r -> (R) r : new Converter<>(inType, vars);
    }

    /**
     * Create a new row with {@code null} set for each variable in {@code vars}.
     *
     * @param vars the list of variables for the row. The names do not include the
     *             leading '?' or '$' markers.
     * @return a new, non-null row object.
     */
    public abstract R createEmpty(Vars vars);

    /**
     * Tests whether two rows which share the same variable lists are equals.
     *
     * @param left one row to compare
     * @param right another row to compare.
     * @return {@code} true iff {@code left} and {@code right} are null or both have
     *         the same values for all variables.
     */
    public boolean equalsSameVars(@Nullable R left, @Nullable R right) {
        return Objects.equals(left, right);
    }

    /**
     * Compute a hash code for the given row.
     *
     * @param row the row
     * @return an integer representing the hash code.
     */
    public int hash(@Nullable Object row) { return row == null ? 0 : row.hashCode(); }

    /**
     * Return a string representation of the row. This should be used for logging
     * and debug purposes.
     *
     * @param row the row, which can be null
     * @return a non-null & non-empty string representing row.
     */
    public String toString(@Nullable Object row) { return Objects.toString(row); }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof RowType<?, ?> rowType)) return false;
        return rowClass.equals(rowType.rowClass) && itemClass.equals(rowType.itemClass);
    }

    @Override public int hashCode() {
        return Objects.hash(rowClass, itemClass);
    }
}
