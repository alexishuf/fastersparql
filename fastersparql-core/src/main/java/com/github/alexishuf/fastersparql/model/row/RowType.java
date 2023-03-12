package com.github.alexishuf.fastersparql.model.row;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.Batch;
import com.github.alexishuf.fastersparql.batch.operators.FilteringTransformBIt;
import com.github.alexishuf.fastersparql.model.BindType;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.ByteRope;
import com.github.alexishuf.fastersparql.model.rope.Rope;
import com.github.alexishuf.fastersparql.sparql.PrefixAssigner;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

import static jdk.incubator.vector.ByteVector.fromArray;

@SuppressWarnings("UnusedReturnValue")
public abstract class RowType<R> {
    private static final VectorSpecies<Byte> B_SP = ByteVector.SPECIES_PREFERRED;
    public final Class<R> rowClass;
    public final Class<?> itemClass;

    private static final class Simple<T> extends RowType<T> {
        private static final Term[] EMPTY_TERM_ARRAY = new Term[0];

        private Simple(Class<T> rowClass, Class<?> itemClass) {
            super(rowClass, itemClass);
        }
        @Override public Builder<T> builder(int columns) {
            return new ArrayBuilder(columns);
        }

        @SuppressWarnings("unchecked") @Override public T empty() {
            if      (rowClass.equals(List.class))               return (T) List.of();
            else if (rowClass.equals(Term[].class))             return (T) EMPTY_TERM_ARRAY;
            else if (Object[].class.isAssignableFrom(rowClass)) return (T) Array.newInstance(itemClass, 0);
            throw new UnsupportedOperationException("Unexpected rowClass");
        }

        @Override public String toString() {
            if (Collection.class.isAssignableFrom(rowClass))
                return rowClass.getSimpleName()+"<"+itemClass.getSimpleName()+">";
            return rowClass.getSimpleName();
        }
    }

    @SuppressWarnings("StaticInitializerReferencesSubClass")
    public static final RowType<Term[]> ARRAY = new Simple<>(Term[].class, Term.class);
    @SuppressWarnings({"StaticInitializerReferencesSubClass", "unchecked"})
    public static final RowType<List<Term>> LIST
            = new Simple<>((Class<List<Term>>)(Object)List.class, Term.class);
    @SuppressWarnings("StaticInitializerReferencesSubClass")
    public static final CompressedRow COMPRESSED = CompressedRow.INSTANCE;

    protected RowType(Class<R> rowClass, Class<?> itemClass) {
        this.rowClass = rowClass;
        this.itemClass = itemClass;
    }

    /** The {@link Class} of rows */
    @SuppressWarnings("unused") public final Class<R> rowClass() { return rowClass; }

    public interface Builder<R> {
        boolean isEmpty();
        /** Sets the given column to the given {@link Term} */
        @This Builder<R> set(int column, @Nullable Term term);

        /**
         * Equivalent to {@code set(column, inRowType.get(inRow, inCol))}.
         */
        @This Builder<R> set(int column, R inRow, int inCol);
        /**
         * Get a row with all set values. Columns that were not {@code set()} will have {@code null}s
         *
         * <p>This will automatically undo all effects of previous {@code set()} calls on
         * {@code this} {@link Builder}.</p>
         */
        R build();
    }

    protected final class ArrayBuilder implements Builder<R> {
        private Term[] arr;
        private boolean empty = true;
        private final boolean wrap;

        public ArrayBuilder(int columns) {
            this.arr = columns == 0 ? Simple.EMPTY_TERM_ARRAY : new Term[columns];
            this.wrap = rowClass.equals(List.class);
            if (this.wrap && !itemClass.equals(Term.class))
                throw new UnsupportedOperationException("List<"+itemClass+"> not supported");
            else if (!this.wrap && !rowClass.isAssignableFrom(Term[].class))
                throw new UnsupportedOperationException(rowClass+" not supported");
        }

        @Override public boolean isEmpty() {
            return empty;
        }

        @Override public @This Builder<R> set(int column, @Nullable Term term) {
            arr[column] = term;
            empty = false;
            return this;
        }

        @Override public @This Builder<R> set(int column, R inRow, int inCol) {
            arr[column] = (Term)(wrap ? ((List<?>)inRow).get(inCol) : Array.get(inRow, inCol));
            empty = false;
            return this;
        }

        @Override public R build() {
            if (arr == Simple.EMPTY_TERM_ARRAY)
                return empty();
            empty = true;
            //noinspection unchecked
            R row = (R)(wrap ? Arrays.asList(arr) : arr);
            arr = new Term[arr.length];
            return row;
        }
    }

    /** Create a new {@link Builder} that will create rows with this many columns. */
    public abstract Builder<R> builder(int columns);

    /** Create an immutable empty row (zero columns). */
    public abstract R empty();

    /** Get the number of columns in a row (includes {@code null}/unset columns). */
    public int columns(R row) {
        return switch (row) {
            case null -> 0;
            case Object[] a -> a.length;
            case Collection<?> l -> l.size();
            default -> throw new UnsupportedOperationException();
        };
    }

    /**
     * Gets the value for the {@code column}-th term at {@code row}.
     *
     * @param row the row from where to get the value.
     * @param column the index of the term within {@code row} to get
     * @return the value of the column in the given row, or null if there is no value at
     *         that column or if {@code row} is null.
     * @throws IndexOutOfBoundsException if {@code column} is negative or out of bounds for
     *                                   a non-null {@code row}
     */
    public @Nullable Term get(@Nullable R row, int column) {
        return switch (row) {
            case null -> null;
            case CharSequence[] a -> Term.valueOf(a[column]);
            case List<?> list -> {
                Object o = list.get(column);
                yield switch (o) {
                    case null -> null;
                    case CharSequence cs -> Term.valueOf(cs);
                    default -> throw new IllegalArgumentException("Not a Term");
                };
            }
            default -> throw new UnsupportedOperationException();
        };
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
    public void writeSparql(ByteRope dest, R row, int column, PrefixAssigner prefixAssigner) {
        Term term = get(row, column);
        if (term != null)
            term.toSparql(dest, prefixAssigner);
    }

    /**
     * A merger holds reusable data structures used to perform projections and merges among
     * at most two rows. Thus instances of a {@link Merger} should only ever be used by a single
     * thread. The only exception is read access to {@link Merger#outVars}. */
    public abstract class Merger {
        /** Ordered list of vars in the resulting rows produces by this {@link Merger}. */
        public final Vars outVars;
        protected final int @Nullable [] sources;

        protected Merger(Vars outVars, int @Nullable[] sources) {
            this.outVars = outVars;
            this.sources = sources;
        }

        public RowType<R> rowType() { return RowType.this; }

        /**
         * Execute a projection on row, but instead of creating a new {@code R} instance,
         * try to mutate {@code row}. If the projection cannot be done in-place, will create
         * a new instance via {@link Merger#merge(Object, Object)} with a {@code null}
         * right operand.
         *
         * <p><strong>This method is not thread safe</strong></p>
         *
         * @param row the row to be projected in-place (if possible)
         * @return Either {@code row} (mutated) or {@code merge(row, null)}.
         */
        public abstract R projectInPlace(R row);


        /** {@link Merger#projectInPlace(Object)} all rows in {@code batch} and return it. */
        public void projectInPlace(Batch<R> batch) {
            R[] a = batch.array;
            for (int i = 0, n = batch.size; i < n; i++)
                a[i] = projectInPlace(a[i]);
        }

        /**
         * Create a new {@code R} instance with terms copied from {@code left} or {@code right}
         * as specified in the {@link RowType#merger(Vars, Vars, Vars)} or
         * {@link RowType#projector(Vars, Vars)} call that created this {@link Merger}.
         *
         * <p><strong>This method is not thread safe</strong></p>
         *
         * @param left the left-side operand corresponding to {@code leftVars} in
         *             {@link RowType#merger(Vars, Vars, Vars)}
         * @param right the right-side operand corresponding to {@code rightVars} in
         *             {@link RowType#merger(Vars, Vars, Vars)}
         * @return a new {@code R} instance merging values from {@code left} and {@code right}
         */
        public abstract R merge(R left, R right);
    }

    /** An implementation of Merger */
    protected final class BuilderMerger extends Merger {
        private final Builder<R> builder;

        public BuilderMerger(Builder<R> builder, Vars out, int @Nullable [] sources) {
            super(out, sources);
            this.builder = builder;
        }

        @Override public R projectInPlace(R row) {
            if (sources == null)
                return row;
            for (int i = 0; i < sources.length; i++) {
                int s = sources[i];
                if (s < 0) throw new IllegalStateException("Not a projection Merger!");
                if (s == 0) continue;
                builder.set(i, get(row, s-1));
            }
            return builder.build();
        }

        @Override public R merge(@Nullable R left, @Nullable R right) {
            if (sources == null)
                return left;
            for (int i = 0; i < sources.length; i++) {
                int s = sources[i];
                if (s != 0)
                    builder.set(i, s > 0 ? get(left, s-1) : get(right, -s-1));
            }
            return builder.build();
        }
    }

    protected static int @Nullable [] mergerSources(Vars out, Vars leftVars, Vars rightVars) {
        int[] sources = new int[out.size()];
        boolean trivial = out.size() == leftVars.size();
        for (int i = 0; i < sources.length; i++) {
            Rope var = out.get(i);
            int s = leftVars.indexOf(var);
            trivial &= s == i;
            sources[i] = s >= 0 ? s+1 : -rightVars.indexOf(var)-1;
        }
        return trivial ? null : sources;
    }

    /** Get a {@link Merger} that only executes a projection on its left operand. */
    public Merger projector(Vars out, Vars in) {
        return new BuilderMerger(builder(out.size()), out, mergerSources(out, in, Vars.EMPTY));
    }
    /**
     * Get a merger that builds a new row taking values from both a left and a right row.
     *
     * <p>If {@code leftVars.containsAll(out)}, this will be equivalent to
     * {@link RowType#projector(Vars, Vars)}.</p>
     *
     * @param out the variables of the result (merged) row
     * @param left the variables present in {@code left} parameter of
     *        {@link Merger#merge(Object, Object)}
     * @param right the variables present in {@code right} parameter of
     *        {@link Merger#merge(Object, Object)}
     * @return a new {@link Merger}
     */
    public Merger merger(Vars out, Vars left, Vars right) {
        return new BuilderMerger(builder(out.size()), out, mergerSources(out, left, right));
    }

    /** Equivalent to {@code merger(bindType.resultVars(left, right), left, right)} */
    public final Merger merger(BindType bindType, Vars left, Vars right) {
        return merger(bindType.resultVars(left, right), left, right);
    }

    /** If necessary, convert a {@link BIt} of {@code S} rows into one of {@code R} rows. */
    public final <S> BIt<R> convert(BIt<S> in) {
        if (in == null || equals(in.rowType())) //noinspection unchecked
            return (BIt<R>) in;
        return new ConvertingBIt<>(in, in.vars());
    }

    public final <S> Batch<R> convert(RowType<S> inType, Batch<S> in) {
        if (equals(inType)) //noinspection unchecked
            return (Batch<R>) in;
        if (in.size == 0)
            return Batch.terminal();
        Batch<R> out = new Batch<>(rowClass, in.size);
        S[] inArray = in.array;
        R[] outArray = out.array;
        int nVars = inType.columns(inArray[0]);
        Builder<R> b = builder(nVars);
        for (int i = 0; i < inArray.length; i++) {
            S row = inArray[i];
            for (int col = 0; col < nVars; col++)
                b.set(col, inType.get(row, col));
            outArray[i] = b.build();
        }
        out.size = in.size;
        return out;
    }

    private final class ConvertingBIt<S> extends FilteringTransformBIt<R, S> {
        private final RowType<S> inType;
        private final Builder<R> builder;

        public ConvertingBIt(BIt<S> delegate, Vars vars) {
            super(delegate, RowType.this, vars);
            inType = delegate.rowType();
            builder = builder(vars.size());
        }

        @Override protected Batch<R> process(Batch<S> inBatch) {
            int n = inBatch.size, nVars = vars.size();
            Batch<R> output = recycleOrAlloc(n);
            S[] in = inBatch.array;
            R[] out = output.array;
            for (int i = 0; i < n; i++) {
                S inRow = in[i];
                for (int j = 0; j < nVars; j++)
                    builder.set(j, inType.get(inRow, j));
                out[i] = builder.build();
            }
            output.size = n;
            return output;
        }
    }

    private class Converter<S> implements Function<S, R> {
        private final Vars vars;
        private final RowType<S> inType;
        private final Builder<R> builder;

        public Converter(RowType<S> inType, Vars vars) {
            this.vars = vars;
            this.inType = inType;
            this.builder = builder(vars.size());
        }

        @Override public R apply(S inRow) {
            for (int i = 0, n = vars.size(); i < n; i++)
                builder.set(i, inType.get(inRow, i));
            return builder.build();
        }
    }

    /** Get a {@link Function} that converts from {@code S} to {@code R}, if required */
    public <S> Function<S, R> converter(RowType<S> inType, Vars vars) {
        //noinspection unchecked
        Function<S, R> id = (Function<S, R>) Function.identity();
        return equals(inType) ? id : new Converter<>(inType, vars);
    }

    /**
     * Tests whether two rows which share the same variable lists are equals.
     *
     * @param left one row to compare
     * @param right another row to compare.
     * @return {@code} true iff {@code left} and {@code right} are null or both have
     *         the same values for all variables.
     */
    public boolean equalsSameVars(@Nullable R left, @Nullable R right) {
        int n = columns(left);
        if (columns(right) != n) return false;
        for (int i = 0; i < n; i++) {
            if (!Objects.equals(get(left, i), get(right, i))) return false;
        }
        return true;
    }

    /**
     * Compute a hash code for the given row.
     *
     * @param r the row
     * @return an integer representing the hash code.
     */
    public int hash(@Nullable R r) {
        int acc = 0, bit = 24;
        for (int col = 0, n = columns(r); col < n; col++) {
            var t = get(r, col);
            if (t != null) {
                acc ^= t.flaggedDictId;
                byte[] local = t.local;
                int i = 0;
                for (int e = B_SP.loopBound(local.length); i < e; i += B_SP.length())
                    acc ^= fromArray(B_SP, local, i).reduceLanes(VectorOperators.XOR);
                while (i < local.length)
                    acc ^= (0xff & local[i++]) << (bit = (bit+8) & 31);
            }
        }
        return acc;
    }

    /**
     * Return a string representation of the row. This should be used for logging
     * and debug purposes.
     *
     * @param row the row, which can be null
     * @return a non-null & non-empty string representing row.
     */
    public final String toString(@Nullable R row) {
        var sb = new StringBuilder().append('[') ;
        for (int i = 0, n = columns(row); i < n; i++) {
            Term term = get(row, i);
            sb.append(term == null ? "null" : term.toSparql()).append(", ");
        }
        if (sb.length() > 1)
            sb.setLength(sb.length()-2);
        return sb.append(']').toString();
    }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        return o instanceof RowType<?> r && rowClass.equals(r.rowClass)
                                         && itemClass.equals(r.itemClass);
    }

    @Override public int hashCode() { return rowClass.hashCode()*31 + itemClass.hashCode(); }

    @Override public String toString() {
        return super.toString();
    }
}
