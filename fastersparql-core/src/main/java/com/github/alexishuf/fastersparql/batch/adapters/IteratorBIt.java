package com.github.alexishuf.fastersparql.batch.adapters;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.base.UnitaryBIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.Rope;
import com.github.alexishuf.fastersparql.model.rope.SharedRopes;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collection;
import java.util.Iterator;

/** Wrap a plain {@link Iterator} as a {@link BIt}. */
public class IteratorBIt<B extends Batch<B>, T> extends UnitaryBIt<B> {
    private final Iterator<T> it;

    /**
     * Create a new adapter for {@code it}.
     *
     * <p>If {@code it} implements {@link AutoCloseable}, it will be {@code close()}d when
     * this {@link IteratorBIt} finishes or is {@link IteratorBIt#close()}d.</p>
     *
     * @param it the iterator to wrap.
     * @param batchType methods for manipulating rows of type {@code R}
     */
    public IteratorBIt(Iterator<T> it, BatchType<B> batchType, Vars vars) {
        super(batchType, vars);
        this.it = it;
    }

    /** Equivalent to {@code IteratorBIt(iterable.iterator(), batchType)}. */
    public IteratorBIt(Iterable<T> iterable, BatchType<B> batchType, Vars vars) {
        this(iterable.iterator(), batchType, vars);
    }

    @Override protected void cleanup(@Nullable Throwable cause) {
        if (it instanceof AutoCloseable c) {
            try {
                c.close();
            } catch (Exception e) { throw new RuntimeException(e); }
        }
    }

    @Override protected B fetch(B dest)  {
        if (!it.hasNext()) {
            exhausted = true;
            return dest;
        }
        T next = it.next();
        return switch (next) {
            case Term[] a -> dest.putRow(a);
            case Batch<?> b -> { dest.putConverting(b); yield dest; }
            case Collection<?> coll -> dest.putRow(coll);
            case Integer i -> { // test cases compatibility
                if (dest.cols != 1)
                    throw new IllegalArgumentException("expected >1 column, cannot feed integer");
                dest.beginPut();
                dest.putTerm(0, Term.splitAndWrap(Rope.of('"', i, SharedRopes.DT_integer)));
                dest.commitPut();
                yield dest;
            }
            case Term term -> {
                dest.beginPut();
                dest.putTerm(0, term);
                dest.commitPut();
                yield dest;
            }
            case null, default ->
                    throw new IllegalArgumentException("Unexpected value from it.next(): " + next);
        };
    }

    @Override public String toString() { return toStringNoArgs()+'('+it+')'; }
}
