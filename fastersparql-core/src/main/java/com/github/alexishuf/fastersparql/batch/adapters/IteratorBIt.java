package com.github.alexishuf.fastersparql.batch.adapters;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.base.UnitaryBIt;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.row.RowType;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Iterator;
import java.util.NoSuchElementException;

/** Wrap a plain {@link Iterator} as a {@link BIt}. */
public class IteratorBIt<T> extends UnitaryBIt<T> {
    private final Iterator<T> it;

    /**
     * Create a new adapter for {@code it}.
     *
     * <p>If {@code it} implements {@link AutoCloseable}, it will be {@code close()}d when
     * this {@link IteratorBIt} finishes or is {@link IteratorBIt#close()}d.</p>
     *
     * @param it the iterator to wrap.
     * @param rowType methods for manipulating rows of type {@code R}
     */
    public IteratorBIt(Iterator<T> it, RowType<T> rowType, Vars vars) {
        super(rowType, vars);
        this.it = it;
    }

    /** Equivalent to {@code IteratorBIt(iterable.iterator(), rowType)}. */
    public IteratorBIt(Iterable<T> iterable, RowType<T> rowType, Vars vars) {
        this(iterable.iterator(), rowType, vars);
    }

    @Override protected void cleanup(@Nullable Throwable cause) {
        if (it instanceof AutoCloseable c) {
            try {
                c.close();
            } catch (Exception e) { throw new RuntimeException(e); }
        }
    }

    @Override public boolean hasNext() {
        boolean has = it.hasNext();
        if (!has)
            onTermination(null);
        return has;
    }

    @Override public T next() {
        try {
            return it.next();
        } catch (Throwable t) {
            if (!(t instanceof NoSuchElementException)) onTermination(t);
            throw t;
        }

    }
    @Override public String toString() { return it.toString(); }
}
