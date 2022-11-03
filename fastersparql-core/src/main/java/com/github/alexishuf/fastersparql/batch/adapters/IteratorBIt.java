package com.github.alexishuf.fastersparql.batch.adapters;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.base.UnitaryBIt;
import com.github.alexishuf.fastersparql.client.model.Vars;

import java.util.Iterator;

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
     * @param elementCls the class of elements produced by {@code it.next()}.
     */
    public IteratorBIt(Iterator<T> it, Class<? super T> elementCls, Vars vars) {
        super(elementCls, vars);
        this.it = it;
    }

    /** Equivalent to {@code IteratorBIt(iterable.iterator(), elementCls)}. */
    public IteratorBIt(Iterable<T> iterable, Class<? super T> elementCls, Vars vars) {
        this(iterable.iterator(), elementCls, vars);
    }

    @Override protected void cleanup(boolean interrupted) {
        if (it instanceof AutoCloseable c) {
            try {
                c.close();
            } catch (Exception e) { throw new RuntimeException(e); }
        }
    }

    @Override public boolean hasNext() {
        boolean has = it.hasNext();
        if (!has)
            onExhausted();
        return has;
    }

    @Override public T      next()     { return it.next(); }
    @Override public String toString() { return it.toString(); }
}
