package com.github.alexishuf.fastersparql.client.model.batch.adapters;

import com.github.alexishuf.fastersparql.client.model.batch.BatchIt;
import com.github.alexishuf.fastersparql.client.model.batch.base.UnitaryBatchIt;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Iterator;

/** Wrap a plain {@link Iterator} as a {@link BatchIt}. */
public class IteratorBatchIt<T> extends UnitaryBatchIt<T> {
    private final Iterator<T> it;

    /** Equivalent to {@code this(it, elementCls, it.toString())}. */
    public IteratorBatchIt(Iterator<T> it, Class<T> elementCls) {
        this(it, elementCls, it.toString());
    }

    /**
     * Create a new adapter for {@code it}.
     *
     * <p>If {@code it} imlements {@link AutoCloseable}, it will be {@code close()}d when
     * this {@link IteratorBatchIt} finishes or is {@link IteratorBatchIt#close()}d.</p>
     *
     * @param it the iterator to wrap.
     * @param elementCls the class of elements produced by {@code it.next()}.
     * @param name the name of the new {@link IteratorBatchIt}.
     *             If null, will use {@code it.toString()}
     */
    public IteratorBatchIt(Iterator<T> it, Class<T> elementCls, @Nullable String name) {
        super(elementCls, name == null ? it.toString() : name);
        this.it = it;
    }

    @Override protected void cleanup() {
        if (it instanceof AutoCloseable c) {
            try {
                c.close();
            } catch (Exception e) { throw new RuntimeException(e); }
        }
    }

    @Override public boolean hasNext() {
        return it.hasNext();
    }

    @Override public T next() {
        return it.next();
    }
}
