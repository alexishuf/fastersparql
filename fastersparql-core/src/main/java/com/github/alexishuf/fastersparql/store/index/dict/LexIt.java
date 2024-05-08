package com.github.alexishuf.fastersparql.store.index.dict;

import com.github.alexishuf.fastersparql.model.rope.PlainRope;
import com.github.alexishuf.fastersparql.util.owned.AbstractOwned;

public abstract class LexIt<I extends LexIt<I>> extends AbstractOwned<I> {
    public long id;

    /**
     * Resets this iterator to the position <strong>before</strong> the first term {@code t}
     * for which the SPARQL expression {@code str(t) = str(nt)} is {@code true}.
     *
     * <p>Note that {@link #advance()} must be called to move the iterator to the first term
     * {@code t}, if there is any term.</p>
     *
     * @param nt A RDF term in N-Triples syntax.
     */
    public abstract void find(PlainRope nt);

    /**
     * Moves the iterator to its next value ({@code id !=} {@link Dict#NOT_FOUND}).
     *
     * @return {@code true} iff {@code id != NOT_FOUND}, false if the iterator reached its end.
     */
    public abstract boolean advance();

    /**
     * Move the iterator to its end, causing the next {@link #advance()} call to
     * return {@code false}.
     */
    @SuppressWarnings("unused") public abstract void end();
}
