package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.model.rope.TwoSegmentRope;
import com.github.alexishuf.fastersparql.org.apache.jena.graph.Node;
import com.github.alexishuf.fastersparql.org.apache.jena.riot.system.PrefixMap;
import com.github.alexishuf.fastersparql.org.apache.jena.riot.system.Prefixes;
import com.github.alexishuf.fastersparql.org.apache.jena.riot.tokens.TokenizeTextBuilder;
import com.github.alexishuf.fastersparql.org.apache.jena.riot.tokens.TokenizerText;
import com.github.alexishuf.fastersparql.org.apache.jena.shared.impl.PrefixMappingImpl;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import com.github.alexishuf.fastersparql.util.TwoSegmentInputStream;
import com.github.alexishuf.fastersparql.util.concurrent.Alloc;
import com.github.alexishuf.fastersparql.util.owned.AbstractOwned;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import com.github.alexishuf.fastersparql.util.owned.StaticMethodOwner;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.PolyNull;

import java.lang.foreign.MemorySegment;
import java.util.function.Supplier;

import static com.github.alexishuf.fastersparql.util.owned.SpecialOwner.RECYCLED;

public abstract sealed class JenaNodeParser extends AbstractOwned<JenaNodeParser> {
    private static final int BYTES = 16 + 4*4 + 3*24 + TwoSegmentRope.BYTES;
    private static final Fac FAC = new Fac();
    private static final PrefixMap STD_PREFIX_MAP = Prefixes.adapt(new PrefixMappingImpl());
    private static final Alloc<JenaNodeParser> ALLOC = new Alloc<>(
            JenaNodeParser.class, "JenaNodeParser.ALLOC",
            Alloc.THREADS*64, FAC, BYTES);
    private static final class Fac implements Supplier<JenaNodeParser> {
        @Override public JenaNodeParser get() {
            return new Concrete().takeOwnership(RECYCLED);
        }
        @Override public String toString() {return "JenaNodeParser.Fac";}
    }

    private final TokenizeTextBuilder tokenizerBuilder;
    private final TwoSegmentInputStream tsrIS;
    private final TwoSegmentRope tsr;

    private JenaNodeParser() {
        this.tsr = new TwoSegmentRope();
        this.tsrIS = new TwoSegmentInputStream().wrap(tsr, 0, 0);
        this.tokenizerBuilder = TokenizerText.create();
    }

    public static Orphan<JenaNodeParser> create() {
        return ALLOC.create().releaseOwnership(RECYCLED);
    }

    private static final class Concrete extends JenaNodeParser
            implements Orphan<JenaNodeParser> {
        @Override public JenaNodeParser takeOwnership(Object o) {return takeOwnership0(o);}
    }

    @Override public @Nullable JenaNodeParser recycle(Object currentOwner) {
        internalMarkRecycled(currentOwner);
        if (ALLOC.offer(this) != null)
            internalMarkGarbage(currentOwner);
        return null;
    }

    public @Nullable Node makeNode0(TwoSegmentRope tsr) {
        var tokenizer = tokenizerBuilder.source(tsrIS.wrap(tsr, 0, tsr.len)).build();
        if (tokenizer.hasNext())
            return tokenizer.next().asNode(STD_PREFIX_MAP);
        return null;
    }

    /**
     * Parse an N-Triples representation of a term in {@code nt} as efficiently as possible.
     *
     * <p><strong>Caution:</strong>{@code nt} is assumed to contain a valid single N-Triples
     * representation of an RDF term at position {@code 0}. Jena's parser will throw on most
     * syntax violations, but some may not be detected</p>
     *
     * @param nt an N-Triples representation of an RDF term. If {@code null} or empty,
     *           will return {@code null}
     * @return a {@link Node} for the term in {@code nt} or {@code null} if
     *         {@code nt} is null or empty.
     */
    public @Nullable Node makeNode(@Nullable TwoSegmentRope nt) {
        return nt == null ? null : makeNode0(nt);
    }

    /**
     * Get a {@link Node} that represents the same RDF term as {@code term}
     * @param term {@code null} or a term
     * @return {@code null} if {@code term} is null, else the same RDF term, as a jena {@link Node}
     */
    public @PolyNull Node makeNode(@PolyNull Term term) {
        if (term == null)
            return null;
        tsr. wrapFirst(term. first());
        tsr.wrapSecond(term.second());
        return makeNode0(tsr);
    }

    /**
     * Get a Jena {@link Node} that represents the same RDF term the given row and
     * column of {@code batch}, if there is a term there.
     *
     * @param batch a non-null non-empty batch
     * @param row a valid row in {@code batch}
     * @param col a valid column in {@code batch}
     * @return A {@link Node} with the same value as {@code batch.get(row, col)}
     */
    public @Nullable Node makeNode(Batch<?> batch, int row, int col) {
        return batch.getRopeView(row, col, tsr) ? makeNode0(tsr) : null;
    }

    public @Nullable Node
    makeNode(MemorySegment fst, byte @Nullable[] fstU8, long fstOff, int fstLen,
             MemorySegment snd, byte @Nullable[] sndU8, long sndOff, int sndLen) {
        tsr. wrapFirst(fst, fstU8, fstOff, fstLen);
        tsr.wrapSecond(snd, sndU8, sndOff, sndLen);
        return makeNode(tsr);
    }

    public static @PolyNull Node asNode(@PolyNull Term term) {
        var p = create().takeOwnership(AS_NODE);
        try {
            return p.makeNode(term);
        } finally { p.recycle(AS_NODE); }
    }
    public static @Nullable Node asNode(TwoSegmentRope nt) {
        var p = create().takeOwnership(AS_NODE);
        try {
            return p.makeNode(nt);
        } finally { p.recycle(AS_NODE); }
    }
    public static Node asNode(Batch<?> batch, int row, int col) {
        var p = create().takeOwnership(AS_NODE);
        try {
            return p.makeNode(batch, row, col);
        } finally { p.recycle(AS_NODE); }
    }
    public static @Nullable Node
    asNode(MemorySegment fst, byte @Nullable[] fstU8, long fstOff, int fstLen,
           MemorySegment snd, byte @Nullable[] sndU8, long sndOff, int sndLen) {
        var p = create().takeOwnership(AS_NODE);
        try {
            return p.makeNode(fst, fstU8, fstOff, fstLen, snd, sndU8, sndOff, sndLen);
        } finally { p.recycle(AS_NODE); }
    }
    private static final StaticMethodOwner AS_NODE = new StaticMethodOwner("JenaNodeParser.asNode");
}
