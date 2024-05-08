package com.github.alexishuf.fastersparql.sparql.expr;

import com.github.alexishuf.fastersparql.model.rope.FinalSegmentRope;
import com.github.alexishuf.fastersparql.model.rope.MutableRope;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * An immutable {@link Term} (in contrast to {@link TermView})
 */
public final class FinalTerm extends Term {

    /**
     * Get {@code term} or a new {@link FinalTerm} instance with same content.
     * If {@code term} is not a {@link FinalTerm}, {@link Term#shared()} will be taken by
     * reference and {@link Term#local()} will have its bytes copied into a
     * {@link FinalSegmentRope} to instantiate the {@link FinalTerm}.
     */
    public static FinalTerm asFinal(@Nullable Term term) {
        if (term == null)
            return null;
        if (term instanceof FinalTerm t)
            return t;
        return new FinalTerm(term.finalShared(), FinalSegmentRope.asFinal(term.local()),
                             term.sharedSuffixed());
    }

    /**
     * Equivalent to {@link FinalTerm#FinalTerm(FinalSegmentRope, SegmentRope, boolean)}
     */
    public FinalTerm(@Nullable FinalSegmentRope shared, @NonNull String local,
                     boolean suffixShared) {
        super(shared, FinalSegmentRope.asFinal(local), suffixShared);
    }

    /**
     * Build a term for {@code shared+local} or {@code local+shared}, (if {@code suffixShared}).
     *
     * <p>Both {@link SegmentRope}s are held by reference, thus changes to them will be
     * reflected on the {@link Term}.</p>
     *
     * @param shared a shared segment, ideally this should  be interned and shared among multiple
     *               Term instances. This may be null
     * @param local a segment that comes after or before (if {@code suffixShared}) {@code shared}
     *              This must not be {@code null} nor empty.
     * @param suffixShared whether {@code shared} is a suffix to {@code local}
     */
    public FinalTerm(@Nullable FinalSegmentRope shared, @NonNull SegmentRope local,
                     boolean suffixShared) {
        super(shared,
              local instanceof MutableRope m ? FinalSegmentRope.asFinal(m) : local,
              suffixShared);
    }

    /**
     * Equivalent to {@link FinalTerm#FinalTerm(FinalSegmentRope, SegmentRope, boolean)}.
     */
    public FinalTerm(@Nullable SegmentRope shared, @NonNegative SegmentRope local,
                     boolean suffixShared) {
        this(FinalSegmentRope.asFinalByReference(shared), local, suffixShared);
    }
}
