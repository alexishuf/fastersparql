package com.github.alexishuf.fastersparql.fed;

import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.exceptions.BadSerializationException;
import com.github.alexishuf.fastersparql.operators.plan.TriplePattern;
import com.github.alexishuf.fastersparql.sparql.binding.Binding;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.IOException;

public class PatternCardinalityEstimator extends CardinalityEstimator {
    /* --- --- --- constants --- --- --- */
    public static final String NAME = "pattern";
    public static final String PENALTY = "penalty";
    public static final int DEFAULT_PENALTY = 1_000;

    public static final PatternCardinalityEstimator DEFAULT
            = new PatternCardinalityEstimator(DEFAULT_PENALTY);

    /* --- --- --- lifecycle --- --- --- */

    protected final int uncertaintyPenalty;

    public PatternCardinalityEstimator(int uncertaintyPenalty) {
        this.uncertaintyPenalty = uncertaintyPenalty;
    }

    public static class PatternLoader implements Loader {
        @Override
        public CardinalityEstimator load(SparqlClient client, Spec spec) throws IOException, BadSerializationException {
            return new PatternCardinalityEstimator(spec.getOr(PENALTY, DEFAULT_PENALTY));
        }

        @Override public String name() { return NAME; }
    }

    /* --- --- --- estimation --- --- --- */

    @Override public int estimate(TriplePattern tp, @Nullable Binding binding) {
        return switch (binding == null ? tp.varRoles() : tp.varRoles(binding)) {
            //                              groundRoles
            case EMPTY       -> 1;       // SUB_PRE_OBJ
            case OBJ         -> 20;      // SUB_PRE
            case PRE         -> 10;      // SUB_OBJ
            case PRE_OBJ     -> 100;     // SUB
            case SUB         -> 1_000;   // PRE_OBJ
            case SUB_OBJ     -> 10_000;  // PRE
            case SUB_PRE     -> 2_000;   // OBJ
            case SUB_PRE_OBJ -> 10_0000; // EMPTY
        } + uncertaintyPenalty;
    }
}
