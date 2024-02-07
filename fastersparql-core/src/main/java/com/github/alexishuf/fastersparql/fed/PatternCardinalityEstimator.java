package com.github.alexishuf.fastersparql.fed;

import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.exceptions.BadSerializationException;
import com.github.alexishuf.fastersparql.operators.plan.TriplePattern;
import com.github.alexishuf.fastersparql.sparql.binding.Binding;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.concurrent.CompletableFuture;

import static com.github.alexishuf.fastersparql.sparql.expr.Term.RDF_TYPE;

public class PatternCardinalityEstimator extends CardinalityEstimator {
    /* --- --- --- constants --- --- --- */
    public static final String NAME = "pattern";

    public static final PatternCardinalityEstimator DEFAULT
            = new PatternCardinalityEstimator();

    /* --- --- --- lifecycle --- --- --- */

    public PatternCardinalityEstimator() {
        this(new CompletableFuture<>());
    }

    public PatternCardinalityEstimator(CompletableFuture<CardinalityEstimator> ready) {
        super(ready);
        ready.complete(this);
    }

    public static class PatternLoader implements Loader {
        @Override
        public CardinalityEstimator load(SparqlClient client, Spec spec) throws BadSerializationException {
            return new PatternCardinalityEstimator();
        }

        @Override public String name() { return NAME; }
    }

    /* --- --- --- estimation --- --- --- */

    @Override public int estimate(TriplePattern tp, @Nullable Binding binding) {
        return switch (binding == null ? tp.freeRoles() : tp.freeRoles(binding)) {
            //                                                       groundRoles
            case EMPTY       ->         1;                       // SUB_PRE_OBJ
            case OBJ         ->        20;                       // SUB_PRE
            case PRE         ->        10;                       // SUB_OBJ
            case PRE_OBJ     ->       210;                       // SUB
            case SUB         -> tp.p==RDF_TYPE ? 99_999 : 1_000; // PRE_OBJ
            case SUB_OBJ     ->   100_000;                       // PRE
            case SUB_PRE     ->    11_000;                       // OBJ
            case SUB_PRE_OBJ -> 1_000_000;                       // EMPTY
        };
    }
}
