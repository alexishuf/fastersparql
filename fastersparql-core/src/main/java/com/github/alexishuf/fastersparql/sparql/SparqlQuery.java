package com.github.alexishuf.fastersparql.sparql;

import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.ByteRope;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import com.github.alexishuf.fastersparql.sparql.binding.Binding;

import static java.nio.charset.StandardCharsets.UTF_8;

public interface SparqlQuery {
    /** Gets the SPARQL representation of this query. The representation may use prefixed IRIs. */
    SegmentRope sparql();

    /** Whether this is an ASK query */
    boolean isAsk();

    /** Whether this is a {@code CONSTRUCT} or {@code DESCRIBE} query */
    boolean isGraph();

    /** List of unique var names present in the result set of this query */
    Vars publicVars();

    Vars allVars();

    /**
     * Get this query as an ASK query. This replaces "SELECT ..." with "ASK"
     */
    SparqlQuery toAsk();

    /**
     * It this is a non-DISTINCT SELECT, get it as SELECT DISTINCT, else return {@code this}.
     */
    SparqlQuery toDistinct(DistinctType distinctType);

    /**
     * Get a query with all replacing all variables with their values in {@code binding}.
     *
     * <p>The implementation does a no-op check and will return {@code this} if no variable in
     * {@code binding} appears in the SPARQL string. If a bound variable appears in the
     * projection list (e.g., "SELECT ?bound" or "SELECT (... AS ?bound)"), it will be removed
     * from the projection. If the bound query would have an empty projection list, SELECT will
     * be replaced with an ASK.</p>
     *
     * @return a {@link SparqlQuery} for the bound query or {@code this} if no var is bound.
     */
    SparqlQuery bound(Binding binding);

    enum DistinctType {
        STRONG,
        WEAK,
        DEDUP;

        private static final ByteRope DISTINCT_ROPE = new ByteRope("DISTINCT".getBytes(UTF_8));
        private static final ByteRope  REDUCED_ROPE = new ByteRope( "REDUCED".getBytes(UTF_8));
        private static final ByteRope   PRUNED_ROPE = new ByteRope(  "PRUNED".getBytes(UTF_8));

        public ByteRope sparql() {
            return switch (this) {
                case STRONG -> DISTINCT_ROPE;
                case WEAK   ->  REDUCED_ROPE;
                case DEDUP  ->   PRUNED_ROPE;
            };
        }
    }
}
