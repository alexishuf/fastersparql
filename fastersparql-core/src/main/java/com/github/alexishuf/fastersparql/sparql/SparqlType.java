package com.github.alexishuf.fastersparql.sparql;

import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;

public enum SparqlType {
    /**
     * The algebra expression has a SPARQL representation.
     */
    SPARQL,

    /**
     * The SPARQL will contain a SELECT * and there will be variables in its body. However,
     * the algebra represented an empty projection that is not an ASK nor a SELECT.
     *
     * <p>Calling {@link #sparql(SparqlQuery)} will return a SELECT * that will expose
     * all variables of the query, unlike what was intended by the algebra representation.
     * The results of such SPARQL query should be manually wrapped in a
     * {@link BatchType#projector(Vars, Vars)} to achieve the desired zero column batches. </p>
     */
    REQUIRES_MANUAL_PROJECTION;

    public SegmentRope sparql(SparqlQuery sparqlQuery) {
        return ((SparqlGenerator)sparqlQuery).generateSparql();
    }

    public interface SparqlGenerator {
        SegmentRope generateSparql();
    }
}
