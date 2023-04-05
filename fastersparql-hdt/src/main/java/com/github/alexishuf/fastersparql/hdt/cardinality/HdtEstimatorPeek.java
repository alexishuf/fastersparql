package com.github.alexishuf.fastersparql.hdt.cardinality;

public enum HdtEstimatorPeek {
    /** Estimator never peeks cardinality info from HDT */
    NEVER,
    /**
     * Estimator can use dictionary sizes and total triple count to improve
     * pattern-based heuristics.
     * */
    METADATA,
    /**
     * Estimator can use metadata and also the cached number of subject-object pairs
     * given a predicate.
     */
    PREDICATES,
    /**
     * Estimator can create an iterator for the triple pattern and ask an estimate
     * from the iterator
     */
    ALWAYS
}
