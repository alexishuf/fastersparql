package com.github.alexishuf.fastersparql.hdt.cardinality;

public enum HdtEstimatorPeek {
    /** Estimator never peeks cardinality info from HDT */
    NEVER,
    /**
     * Use statistics such as number of class instances and number of subject-object pairs
     * per predicate
     */
    STATISTICS,
    /**
     * Estimator can create an iterator for the triple pattern and ask an estimate
     * from the iterator
     */
    ALWAYS
}
