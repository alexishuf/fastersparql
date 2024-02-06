package com.github.alexishuf.fastersparql.hdt;

import com.github.alexishuf.fastersparql.FSProperties;
import com.github.alexishuf.fastersparql.hdt.cardinality.HdtEstimatorPeek;

public class FSHdtProperties extends FSProperties {
    /* --- --- --- property names --- --- --- */
    public static final String MAP_PROGRESS_INTERVAL_MS = "fastersparql.hdt.map.progress.ms";
    public static final String ESTIMATOR_PEEK           = "fastersparql.hdt.estimator.peek";
    public static final String ESTIMATOR_MAX_PREDICATES = "fastersparql.hdt.estimator.max-predicates";

    /* --- --- --- default values --- --- --- */
    public static final int DEF_MAP_PROGRESS_INTERVAL_MS    = 5_000;
    public static final int DEF_ESTIMATOR_MAX_PREDICATES    = 1<<16;
    public static final HdtEstimatorPeek DEF_ESTIMATOR_PEEK = HdtEstimatorPeek.STATISTICS;

    /* --- --- --- cached values --- --- --- */
    public static int CACHE_MAP_PROGRESS_INTERVAL_MS    = -1;
    public static int CACHE_ESTIMATOR_MAX_PREDICATES    = -1;
    public static HdtEstimatorPeek CACHE_ESTIMATOR_PEEK = null;

    /* --- --- --- management --- --- --- */

    public static void refresh() {
        FSProperties.refresh();
        CACHE_MAP_PROGRESS_INTERVAL_MS = -1;
        CACHE_ESTIMATOR_PEEK           = null;
        CACHE_ESTIMATOR_MAX_PREDICATES = -1;
    }

    /* --- --- --- accessors --- --- --- */

    public static int mapProgressIntervalMs() {
        int i = CACHE_MAP_PROGRESS_INTERVAL_MS;
        if (i < 0)
            CACHE_MAP_PROGRESS_INTERVAL_MS = i = readPositiveInt(MAP_PROGRESS_INTERVAL_MS, DEF_MAP_PROGRESS_INTERVAL_MS);
        return i;
    }

    public static HdtEstimatorPeek estimatorPeek() {
        HdtEstimatorPeek p = CACHE_ESTIMATOR_PEEK;
        if (p == null)
            CACHE_ESTIMATOR_PEEK = p = readEnum(ESTIMATOR_PEEK, HEP_VALUES, DEF_ESTIMATOR_PEEK);
        return p;
    }
    private static final HdtEstimatorPeek[] HEP_VALUES = HdtEstimatorPeek.values();

    public static int estimatorMaxPredicates() {
        int v = CACHE_ESTIMATOR_MAX_PREDICATES;
        if (v < 0)
            CACHE_ESTIMATOR_MAX_PREDICATES = v = readPositiveInt(ESTIMATOR_MAX_PREDICATES, DEF_ESTIMATOR_MAX_PREDICATES);
        return v;
    }
}
