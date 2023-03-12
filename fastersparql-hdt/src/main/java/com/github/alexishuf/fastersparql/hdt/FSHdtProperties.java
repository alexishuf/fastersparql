package com.github.alexishuf.fastersparql.hdt;

import com.github.alexishuf.fastersparql.FSProperties;

public class FSHdtProperties extends FSProperties {
    /* --- --- --- property names --- --- --- */
    public static final String MAP_PROGRESS_INTERVAL_MS = "fastersparql.hdt.map.progress.ms";

    /* --- --- --- default values --- --- --- */
    public static final int DEF_MAP_PROGRESS_INTERVAL_MS = 5_000;

    /* --- --- --- accessors --- --- --- */

    public static int mapProgressIntervalMs() { return readPositiveInt(MAP_PROGRESS_INTERVAL_MS, DEF_MAP_PROGRESS_INTERVAL_MS); }
}
