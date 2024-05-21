package com.github.alexishuf.fastersparql.lrb.sources;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

public enum LrbSource {
    Affymetrix,
    ChEBI,
    DBPedia_Subset,
    DrugBank,
    GeoNames,
    Jamendo,
    KEGG,
    LinkedTCGA_A,
    LinkedTCGA_E,
    LinkedTCGA_M,
    LMDB,
    NYT,
    SWDFood,
    LargeRDFBench_all;

    private static final Set<LrbSource> ALL;
    static {
        LinkedHashSet<LrbSource> mutable = new LinkedHashSet<>(Arrays.asList(values()));
        mutable.remove(LargeRDFBench_all);
        ALL = Collections.unmodifiableSet(mutable);
    }
    public static Set<LrbSource> all() { return ALL; }

    public static LrbSource tolerantValueOf(CharSequence cs) {
        return switch (cs.toString().trim().replace("-", "").replace("_", "").toLowerCase()) {
            case "affymetrix" -> Affymetrix;
            case "chebi" -> ChEBI;
            case "dbpediasubset", "dbpedia" -> DBPedia_Subset;
            case "drugbank" -> DrugBank;
            case "geonames", "geonanmes" -> GeoNames;
            case "jamendo" -> Jamendo;
            case "kegg" -> KEGG;
            case "linkedtcgaa", "ltcga" -> LinkedTCGA_A;
            case "linkedtcgae", "ltcge" -> LinkedTCGA_E;
            case "linkedtcgam", "ltcgm" -> LinkedTCGA_M;
            case "lmdb", "linkedmdb" -> LMDB;
            case "nyt", "nytimes" -> NYT;
            case "swdfood", "swdogfood" -> SWDFood;
            case "union", "largerdfbenchall", "all", "largerdfbench" -> LargeRDFBench_all;
            default -> throw new IllegalArgumentException("Unknown source "+cs);
        };
    }

    public String filename(SourceKind type) {
        if      (type.isHdt())     return name().replace('_', '-') + ".hdt";
        else if (type.isFsStore()) return name().replace('_', '-');
        else if (type.isTdb2())    return name().replace('_', '-') + ".tdb";
        else                       return name();
    }

}
