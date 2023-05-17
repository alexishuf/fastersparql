package com.github.alexishuf.fastersparql.lrb.sources;

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
    SWDFood;

    private static final Set<LrbSource> ALL = Set.of(values());

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
            default -> throw new IllegalArgumentException("Unknown source "+cs);
        };
    }

    public String filename(SourceKind type) {
        return switch (type) {
            case HDT_FILE, HDT_TSV, HDT_JSON, HDT_WS -> name().replace('_', '-') + ".hdt";
            case FS_STORE -> name().replace('_', '-');
        };
    }

}
