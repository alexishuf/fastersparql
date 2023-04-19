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

    public String filename(SourceKind type) {
        return switch (type) {
            case HDT, HDT_TSV, HDT_JSON, HDT_WS -> hdtFilename();
        };
    }

    public String hdtFilename() { return name().replace('_', '-'); }
}
