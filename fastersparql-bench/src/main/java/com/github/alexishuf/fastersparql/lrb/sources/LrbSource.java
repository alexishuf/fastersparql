package com.github.alexishuf.fastersparql.lrb.sources;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

import static com.github.alexishuf.fastersparql.lrb.sources.SourceKind.*;

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
            case "affymetrix"               -> Affymetrix;
            case "chebi"                    -> ChEBI;
            case "dbpediasubset", "dbpedia" -> DBPedia_Subset;
            case "drugbank"                 -> DrugBank;
            case "geonames", "geonanmes"    -> GeoNames;
            case "jamendo"                  -> Jamendo;
            case "kegg"                     -> KEGG;
            case "linkedtcgaa", "ltcga"     -> LinkedTCGA_A;
            case "linkedtcgae", "ltcge"     -> LinkedTCGA_E;
            case "linkedtcgam", "ltcgm"     -> LinkedTCGA_M;
            case "lmdb", "linkedmdb"        -> LMDB;
            case "nyt", "nytimes"           -> NYT;
            case "swdfood", "swdogfood"     -> SWDFood;
            case "union", "largerdfbenchall", "all", "largerdfbench" -> LargeRDFBench_all;
            default -> throw new IllegalArgumentException("Unknown source "+cs);
        };
    }

    public String filename(SourceKind type) {
        if (type.isHdt()) {
            return name().replace('_', '-') + ".hdt";
        } else if (type.isFsStore()) {
            return name().replace('_', '-');
        } else if (type.isTdb2()) {
            return name().replace('_', '-') + ".tdb";
        } else if (this==LargeRDFBench_all && (type==COMUNICA_FED_JSON || type==COMUNICA_FED_TSV)) {
            return name().replace('_', '-') + ".hdt.list";
        } else if (type == VIRTUOSO_JSON) {
            return switch (this) {
                case Affymetrix        ->        "Affymetrix-virtuoso-7.1-64bit-linux";
                case ChEBI             ->             "ChEBI-virtuoso-7.1-64bit-linux";
                case DBPedia_Subset    ->           "DBpedia-virtuoso-7.1-64bit-linux";
                case DrugBank          ->          "DrugBank-virtuoso-7.1-64bit-linux";
                case GeoNames          ->         "GeoNanmes-virtuoso-7.1-64bit-linux";
                case Jamendo           ->           "Jamendo-virtuoso-7.1-64bit-linux";
                case KEGG              ->              "KEGG-virtuoso-7.1-64bit-linux";
                case LinkedTCGA_A      ->      "LinkedTCGA-A-virtuoso-7.1-64bit-linux";
                case LinkedTCGA_E      ->           "LTCGA-E-virtuoso-7.1-64bit-linux";
                case LinkedTCGA_M      ->           "LTCGA-M-virtuoso-7.1-64bit-linux";
                case LMDB              ->         "LinkedMDB-virtuoso-7.1-64bit-linux";
                case NYT               ->           "NYTimes-virtuoso-7.1-64bit-linux";
                case SWDFood           ->         "SWDogFood-virtuoso-7.1-64bit-linux";
                case LargeRDFBench_all -> "LargeRDFBench-all-virtuoso-7.1-64bit-linux";
            };
        }
        return name()+"."+type; // helps diagnose
    }

}
