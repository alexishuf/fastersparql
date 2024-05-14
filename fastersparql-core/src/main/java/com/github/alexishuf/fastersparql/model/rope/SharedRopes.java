package com.github.alexishuf.fastersparql.model.rope;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

public class SharedRopes {
    private static final VarHandle HASHES = MethodHandles.arrayElementVarHandle(int[].class);

    private static final int SKIP_INTERNED_DTYPE_BEGIN = 15; // "^^<http://www.
    private static final int   SKIP_INTERNED_IRI_BEGIN = 12; // <http://www.
    private static final int         SKIP_INTERNED_END = 1;  // / # >
    public static final int    MIN_INTERNED_LEN = SKIP_INTERNED_IRI_BEGIN + 1 + SKIP_INTERNED_END;

    private static final int BUCKET_BITS = 4;
    private static final int BUCKET_SIZE = 1<<BUCKET_BITS;
    public static final SharedRopes SHARED_ROPES = SharedRopes.withBytesCapacity(96*1024*1024);

    public static final FinalSegmentRope P_XSD = SHARED_ROPES.internPrefix("<http://www.w3.org/2001/XMLSchema#");
    public static final FinalSegmentRope P_RDF = SHARED_ROPES.internPrefix("<http://www.w3.org/1999/02/22-rdf-syntax-ns#");

    private static final String XSD_DT = "\"^^<http://www.w3.org/2001/XMLSchema#";
    private static final String RDF_DT = "\"^^<http://www.w3.org/1999/02/22-rdf-syntax-ns#";

    public static final FinalSegmentRope DT_duration           = SHARED_ROPES.internDatatype(XSD_DT+"duration>");
    public static final FinalSegmentRope DT_dateTime           = SHARED_ROPES.internDatatype(XSD_DT+"dateTime>");
    public static final FinalSegmentRope DT_time               = SHARED_ROPES.internDatatype(XSD_DT+"time>");
    public static final FinalSegmentRope DT_date               = SHARED_ROPES.internDatatype(XSD_DT+"date>");
    public static final FinalSegmentRope DT_gYearMonth         = SHARED_ROPES.internDatatype(XSD_DT+"gYearMonth>");
    public static final FinalSegmentRope DT_gYear              = SHARED_ROPES.internDatatype(XSD_DT+"gYear>");
    public static final FinalSegmentRope DT_gMonthDay          = SHARED_ROPES.internDatatype(XSD_DT+"gMonthDay>");
    public static final FinalSegmentRope DT_gDay               = SHARED_ROPES.internDatatype(XSD_DT+"gDay>");
    public static final FinalSegmentRope DT_gMonth             = SHARED_ROPES.internDatatype(XSD_DT+"gMonth>");
    public static final FinalSegmentRope DT_BOOLEAN            = SHARED_ROPES.internDatatype(XSD_DT+"boolean>");
    public static final FinalSegmentRope DT_base64Binary       = SHARED_ROPES.internDatatype(XSD_DT+"base64Binary>");
    public static final FinalSegmentRope DT_hexBinary          = SHARED_ROPES.internDatatype(XSD_DT+"hexBinary>");
    public static final FinalSegmentRope DT_FLOAT              = SHARED_ROPES.internDatatype(XSD_DT+"float>");
    public static final FinalSegmentRope DT_decimal            = SHARED_ROPES.internDatatype(XSD_DT+"decimal>");
    public static final FinalSegmentRope DT_DOUBLE             = SHARED_ROPES.internDatatype(XSD_DT+"double>");
    public static final FinalSegmentRope DT_anyURI             = SHARED_ROPES.internDatatype(XSD_DT+"anyURI>");
    public static final FinalSegmentRope DT_string             = SHARED_ROPES.internDatatype(XSD_DT+"string>");
    public static final FinalSegmentRope DT_integer            = SHARED_ROPES.internDatatype(XSD_DT+"integer>");
    public static final FinalSegmentRope DT_nonPositiveInteger = SHARED_ROPES.internDatatype(XSD_DT+"nonPositiveInteger>");
    public static final FinalSegmentRope DT_LONG               = SHARED_ROPES.internDatatype(XSD_DT+"long>");
    public static final FinalSegmentRope DT_nonNegativeInteger = SHARED_ROPES.internDatatype(XSD_DT+"nonNegativeInteger>");
    public static final FinalSegmentRope DT_negativeInteger    = SHARED_ROPES.internDatatype(XSD_DT+"negativeInteger>");
    public static final FinalSegmentRope DT_INT                = SHARED_ROPES.internDatatype(XSD_DT+"int>");
    public static final FinalSegmentRope DT_unsignedLong       = SHARED_ROPES.internDatatype(XSD_DT+"unsignedLong>");
    public static final FinalSegmentRope DT_positiveInteger    = SHARED_ROPES.internDatatype(XSD_DT+"positiveInteger>");
    public static final FinalSegmentRope DT_SHORT              = SHARED_ROPES.internDatatype(XSD_DT+"short>");
    public static final FinalSegmentRope DT_unsignedInt        = SHARED_ROPES.internDatatype(XSD_DT+"unsignedInt>");
    public static final FinalSegmentRope DT_BYTE               = SHARED_ROPES.internDatatype(XSD_DT+"byte>");
    public static final FinalSegmentRope DT_unsignedShort      = SHARED_ROPES.internDatatype(XSD_DT+"unsignedShort>");
    public static final FinalSegmentRope DT_unsignedByte       = SHARED_ROPES.internDatatype(XSD_DT+"unsignedByte>");
    public static final FinalSegmentRope DT_normalizedString   = SHARED_ROPES.internDatatype(XSD_DT+"normalizedString>");
    public static final FinalSegmentRope DT_token              = SHARED_ROPES.internDatatype(XSD_DT+"token>");
    public static final FinalSegmentRope DT_language           = SHARED_ROPES.internDatatype(XSD_DT+"language>");
    public static final FinalSegmentRope DT_langString         = SHARED_ROPES.internDatatype(RDF_DT+"langString>");
    public static final FinalSegmentRope DT_HTML               = SHARED_ROPES.internDatatype(RDF_DT+"HTML>");
    public static final FinalSegmentRope DT_XMLLiteral         = SHARED_ROPES.internDatatype(RDF_DT+"XMLLiteral>");
    public static final FinalSegmentRope DT_JSON               = SHARED_ROPES.internDatatype(RDF_DT+"JSON>");
    public static final FinalSegmentRope DT_PlainLiteral       = SHARED_ROPES.internDatatype(RDF_DT+"PlainLiteral>");

    static {
        /*  --- --- --- top 48 unique most popular on http://prefix.cc/ */
        SHARED_ROPES.internPrefix("<http://www.loc.gov/mads/rdf/v1#");
        SHARED_ROPES.internPrefix("<http://id.loc.gov/ontologies/bflc/");
        SHARED_ROPES.internPrefix("<http://xmlns.com/foaf/0.1/");
        SHARED_ROPES.internPrefix("<http://www.w3.org/2000/01/rdf-schema#");
        SHARED_ROPES.internPrefix("<http://yago-knowledge.org/resource/");
        SHARED_ROPES.internPrefix("<http://dbpedia.org/ontology/");
        SHARED_ROPES.internPrefix("<http://dbpedia.org/property/");
        SHARED_ROPES.internPrefix("<http://purl.org/dc/elements/1.1/");
        SHARED_ROPES.internPrefix("<http://example.org/");
        SHARED_ROPES.internPrefix("<http://www.w3.org/2002/07/owl#");
        SHARED_ROPES.internPrefix("<http://purl.org/goodrelations/v1#");
        SHARED_ROPES.internPrefix("<http://www.w3.org/2004/02/skos/core#");
        SHARED_ROPES.internPrefix("<http://data.ordnancesurvey.co.uk/ontology/spatialrelations/");
        SHARED_ROPES.internPrefix("<http://www.opengis.net/ont/geosparql#");
        SHARED_ROPES.internPrefix("<http://www.w3.org/ns/dcat#");
        SHARED_ROPES.internPrefix("<http://schema.org/");
        SHARED_ROPES.internPrefix("<http://www.w3.org/ns/org#");
        SHARED_ROPES.internPrefix("<http://purl.org/dc/terms/");
        SHARED_ROPES.internPrefix("<http://purl.org/linked-data/cube#");
        SHARED_ROPES.internPrefix("<http://id.loc.gov/ontologies/bibframe/");
        SHARED_ROPES.internPrefix("<http://www.w3.org/ns/prov#");
        SHARED_ROPES.internPrefix("<http://sindice.com/vocab/search#");
        SHARED_ROPES.internPrefix("<http://rdfs.org/sioc/ns#");
        SHARED_ROPES.internPrefix("<http://purl.org/xtypes/");
        SHARED_ROPES.internPrefix("<http://www.w3.org/ns/sparql-service-description#");
        SHARED_ROPES.internPrefix("<http://purl.org/net/ns/ontology-annot#");
        SHARED_ROPES.internPrefix("<http://rdfs.org/ns/void#");
        SHARED_ROPES.internPrefix("<http://purl.org/vocab/frbr/core#");
        SHARED_ROPES.internPrefix("<http://www.w3.org/ns/posix/stat#");
        SHARED_ROPES.internPrefix("<http://www.ontotext.com/");
        SHARED_ROPES.internPrefix("<http://www.w3.org/2006/vcard/ns#");
        SHARED_ROPES.internPrefix("<http://search.yahoo.com/searchmonkey/commerce/");
        SHARED_ROPES.internPrefix("<http://semanticscience.org/resource/");
        SHARED_ROPES.internPrefix("<http://purl.org/rss/1.0/");
        SHARED_ROPES.internPrefix("<http://purl.org/ontology/bibo/");
        SHARED_ROPES.internPrefix("<http://www.w3.org/ns/people#");
        SHARED_ROPES.internPrefix("<http://purl.obolibrary.org/obo/");
        SHARED_ROPES.internPrefix("<http://www.geonames.org/ontology#");
        SHARED_ROPES.internPrefix("<http://www.productontology.org/id/");
        SHARED_ROPES.internPrefix("<http://purl.org/NET/c4dm/event.owl#");
        SHARED_ROPES.internPrefix("<http://rdf.freebase.com/ns/");
        SHARED_ROPES.internPrefix("<http://www.wikidata.org/entity/");
        SHARED_ROPES.internPrefix("<http://purl.org/dc/dcmitype/");
        SHARED_ROPES.internPrefix("<http://purl.org/openorg/");
        SHARED_ROPES.internPrefix("<http://creativecommons.org/ns#");
        SHARED_ROPES.internPrefix("<http://purl.org/rss/1.0/modules/content/");
        SHARED_ROPES.internPrefix("<http://purl.org/gen/0.1#");
        SHARED_ROPES.internPrefix("<http://usefulinc.com/ns/doap#");

        SHARED_ROPES.freeze();
        if (SHARED_ROPES.writeableIdx >= 4)
            throw new AssertionError("Too many collision among built-ins");
    }


    /* --- --- --- instance methods --- --- --- */

    private final FinalSegmentRope[] buckets;
    private final int bucketMask;
    private final int[] hashes;
    private int writeableIdx;

    public SharedRopes(int buckets) {
        buckets = 1 << (32-Integer.numberOfLeadingZeros(buckets-1));
        this.bucketMask = buckets-1;
        this.buckets = new FinalSegmentRope[buckets<<BUCKET_BITS];
        this.hashes = new int[buckets<<BUCKET_BITS];
    }

    /**
     * Calls {@link SharedRopes#SharedRopes(int)} with a bucket count that will lead to at most
     * {@code bytes} of memory being used with buckets and {@link SegmentRope} objects, without
     * including the actual UTF-8 bytes
     *
     * @param bytes storage capacity in bytes, not including the UTF-8 data
     * @return a new {@link SharedRopes} instance.
     */
    public static SharedRopes withBytesCapacity(int bytes) {
        return new SharedRopes(bytes
                             / (4  + /* SegmentRope reference */
                                32 + /* SegmentRope object */
                                40 + /* MemorySegment object */
                                16   /* byte[] header */
                             ) / BUCKET_SIZE);
    }

    void freeze() {
        int max = -1;
        for (int begin = 0; begin < buckets.length; begin += BUCKET_SIZE) {
            for (int j = begin, e = begin+BUCKET_SIZE; j < e; j++) {
                if (buckets[j] == null) {
                    max = Math.max(max, j - 1-begin);
                    break;
                }
            }
        }
        writeableIdx = max+1;
        for (int begin = 0; begin < buckets.length; begin += BUCKET_SIZE) {
            for (int i = begin, e = begin+writeableIdx; i < e; i++) {
                if (buckets[i] == null) buckets[i] = FinalSegmentRope.EMPTY;
            }
        }
    }

    /**
     * Get a {@link SegmentRope} with the bytes of {@code r.sub(begin, end)}.
     *
     * <p>Instead of allocating a new object, this method will search for an already existing
     * {@link SegmentRope} with same bytes (even if not orignating from {@code r}). If a new
     * {@link SegmentRope} is created, the bytes in the {@code begin, end)} range are copied to
     * ensure the new {@link SegmentRope} remains valid even if {@code r} is mutated.</p>
     *
     * @param r a rope with a substring to be interned
     * @param begin index of the first byte of the string to be interned
     * @param end {@code r.len} or index of the first byte after the last byte of the substring
     * @return a {@link SegmentRope} with a copy of substring {@code r.sub(begin, end)}.
     * @throws IllegalArgumentException if {@code end-begin < MIN_INTERNED_LEN}
     */
    private FinalSegmentRope intern(PlainRope r, int begin, int end, int skipBegin) {
        int len = end - begin;
        if (len < MIN_INTERNED_LEN)
            throw new IllegalArgumentException("interned len < MIN_INTERNED_LEN");
        int h = r.fastHash(begin+skipBegin, end-SKIP_INTERNED_END);
        int bucketBegin = (h&bucketMask) << BUCKET_BITS;
        int i = bucketBegin, bucketEnd = bucketBegin + BUCKET_SIZE;
        for (; i < bucketEnd; ++i) {
            int oldHash = (int) HASHES.getAcquire(hashes, i);
            var old = buckets[i];
            if (old == null) break;
            if (oldHash == h && old.len == len && old.has(0, r, begin, end)) return old;
        }
        if (i == bucketEnd) i = bucketBegin+writeableIdx;
        var interned = FinalSegmentRope.asFinal(r, begin, end);
        buckets[i] = interned;
        HASHES.setRelease(hashes, i, h);
        return interned;
    }

    /** Statically bound variant of {@link #intern(PlainRope, int, int, int)}. */
    private FinalSegmentRope intern(SegmentRope r, int begin, int end, int skipBegin) {
        int len = end - begin;
        if (len < MIN_INTERNED_LEN)
            throw new IllegalArgumentException("interned len < MIN_INTERNED_LEN");
        int h = r.fastHash(begin+skipBegin, end-SKIP_INTERNED_END);
        int bucketBegin = (h&bucketMask) << BUCKET_BITS;
        int i = bucketBegin, bucketEnd = bucketBegin + BUCKET_SIZE;
        for (; i < bucketEnd; ++i) {
            int oldHash = (int) HASHES.getAcquire(hashes, i);
            var old = buckets[i];
            if (old == null) break;
            if (oldHash == h && old.len == len && old.has(0, r, begin, end)) return old;
        }
        if (i == bucketEnd) i = bucketBegin+writeableIdx;
        var interned = FinalSegmentRope.asFinal(r, begin, end);
        buckets[i] = interned;
        HASHES.setRelease(hashes, i, h);
        return interned;
    }

    /** Equivalent to {@link #intern(PlainRope, int, int, int)} with {@code skipBegin =}
     *  {@link #SKIP_INTERNED_DTYPE_BEGIN} */
    public FinalSegmentRope internDatatype(PlainRope r, int begin, int end) {
        return intern(r, begin, end, SKIP_INTERNED_DTYPE_BEGIN);
    }

    /** Statically-bound variant of {@link #internDatatype(PlainRope, int, int)} */
    public FinalSegmentRope internDatatype(SegmentRope r, int begin, int end) {
        int len = end - begin;
        if (len > 38 && r.get(begin+36) == '#') {
            //"^^<http://www.w3.org/2001/XMLSchema#...>
            //0123                       27        37
            byte c1 = r.get(begin+38);
            FinalSegmentRope cand = switch(r.get(begin+37)) {
                case 'a' -> DT_anyURI;
                case 'b' -> switch (c1) {
                    case 'a' -> DT_base64Binary;
                    case 'o' -> DT_BOOLEAN;
                    case 'y' -> DT_BYTE;
                    default -> null;
                };
                case 'd' -> switch (c1) {
                    case 'a' -> len > 37+5 /*date>*/ ? DT_dateTime : DT_date;
                    case 'e' -> DT_decimal;
                    case 'o' -> DT_DOUBLE;
                    case 'u' -> DT_duration;
                    default -> null;
                };
                case 'f' -> DT_FLOAT;
                case 'g' -> switch (c1) {
                    case 'd' -> DT_gDay;
                    case 'm' -> len > 37+7 /*gMonth>*/ ? DT_gMonthDay : DT_gMonth;
                    case 'y' -> len > 37+6 /*gYear>*/ ? DT_gYearMonth : DT_gYear;
                    default -> null;
                };
                case 'h' -> DT_hexBinary;
                case 'i' -> len > 37+4 /*int>*/ ? DT_integer : DT_INT;
                case 'l' -> switch (c1) {
                    case 'a' -> DT_language;
                    case 'o' -> DT_LONG;
                    default -> null;
                };
                case 'n' -> switch (c1) {
                    case 'e' -> DT_negativeInteger;
                    case 'o' -> switch (len > 37+3 ? r.get(begin+37+3) : 0) {
                        case 'N' -> DT_nonNegativeInteger;
                        case 'P' -> DT_nonPositiveInteger;
                        default -> null;
                    };
                    default -> null;
                };
                case 'p' -> DT_positiveInteger;
                case 's' -> switch (c1) {
                    case 'h' -> DT_SHORT;
                    case 't' -> DT_string;
                    default -> null;
                };
                case 't' -> switch (c1) {
                    case 'i' -> DT_time;
                    case 'o' -> DT_token;
                    default -> null;
                };
                case 'u' -> switch (len > 37+8 ? r.get(begin+37+8) : 0) {
                    case 'B' -> DT_unsignedByte;
                    case 'I' -> DT_unsignedInt;
                    case 'L' -> DT_unsignedLong;
                    case 'S' -> DT_unsignedShort;
                    default -> null;
                };
                default -> null;
            };
            if (cand != null && cand.len == len && cand.has(0, r, begin, end))
                return cand;
        }
        return intern(r, begin, end, SKIP_INTERNED_DTYPE_BEGIN);
    }
    /** Internal/test use only */
    public FinalSegmentRope  internDatatype(String s) {
        var r = FinalSegmentRope.asFinal(s);
        return internDatatype(r, 0, r.len);
    }
    /** Equivalent to {@link #intern(PlainRope, int, int, int)} with {@code skipBegin =}
     *  {@link #SKIP_INTERNED_IRI_BEGIN} */
    public FinalSegmentRope internPrefix(PlainRope r, int begin, int end) {
        return intern(r, begin, end, SKIP_INTERNED_IRI_BEGIN);
    }
    /** Statically-bound variant of {@link #internPrefix(PlainRope, int, int)} */
    public FinalSegmentRope internPrefix(SegmentRope r, int begin, int end) {
        return intern(r, begin, end, SKIP_INTERNED_IRI_BEGIN);
    }
    /** Internal/test use only */
    public FinalSegmentRope internPrefix(String s) {
        SegmentRope r = FinalSegmentRope.asFinal(s);
        return internPrefix(r, 0, r.len);
    }

    /** Statically-bound version of {@link #internDatatypeOf(PlainRope, int, int)} to help JIT */
    public FinalSegmentRope internDatatypeOf(SegmentRope r, int begin, int end) {
        int endLex = r.skipUntilLast(begin, end, (byte)'"');
        if (end-endLex < MIN_INTERNED_LEN) return FinalSegmentRope.EMPTY;
        return intern(r, endLex, end, SKIP_INTERNED_DTYPE_BEGIN);
    }

    /**
     * Tries to intern datatype suffixes  (e.g., {@code "^^<http://...>}.
     *
     * @param r a literal in N-Triple syntax (quoted with a single {@code "}.
     * @return {@link FinalSegmentRope#EMPTY} if there is no datatype suffix, else a
     *         {@link SegmentRope} with the datatype suffix segments copied to a safe location.
     * */
    public FinalSegmentRope internDatatypeOf(PlainRope r, int begin, int end) {
        int endLex = r.skipUntilLast(begin, end, (byte)'"');
        if (end-endLex < MIN_INTERNED_LEN) return FinalSegmentRope.EMPTY;
        return intern(r, endLex, end, SKIP_INTERNED_DTYPE_BEGIN);
    }

    /** Statically bound version of {@link #internPrefixOf(PlainRope, int, int)} to help the JIT. */
    public FinalSegmentRope internPrefixOf(SegmentRope r, int begin, int end) {
        int i = 1+r.skipUntilLast(begin, end, (byte)'/', (byte)'#');
        if (i > end || i-begin < MIN_INTERNED_LEN)
            return FinalSegmentRope.EMPTY;
        return intern(r, begin, i, SKIP_INTERNED_IRI_BEGIN);
    }

    /**
     * Tries to intern an IRI prefix of the N-Triple iri stored at {@code r.sub(begin, end)}.
     *
     * @param r A rope containing an IRI.
     * @param begin the index of the first by of the IRI (the '<') inside {@code r}
     * @param end {@code r.len} or the index of the first byte after the IRI in {@code r}
     * @return {@link FinalSegmentRope#EMPTY} if there no long enough IRI prefix to be interned, else a
     *         {@link SegmentRope} that contains a copy of a prefix (including the opening '<')
     *         of {@code r}.
     */
    public FinalSegmentRope internPrefixOf(PlainRope r, int begin, int end) {
        int i = 1+r.skipUntilLast(begin, end, (byte)'/', (byte)'#');
        if (i > end || i-begin < MIN_INTERNED_LEN)
            return FinalSegmentRope.EMPTY;
        return intern(r, begin, i, SKIP_INTERNED_IRI_BEGIN);
    }
}
