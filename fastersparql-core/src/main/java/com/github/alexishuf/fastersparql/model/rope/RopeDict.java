package com.github.alexishuf.fastersparql.model.rope;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.invoke.VarHandle;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.concurrent.locks.LockSupport;

import static java.lang.Integer.numberOfTrailingZeros;
import static java.lang.System.arraycopy;
import static java.lang.invoke.MethodHandles.lookup;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.copyOf;

@SuppressWarnings("unused")
public class RopeDict {
    private static final class Blocker {}

    private static final Blocker BLOCKER = new Blocker();

    /** Size must be positive (MSB==0) and also drop one bit do avoid OOMError  */
    private static final int MAX_SIZE = 0x3fffffff;
    /** Ideally we want 16 ids per bucket  */
    private static final int GOAL_BUCKET_LEN_BITS = 4;
    /** Maximum value for bucketBits,*/
    private static final int MAX_BUCKET_BITS = 32 - 2/*see MAX_SIZE*/ - GOAL_BUCKET_LEN_BITS;
    /** Any string below this length cannot be interned  */
    private static final int MIN_INTERNABLE = MAX_BUCKET_BITS+1;
    private static final int MAX_BUCKETS = MAX_SIZE>>4;
    private static final int INIT_SIZE_BITS = 16;
    private static final int INIT_BUCKET_BITS = INIT_SIZE_BITS-GOAL_BUCKET_LEN_BITS;
    /** Do not hash the {@code <http://www.} prefix common to nearly all IRIs. */
    private static final int IRI_SKIP = 12;
    /** Do not hash the {@code "^^<http://www.} prefix common to nearly all datatype suffixes. */
    private static final int DT_SKIP = 15;
    /** long with bit 63 set, signaling {@link RopeDict#find(Rope, int, int, int, int[][])} found the string. */
    private static final long FOUND_MASK = 0x8000000000000000L;
    private static final int FIND_OFFSET_BIT = 37;
    private static final int FIND_OFFSET_MASK = 0x3ffffff;
    private static final int FIND_BITS_BIT = 32;
    private static final int FIND_BITS_MASK = 0x1f;
    private static final byte[] EMPTY = new byte[0];
    private static final byte[] DT_MID = "\"^^<".getBytes(UTF_8);

    /** Number of spins on spinlock before parking. */
    private static final int SPINS = 128;
    private static final VarHandle LOCK, WAITERS_LOCK;
    @SuppressWarnings("FieldMayBeFinal") // accessed through LOCK, LOCK_WAITERS_LOCK
    private static int plainLock = 0, plainLockWaitersLock = 0;
    private static int nextId = 1;
    private static final ArrayDeque<Thread> lockWaitersQueue = new ArrayDeque<>();
    private static ByteRope[] strings = new ByteRope[1<<INIT_SIZE_BITS];
    private static int[] hashes = new int[1<<INIT_SIZE_BITS];
    private static int[][] buckets = new int[1<<INIT_BUCKET_BITS][];

    static {
        try {
            LOCK = lookup().findStaticVarHandle(RopeDict.class, "plainLock", int.class);
            WAITERS_LOCK = lookup().findStaticVarHandle(RopeDict.class, "plainLockWaitersLock", int.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private static final class InsertStats {
        /** How many intern calls were made. Only few such calls should lead to an insertion */
        int interns;
        /** Among {@link InsertStats#interns}, how many calls caused an insertion. */
        int insertions;
        /** How an IRI will be split into shared and local parts */
        SplitStrategy splitStrategy = SplitStrategy.LAST;

        enum SplitStrategy {
            FORCE_LAST,
            LAST,
            PENULTIMATE,
            FIRST,
            NONE;

            SplitStrategy next() {
                return switch (this) {
                    case FORCE_LAST -> FORCE_LAST;
                    case LAST -> PENULTIMATE;
                    case PENULTIMATE -> FIRST;
                    case FIRST, NONE -> NONE;
                };
            }

        }

        public void reset(SplitStrategy strategy) {
            interns = insertions = 0;
            splitStrategy = strategy;
        }

        public int split(Rope r, int begin, int end) {
            ++interns;
            int local = switch (splitStrategy) {
                case FORCE_LAST, LAST -> 1 + r.skipUntilLast(begin, end, '/', '#');
                case PENULTIMATE -> 1 + r.reverseSkipUntil(begin, r.skipUntilLast(begin, end, '/', '#'), '/');
                case FIRST -> 1 + r.skipUntil(begin + MAX_BUCKET_BITS, end, '/');
                case NONE -> begin;
            };
            // if we could not find a '/' or '#' to split, local will be end+1. return as begin
            // to signal that the whole string is local (there is no prefix)
            return local >= end ? begin : local;
        }

        public void inserted() {
            ++insertions;
            if (splitStrategy != SplitStrategy.NONE && interns > 63
                                                    && insertions > interns-(interns>>4)) {
                interns = insertions = 0;
                splitStrategy = splitStrategy.next();
            }
        }
    }
    private static final int INSERT_STAT_BITS = 8;
    private static final InsertStats[] stats = new InsertStats[1<<INSERT_STAT_BITS];

    static {
        for (int i = 0; i < buckets.length; i++)
            buckets[i] = new int[1<<GOAL_BUCKET_LEN_BITS];
        for (int i = 0; i < stats.length; i++)
            stats[i] = new InsertStats();
        disableFloodProtection();
    }

    public static final int P_XSD = (int)internIri("<http://www.w3.org/2001/XMLSchema#>");
    public static final int P_RDF = (int)internIri("<http://www.w3.org/1999/02/22-rdf-syntax-ns#>");

    private static final String XSD_DT = "\"^^<http://www.w3.org/2001/XMLSchema#";
    private static final String RDF_DT = "\"^^<http://www.w3.org/1999/02/22-rdf-syntax-ns#";

    public static final int DT_duration           = (int)internLit(XSD_DT+"duration>");
    public static final int DT_dateTime           = (int)internLit(XSD_DT+"dateTime>");
    public static final int DT_time               = (int)internLit(XSD_DT+"time>");
    public static final int DT_date               = (int)internLit(XSD_DT+"date>");
    public static final int DT_gYearMonth         = (int)internLit(XSD_DT+"gYearMonth>");
    public static final int DT_gYear              = (int)internLit(XSD_DT+"gYear>");
    public static final int DT_gMonthDay          = (int)internLit(XSD_DT+"gMonthDay>");
    public static final int DT_gDay               = (int)internLit(XSD_DT+"gDay>");
    public static final int DT_gMonth             = (int)internLit(XSD_DT+"gMonth>");
    public static final int DT_BOOLEAN            = (int)internLit(XSD_DT+"boolean>");
    public static final int DT_base64Binary       = (int)internLit(XSD_DT+"base64Binary>");
    public static final int DT_hexBinary          = (int)internLit(XSD_DT+"hexBinary>");
    public static final int DT_FLOAT              = (int)internLit(XSD_DT+"float>");
    public static final int DT_decimal            = (int)internLit(XSD_DT+"decimal>");
    public static final int DT_DOUBLE             = (int)internLit(XSD_DT+"double>");
    public static final int DT_anyURI             = (int)internLit(XSD_DT+"anyURI>");
    public static final int DT_string             = (int)internLit(XSD_DT+"string>");
    public static final int DT_integer            = (int)internLit(XSD_DT+"integer>");
    public static final int DT_nonPositiveInteger = (int)internLit(XSD_DT+"nonPositiveInteger>");
    public static final int DT_LONG               = (int)internLit(XSD_DT+"long>");
    public static final int DT_nonNegativeInteger = (int)internLit(XSD_DT+"nonNegativeInteger>");
    public static final int DT_negativeInteger    = (int)internLit(XSD_DT+"negativeInteger>");
    public static final int DT_INT                = (int)internLit(XSD_DT+"int>");
    public static final int DT_unsignedLong       = (int)internLit(XSD_DT+"unsignedLong>");
    public static final int DT_positiveInteger    = (int)internLit(XSD_DT+"positiveInteger>");
    public static final int DT_SHORT              = (int)internLit(XSD_DT+"short>");
    public static final int DT_unsignedInt        = (int)internLit(XSD_DT+"unsignedInt>");
    public static final int DT_BYTE               = (int)internLit(XSD_DT+"byte>");
    public static final int DT_unsignedShort      = (int)internLit(XSD_DT+"unsignedShort>");
    public static final int DT_unsignedByte       = (int)internLit(XSD_DT+"unsignedByte>");
    public static final int DT_normalizedString   = (int)internLit(XSD_DT+"normalizedString>");
    public static final int DT_token              = (int)internLit(XSD_DT+"token>");
    public static final int DT_language           = (int)internLit(XSD_DT+"language>");
    public static final int DT_langString         = (int)internLit(RDF_DT+"langString>");
    public static final int DT_HTML               = (int)internLit(RDF_DT+"HTML>");
    public static final int DT_XMLLiteral         = (int)internLit(RDF_DT+"XMLLiteral>");
    public static final int DT_JSON               = (int)internLit(RDF_DT+"JSON>");
    public static final int DT_PlainLiteral       = (int)internLit(RDF_DT+"PlainLiteral>");

    static final int BUILTIN_END; // exposed for tests

    static {
        /*  --- --- --- top 48 unique most popular on http://prefix.cc/ */
        internIri("<http://www.loc.gov/mads/rdf/v1#>");
        internIri("<http://id.loc.gov/ontologies/bflc/>");
        internIri("<http://xmlns.com/foaf/0.1/>");
        internIri("<http://www.w3.org/2000/01/rdf-schema#>");
        internIri("<http://yago-knowledge.org/resource/>");
        internIri("<http://dbpedia.org/ontology/>");
        internIri("<http://dbpedia.org/property/>");
        internIri("<http://purl.org/dc/elements/1.1/>");
        internIri("<http://example.org/>");
        internIri("<http://www.w3.org/2002/07/owl#>");
        internIri("<http://purl.org/goodrelations/v1#>");
        internIri("<http://www.w3.org/2004/02/skos/core#>");
        internIri("<http://data.ordnancesurvey.co.uk/ontology/spatialrelations/>");
        internIri("<http://www.opengis.net/ont/geosparql#>");
        internIri("<http://www.w3.org/ns/dcat#>");
        internIri("<http://schema.org/>");
        internIri("<http://www.w3.org/ns/org#>");
        internIri("<http://purl.org/dc/terms/>");
        internIri("<http://purl.org/linked-data/cube#>");
        internIri("<http://id.loc.gov/ontologies/bibframe/>");
        internIri("<http://www.w3.org/ns/prov#>");
        internIri("<http://sindice.com/vocab/search#>");
        internIri("<http://rdfs.org/sioc/ns#>");
        internIri("<http://purl.org/xtypes/>");
        internIri("<http://www.w3.org/ns/sparql-service-description#>");
        internIri("<http://purl.org/net/ns/ontology-annot#>");
        internIri("<http://rdfs.org/ns/void#>");
        internIri("<http://purl.org/vocab/frbr/core#>");
        internIri("<http://www.w3.org/ns/posix/stat#>");
        internIri("<http://www.ontotext.com/>");
        internIri("<http://www.w3.org/2006/vcard/ns#>");
        internIri("<http://search.yahoo.com/searchmonkey/commerce/>");
        internIri("<http://semanticscience.org/resource/>");
        internIri("<http://purl.org/rss/1.0/>");
        internIri("<http://purl.org/ontology/bibo/>");
        internIri("<http://www.w3.org/ns/people#>");
        internIri("<http://purl.obolibrary.org/obo/>");
        internIri("<http://www.geonames.org/ontology#>");
        internIri("<http://www.productontology.org/id/>");
        internIri("<http://purl.org/NET/c4dm/event.owl#>");
        internIri("<http://rdf.freebase.com/ns/>");
        internIri("<http://www.wikidata.org/entity/>");
        internIri("<http://purl.org/dc/dcmitype/>");
        internIri("<http://purl.org/openorg/>");
        internIri("<http://creativecommons.org/ns#>");
        internIri("<http://purl.org/rss/1.0/modules/content/>");
        internIri("<http://purl.org/gen/0.1#>");
        internIri("<http://usefulinc.com/ns/doap#>");

        resetFloodProtection(); // re-enables flood protection
        BUILTIN_END = nextId;
        // compute hash for builtin strings
        for (int i = 1; i < BUILTIN_END; i++) hash(i);
    }

    /** Used during initialization to reset insertion flood counters. */
    public static void resetFloodProtection() {
        for (InsertStats s : stats) s.reset(InsertStats.SplitStrategy.LAST);
    }

    /** Used during test and initialization to disable flood protection */
    public static void disableFloodProtection() {
        for (InsertStats s : stats) s.reset(InsertStats.SplitStrategy.FORCE_LAST);
    }

    /** Used by test code to exercise grow() */
    static void resetWithCapacity(int bits) {
        if (1<<bits < BUILTIN_END || bits <= GOAL_BUCKET_LEN_BITS || bits  > 30)
            throw new IllegalArgumentException("bits out of range");
        lock();
        try {
            nextId = BUILTIN_END;
            strings = copyOf(strings, 1<<bits);
            hashes = copyOf(hashes, 1<<bits);
            for (int i = BUILTIN_END; i < strings.length; i++) {
                strings[i-1] = null;
                hashes[i-1] = 0;
            }
            rehash(bits-GOAL_BUCKET_LEN_BITS);
            resetFloodProtection();
            System.gc();
        } finally {
            unlock();
        }
    }

    /** Erases the effect of any {@code intern*(...)} call after class initialization. */
    public static void reset() {
        lock();
        try {
            nextId = BUILTIN_END;
            strings = Arrays.copyOf(strings, 1<<INIT_SIZE_BITS);
            hashes = Arrays.copyOf(hashes, 1<<INIT_SIZE_BITS);
            for (int i = BUILTIN_END; i < strings.length; i++) {
                strings[i-1] = null;
                hashes[i-1] = 0;
            }
            rehash(INIT_BUCKET_BITS);
            resetFloodProtection();
            System.gc();
        } finally {
            unlock();
        }
    }

    /**
     * Get the {@link ByteRope} stored for the {@code id} which was assigned by a previous
     * {@code intern*(Rope, int, int)} call.
     *
     * @throws IndexOutOfBoundsException if {@code id <= 0} or if it does not match any id
     *                                   previously allocated
     */
    public static ByteRope get(int id) {
        // Since the id returned by get() reached this thread, there is a happens-before
        // chain from here to the get(). If id is garbage, we may return garbage, but
        // then it is not our fault.
        return strings[id-1]; // will "handle" id < 0
    }

    /** Equivalent to {@code id == 0 ? ByteRope.EMPTY : get(id&0x7fffffff)}. */
    public static ByteRope getTolerant(int id) {
        return id == 0 ? ByteRope.EMPTY : strings[(id&0x7fffffff)-1];
    }

    /**
     * Equivalent to {@link #internIri(SegmentRope, int, int)} with {@code 0} and {@code rope.len} .
     */
    public static long internIri(SegmentRope rope) {
        return internIri(rope, 0, rope.len);
    }

    /**
     * Find a split point {@code l >= begin} and {@code < end} of {@code rope} and return both
     * {@code l} and an {@code id} such that {@code RopeDict.get(id).equals(rope.sub(begin, l)}.
     *
     * @param rope A rope containing an IRI that <strong>is wrapped</strong> in {@code <>}.
     * @param begin index of the first byte of the IRI in {@code rope}
     * @param end {@code rope.len()} or index of the first byte after the IRI in {@code rope}
     * @return {@code ((long)l << 32 | id)} where {@code l} is the start of the local part and
     *         {@code id}, if non-zero, {@code RopeDict.get(id).equals(rope.sub(begin, l)}.
     */
    public static long internIri(SegmentRope rope, int begin, int end) {
        if (end-begin < MIN_INTERNABLE)
            return (long)begin << 32; // too short for interning, thus everything is "local"
        var s = stats[rope.lsbHash(begin + IRI_SKIP, begin + IRI_SKIP + 8)];
        int local = s.split(rope, begin, end);
        if (local-begin < MIN_INTERNABLE)
            return (long)begin << 32; //can't be interned: prefix too short, thus everything is "local"
        long insertion = find(rope, begin, local, IRI_SKIP, buckets);
        if ((insertion & FOUND_MASK) == 0) { // not found, must insert
            s.inserted();
            insertion = store(new ByteRope(rope.toArray(begin, local)), insertion, IRI_SKIP);
        }
        return ((long)local << 32) | (int)insertion;
    }
    /** Test/internal use ONLY */
    private static long internIri(String string) {
        var r = SegmentRope.of(string);
        return internIri(r, 0, r.len());
    }

    /**
     * Get an {@code id} to use as a substitute for {@code rope.sub(begin, end)}.
     *
     * @param rope a {@link Rope} containing a {@code "\"^^<...>"} segment
     * @param begin index of the first byte of the {@code "^^<...>} segment in {@code rope}
     * @param end {@code rope.len()} or non-inclusive end index of the {@code "^^<...>} segment
     * @return An {@code id} such that {@code RopeDict.get(id).equals(rope.sub(begin, end))} or
     *         {@code 0} if the {@code "^^<...>} segment was too short or if the dictionary
     *         was full.
     */
    public static int internDatatype(SegmentRope rope, int begin, int end) {
        if (end-begin < MIN_INTERNABLE)
            return 0;
        long insertion = find(rope, begin, end, DT_SKIP, buckets);
        if ((insertion&FOUND_MASK) == 0)
            insertion = store(new ByteRope(rope.toArray(begin, end)), insertion, DT_SKIP);
        return (int)insertion;
    }

    public static int internDatatype(SegmentRope rope) {
        return internDatatype(rope, 0, rope.len);
    }

    /** Test/internal use ONLY */
    private static int internDatatype(String string) {
        var r = SegmentRope.of(string);
        return internDatatype(r, 0, r.len());
    }

    /***
     * Split a N-Triples literal at an un-escaped {@code "\"^^<"} and return the index {@code i} of
     * {@code "\"^^<"} of the split and an {@code id} such that
     * {@code RopDict.get(id).equals(rope.sub(i, end)}.
     *
     * @param rope a {@link Rope} containing a N-Triples representation of a literal.
     * @param begin index of the first byte of the literal.
     * @param end {@code rope.len()} or index of the first byte AFTER the literal.
     * @return either {@code (long)end << 32} if no interning is possible (too short, no
     *         suffix or dictionary full) or {@code (endLex << 32) | id}, where {@code endLex}
     *         is the index of {@code "\"^^<"} and id is such that
     *         {@code RopeDict.get(id).equals(rope.sub(endLex, end))}.
     */
    public static long internLit(SegmentRope rope, int begin, int end) {
        if (end-begin < MIN_INTERNABLE) // whole literal is too short
            return (long) end << 32;
        int endLex = rope.skipUntilUnescaped(begin, end, DT_MID);
        if (end - endLex < MIN_INTERNABLE) // datatype suffix is too short
             return (long) end << 32;
        long insertion = find(rope, endLex, end, DT_SKIP, buckets);
        if ((insertion&FOUND_MASK) == 0) // not found
            insertion = store(new ByteRope(rope.toArray(endLex, end)), insertion, DT_SKIP);
        return (int) insertion == 0 ? (long) end << 32 //cannot intern
                                    : ((long) endLex << 32) | (int) insertion; //
    }

    public static long internLit(SegmentRope rope) {
        return internLit(rope, 0, rope.len);
    }

    /** Test use ONLY */
    private static long internLit(String lit) {
        var r = SegmentRope.of(lit);
        return internLit(r, 0, r.len());
    }

    /***
     * Either finds the id for {@code Rope.of(prepend, r.sub(begin, end))} or the bucket
     * where a new id for it shall be inserted.
     *
     * <p>The returned value of this function is a long combining a flag with up to two
     * values:</p>
     *
     * <pre>
     *        6         5         4         3         2         1            bit/10
     *     3210987654321098765432109876543210987654321098765432109876543210  bit%10
     *     0[      bucketOffset      ][bb ][            bucket            ]
     *     1                               [              id              ]
     * </pre>
     *
     * <p>Where:</p>
     * <ul>
     *     <li>Bit 63 is set iff the string was found. In that case, bits {@code 0, 32)}
     *         contain the already assigned id</li>
     *     <li>Else:
     *          <ul>
     *              <li>Bits {@code [0, 31} contain the bucket index</li>
     *              <li>Bits {@code [32, 37)} contain {@code bucketBits = numberOfTrailingZeros(buckets.length)}.</li>
     *              <li>Bits {@code [37, 63)} contain the first index into the bucket that was
     *                  set to zero.</li>
     *          </ul>
     *     </li>
     * </ul>
     *
     * @param r Non-empty string to search a sub-string of (optionally prefixed with {@code prepend}.
     * @param begin index of the first byte of {@code r} to include in the searched string
     * @param end {@code r.len()} or the index of the first of {@code r} to not include.
     * @param leftHashSkip This number of leading bytes in {@code Rope.of(prepend, r.sub(begin, end)}
     *                     will not be used when hashing.
     * @return a {@code long} where if the MSB is set, contains the id for
     *         {@code Rope.of(prepend, r.sub(begin, end))} in bits {@code [0, 31)}, else the
     *         bucket number in bits {@code [0, 31)} and the observed {@code bucketBits} in bits
     *         {@code [32, 37)}.
     */
    private static long find(Rope r, int begin, int end, int leftHashSkip, int[][]buckets) {
        int observedBits = numberOfTrailingZeros(buckets.length);
        int quarter = observedBits>>2, notQuarter = observedBits-quarter;
        int bucket = (r.lsbHash(begin+leftHashSkip, begin+leftHashSkip+quarter) << notQuarter)
                   |  r.lsbHash(end-1-notQuarter,   end-1);
        int[] b = buckets[bucket];
        int offset = 0;
        while (offset < b.length) {
            int id = b[offset];
            if (id == 0)
                break;
            var stored = strings[id-1];
            if (stored == null)
                stored = syncString(id-1);
            if (stored.has(0, r, begin, end))
                return FOUND_MASK | id; // found: return id
            ++offset;
        }
        // not found: return offset, observedBits and bucket
        return (((long)offset & FIND_OFFSET_MASK) << FIND_OFFSET_BIT)
             |  ((long)observedBits << FIND_BITS_BIT)
             |    bucket;
    }

    /** Blocks the caller thread until a store to {@code strings[index]} becomes visible. */
    private static ByteRope syncString(int index) {
        lock();
        unlock();
        return strings[index];
    }

    /**
     * Stores the given {@code string} and return the assigned id.
     *
     * @param string the string to be stored
     * @param insertion previous result of {@link RopeDict#find(Rope, int, int, int, int[][])}
     * @param leftHashSkip number of leading bytes in {@code string} that must not be used for
     *                     hashing. TRhis value will be forwarded to
     *                     {@link RopeDict#find(Rope, int, int, int, int[][])} if {@code insertion} needs
     *                     to be updated due to a race with another inserting thread
     * @return the id assigned for {@code string} by this thread or by a thread that added
     *         {@code string} before this thread acquired the lock.
     */
    private static int store(ByteRope string, long insertion, int leftHashSkip) {
        int[] biggerBucket = null;
        while (true) {
            if (((insertion >> FIND_BITS_BIT) & FIND_BITS_MASK) != numberOfTrailingZeros(buckets.length)) {// if rehashed
                insertion = find(string, 0, string.len, leftHashSkip, buckets); // recompute insertion
                if ((insertion & FOUND_MASK) != 0) // string added by another thread
                    return (int) insertion; // return already assigned id
            } // else: string not found and bucket number is valid
            int bOffset = (int) (insertion >> FIND_OFFSET_BIT) & FIND_OFFSET_MASK;
            int[] b = buckets[(int) insertion];
            if (bOffset >= b.length && (biggerBucket == null || bOffset >= biggerBucket.length))
                biggerBucket = new int[b.length + (b.length>>1)];
            lock();
            try {
                if (((insertion >> FIND_BITS_BIT) & FIND_BITS_MASK) != numberOfTrailingZeros(buckets.length))
                    continue; //another thread re-hashed, insertion is invalid
                if (nextId == MAX_SIZE) // no space and cannot grow
                    return 0; // no space left
                if (nextId == strings.length) { //must grow
                    grow();
                } else { // bucket number is valid, and we can issue a new id.
                    b = buckets[(int) insertion];
                    if (bOffset < b.length && b[bOffset] != 0) { // new strings added to the bucket
                        bOffset = scan(string, b, bOffset); // update index of first 0
                        if (bOffset < 0) return bOffset&0x7fffffff; // string found, abort
                    }
                    if (bOffset >= b.length) // bucket is too small
                        b = swapBucket((int)insertion, biggerBucket);
                    int id = nextId++;      // acquire id
                    strings[id-1] = string; // store string
                    b[bOffset] = id;        // remember id <-> string association
                    return id;
                }
            } finally {
                unlock();
            }
        }
    }

    /**
     * Scan for {@code string} in {@code bucket} starting from {@code offset}.
     *
     * @param string string being probed for insertion.
     * @param bucket bucket where to search for {@code string}.
     * @param offset first index which is known to not contain an id for {@code string}.
     * @return index of first zero in {@code bucket} or {@code id|0x80000000} if found an
     *         {@code id} such that {@code strings[id-1].equals(string)}
     */
    private static int scan(ByteRope string, int[] bucket, int offset) {
        if (bucket.length-offset > SPINS)
            LOCK.setRelease(2); // park() now instead of spinning then parking
        for (; offset < bucket.length; ++offset) {
            int id = bucket[offset];
            if      (id == 0)                      return offset;
            else if (strings[id-1].equals(string)) return id | 0x80000000;
        }
        return bucket.length;
    }

    /**
     * Replaces {@code buckets[bucket]} with a bucket 50% bigger. If possible uses
     * {@code biggerBucket} to avoid performing an allocation.
     *
     * @param bucket the index of the bucket to be replaced.
     * @param biggerBucket If non-null and the length is enough, will use this instead of
     *                     a {@code new int[]}.
     * @return the new bucket at {@code buckets[bucket]}.
     */
    private static int[] swapBucket(int bucket, int @Nullable [] biggerBucket) {
        LOCK.setRelease(2);
        int[] old = buckets[bucket];
        int required = old.length + (old.length >> 1);
        if (biggerBucket == null || biggerBucket.length < required)
            biggerBucket = new int[required];
        arraycopy(old, 0, biggerBucket, 0, old.length);
        return buckets[bucket] = biggerBucket;
    }

    private static void lock() {
        for (int i = 0, saw; (saw = (int)LOCK.compareAndExchangeAcquire(0, 1)) != 0; ++i) {
            Thread.onSpinWait();
            if (saw == 2 || (i & SPINS) != 0) {
                Thread me = Thread.currentThread();
                while (!WAITERS_LOCK.weakCompareAndSetAcquire(0, 1)) Thread.onSpinWait();
                lockWaitersQueue.addLast(me);
                WAITERS_LOCK.setRelease(0);
                LockSupport.park(BLOCKER);
            }
        }
    }

    private static void unlock() {
        LOCK.setRelease(0);
        while (!WAITERS_LOCK.weakCompareAndSetAcquire(0, 1)) Thread.onSpinWait();
        LockSupport.unpark(lockWaitersQueue.pollFirst());
        WAITERS_LOCK.setRelease(0);
    }

    private static void grow() {
        if (strings.length >= MAX_SIZE)
            throw new IllegalStateException("Cannot grow past MAX_SIZE");
        LOCK.setRelease(2); // make spinning threads park()
        //grow arrays
        strings = copyOf(RopeDict.strings, RopeDict.strings.length<<1);
        hashes = copyOf(hashes, hashes.length<<1);
        int currentBits = numberOfTrailingZeros(buckets.length);
        if (currentBits < MAX_BUCKET_BITS)
            rehash(currentBits+1);
    }

    private static void rehash(int bucketBits) {
        var buckets = new int[1<<bucketBits][];
        for (int i = 0; i < buckets.length; i++)
            buckets[i] = new int[1<<GOAL_BUCKET_LEN_BITS];
        for (int id = 1; id < nextId; id++) {
            ByteRope string = strings[id - 1];
            int skip = string.get(0) == '<' ? IRI_SKIP
                     : string.has(0, DT_MID) ? DT_SKIP : 0;
            long ins = find(string, 0, string.len, skip, buckets);
            if ((ins&FOUND_MASK) != 0) {
                assert false : "duplicate string";
                continue;
            }
            int offset = (int) (ins >>> FIND_OFFSET_BIT) & FIND_OFFSET_MASK;
            int[] b = buckets[(int) ins];
            if (offset >= b.length)
                buckets[(int)ins] = b = Arrays.copyOf(b, b.length + (b.length>>1));
            b[offset] = id;
        }
        RopeDict.buckets = buckets;
    }

    /** Memoized {@code ByteRope.hash(get(i), 0, get(i).length)}. */
    public static int hash(int id) {
        if (id < 1 || id > hashes.length) throw new IndexOutOfBoundsException(id);
        --id;
        int h = hashes[id];
        if (h == 0) {
            // no need to spinlock, worst case we compute the same hash more than once
            byte[] utf8 = strings[id].u8();
            h = hashes[id] = RopeSupport.hash(utf8, 0, utf8.length);
        }
        return h;
    }
}
