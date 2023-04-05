package com.github.alexishuf.fastersparql.hdt.batch;

import com.github.alexishuf.fastersparql.sparql.expr.Term;
import com.github.alexishuf.fastersparql.util.BS;
import org.rdfhdt.hdt.dictionary.Dictionary;
import org.rdfhdt.hdt.dictionary.DictionarySection;
import org.rdfhdt.hdt.enums.TripleComponentRole;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.VarHandle;

import static java.lang.invoke.MethodHandles.lookup;
import static org.rdfhdt.hdt.enums.TripleComponentRole.*;

public class IdAccess {
    private static final Logger log = LoggerFactory.getLogger(IdAccess.class);

    /* --- --- --- constants --- --- --- */
    static final long  ROLE_MASK = 0xc000000000000000L;
    static final long  DICT_MASK = 0x3ff0000000000000L;
    static final long PLAIN_MASK = 0x000fffffffffffffL;

    static final int  ROLE_BIT = Long.numberOfTrailingZeros(ROLE_MASK);
    static final int  DICT_BIT = Long.numberOfTrailingZeros(DICT_MASK);
    static final int PLAIN_BIT = Long.numberOfTrailingZeros(PLAIN_MASK);

    public static final int MAX_DICT = (int)(DICT_MASK >> DICT_BIT) + 1;

    public static final long NOT_FOUND = ~ROLE_MASK;

    /* --- --- --- state --- --- --- */

    private static final VarHandle LOCK;
    static  {
        try {
            LOCK = lookup().findStaticVarHandle(IdAccess.class, "plainLock", int.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }
    private static int plainLock;
    private static final Dictionary[] dicts = new Dictionary[MAX_DICT];
    private static final int[] freeDictSlots = BS.init(null, MAX_DICT);
    private static int nextDictId = 1;

    /* --- --- --- inner classes --- --- --- */

    public static final class NoDictException extends IllegalStateException {
        public NoDictException(String msg) { super(msg); }
        public NoDictException(long sourcedId) {
            super("Dictionary "+dictId(sourcedId)+" not found for sourcedId="+sourcedId+". Likely already release()d.");
        }
    }

    public static final class NoStringException extends IllegalArgumentException {
        public NoStringException(long sourcedId) {
            super("Id "+sourcedId+" not found in dictionary. Possible causes: dictionary mutation or release()d dictionary id reused by register()");
        }
    }

    public static final class PlainIdException extends IllegalArgumentException {
        public PlainIdException(long id) {
            super("id="+id+" was not produced by IdAccess.encode()");
        }
    }

    public static final class NoSpaceForDictException extends IllegalStateException {
        public NoSpaceForDictException() {
            super("No space left for registering a new Dictionary");
        }
    }

    /* --- --- --- id lookup methods --- --- --- */

    /**
     * Get a plain (without dictId and role bits) that corresponds to {@code term}
     *
     * <p>If {@code term} is null or a var, will return 0. If term is not found in the given role,
     * {@link #NOT_FOUND} will be returned.</p>
     *
     * @param dictionary {@link Dictionary} where a lookup for {@code term} will be made
     * @param term A {@link Term} to lookup, can be a var or null
     * @param role the position in a  triple where {@code term} must appear.
     * @return {@code 0} if {@code term} is null or a var, -1 if it was not found or an
     *         id that can be used with {@link Dictionary#idToString(long, TripleComponentRole)}.
     */
    public static long plain(Dictionary dictionary, CharSequence term, TripleComponentRole role) {
        if (term == null || term.isEmpty()) return 0;
        char f = term.charAt(0);
        if (f == '?' || f == '$') return 0;
        long id = dictionary.stringToId(toHdtString(term), role);
        return id <= 0 ? -1 : id;
    }

    /* --- --- --- id decoding methods --- --- --- */

    /**
     * Get the {@code dictId} embedded in {@code sourcedId}.
     *
     * @param sourcedId an HDT id augmented with {@link #encode(long, int, TripleComponentRole)}
     * @return {@code dictId} used in {@link #encode(long, int, TripleComponentRole)}.
     *
     * @throws PlainIdException if {@code sourcedId} was not created with {@link #encode(long, int, TripleComponentRole)}
     */
    public static int dictId(long sourcedId) {
        int d = (int)((sourcedId&DICT_MASK) >>> DICT_BIT);
        if (d == 0) throw new PlainIdException(sourcedId);
        return d;
    }

    /**
     * Get the {@link Dictionary} referred to by {@code sourcedId}
     *
     * @param sourcedId an HDT id augmented with {@link #encode(long, int, TripleComponentRole)}
     * @return the {@link Dictionary} that corresponds to the {@code dictId} given in {@link #encode(long, int, TripleComponentRole)}
     * @throws PlainIdException if {@code sourcedId}  was not created by {@link #encode(long, int, TripleComponentRole)}
     * @throws NoDictException if the {@link #release(int)} has been called for the referred dictionary
     */
    public static Dictionary dictOf(long sourcedId) {
        Dictionary d = dicts[dictId(sourcedId)-1];
        if (d == null)
            throw new NoDictException(sourcedId);
        return d;
    }

    /**
     * Get the {@link TripleComponentRole} embedded in {@code sourcedId}.
     *
     * @param sourcedId an HDT id augmented with {@link #encode(long, int, TripleComponentRole)}
     * @return {@code role} used in {@link #encode(long, int, TripleComponentRole)}.
     * @throws PlainIdException if {@code sourcedId} was not created by {@link #encode(long, int, TripleComponentRole)}
     */
    public static TripleComponentRole role(long sourcedId) {
        return switch ((int) (sourcedId >>> ROLE_BIT)) {
            case 1 -> SUBJECT;
            case 2 -> PREDICATE;
            case 3 -> OBJECT;
            default -> throw new PlainIdException(sourcedId);
        };
    }

    /**
     * Get the HDT id embedded in {@code sourcedId}.
     *
     * @param sourcedId An HDT id augmented with {@link #encode(long, int, TripleComponentRole)}
     * @return the {@code id} given to {@link #encode(long, int, TripleComponentRole)}.
     */
    public static long plain(long sourcedId) { return sourcedId &  PLAIN_MASK; }

    /**
     * Get an id for a term of given {@code role} in {@code dict} that has the same string as
     * {@code sourcedId} in its original dictionary.
     *
     * @param dict Where a string equals to {@code sourcedId} will be looked up.
     *             This may be distinct from the dictionary that originated {@code sourcedId}.
     * @param role The role at which a string equals to {@code sourcedId} should occur
     * @param sourcedId an ID annotated with a role and dictId that need not correspond to the
     *                  {@code dict} and {@code role} arguments.
     * @return -1 If {@code sourcedId} does not appear in {@code dict} at the given {@code role},
     *         0 iff {@code plain(sourcedId} == 0} else an id {@code > 0} such that
     *         {@code dict.idToString(id, role).equals(toString(sourcedId))}
     */
    public static long plainIn(Dictionary dict, TripleComponentRole role, long sourcedId) {
        if (sourcedId == 0 || sourcedId == NOT_FOUND) return sourcedId;
        if (dict == dictOf(sourcedId)) { //maybe we can avoid materializing a String
            var oRole = role(sourcedId);
            long plain = plain(sourcedId);
            if (oRole == role)
                return plain;
            if (oRole != PREDICATE && role != PREDICATE)
                return plain <= dict.getNshared() ? plain : -1;
        }
        return plain(dict, toString(sourcedId), role);
    }

    @SuppressWarnings("unused")
    public static String describe(long sourcedId) {
        return "id="+plain(sourcedId)+", role="+role(sourcedId)
                +", on dict["+dictId(sourcedId)+"]="+toString(sourcedId);
    }


    /**
     * Get a {@link Term} instance for the RDF term represented by {@code sourcedId}.
     *
     * @param sourcedId An HDT id annotated with an {@code dictId} and a {@code role}
     * @return 0 if sourcedId is zero, else a {@link Term} built from
     *         {@link Dictionary#idToString(long, TripleComponentRole)}.
     * @throws IllegalArgumentException if {@code sourcedId == } {@link #NOT_FOUND}
     * @throws PlainIdException if {@code sourcedId} has no {@code role} and {@code dictId} set.
     * @throws NoDictException if {@code sourcedId} has a {@code dictId} that is invalid or was
     *                          {@link #release(int)}d
     */
    public static Term toTerm(long sourcedId) {
        var str = toString(sourcedId);
        if (str == null) return null;
        char f = str.charAt(0);
        return f == '"' || f == '_' ? Term.valueOf(str) : Term.iri(str);
    }

    /** Equivalent to {@code dict(sourcedId).idToString(plain(sourcedId), role(sourcedId))}. */
    public static CharSequence toString(long sourcedId) {
        if (sourcedId == 0) return null;
        var role = switch ((int)(sourcedId >>> ROLE_BIT)) {
            case 1 -> SUBJECT;
            case 2 -> PREDICATE;
            case 3 -> OBJECT;
            default -> throw new PlainIdException(sourcedId);
        };
        int dictId = (int) ((sourcedId&DICT_MASK) >>> DICT_BIT);
        if (dictId == 0) throw new PlainIdException(sourcedId);
        Dictionary d = dicts[dictId-1];
        if (d == null) throw new NoDictException(sourcedId);
        var string = d.idToString(sourcedId & PLAIN_MASK, role);
        if (string == null)
            throw new NoStringException(sourcedId);
        return string;
    }

    /* --- --- --- id encoding methods --- --- --- */

    /**
     * Tries to obtain a {@code sourcedId} with same string representation in the same
     * dictionary but in another role.
     *
     * <p>This method should always be used before using an id to query HDT triples.
     * Since ids are specific to dictionary sections, the same id con represent an unrelated
     * string in another section of the same {@link Dictionary}. In most cases this function
     * will do the conversion without </p>
     *
     *
     * @param desired role where an id for the same string is to be looked up
     * @param sourcedId An HDT id annotated with a {@code dictId} and role
     * @return {@code sourcedId} if its role already is {@code desired}, {@link #NOT_FOUND}
     *         if the string identified by {@code sourcedId} does not occur in the
     *         {@code desired} role of the same dictionary or a new sourced id, annotated with
     *         {@code desired} and the same {@code dictId}.
     */
    public static long toRole(TripleComponentRole desired, long sourcedId) {
        if (sourcedId == 0 || sourcedId == NOT_FOUND) return sourcedId;
        var current = role(sourcedId);
        if (current == desired) return sourcedId;
        var dict = dictOf(sourcedId);
        long plain = plain(sourcedId);
        if (current != PREDICATE && desired != PREDICATE) {
            if (plain <= dict.getNshared())
                return desired.ordinal()+1L << ROLE_BIT | (sourcedId & ~ROLE_MASK);
            return NOT_FOUND;
        }
        var string = dict.idToString(sourcedId & PLAIN_MASK, current);
        if (string == null)
            throw new NoDictException(sourcedId);
        plain = dict.stringToId(string, desired);
        return plain <= 0 ? NOT_FOUND : encode(plain, dictId(sourcedId), desired);
    }

    /**
     * Equivalent to {@link #encode(int, Dictionary, CharSequence)} using {@code dict(dictId)}.
     */
    public static long encode(int dictId, CharSequence term) {
        return encode(dictId, dict(dictId), term);
    }

    /**
     * Lookup {@code term} in any section of {@code dict} and returns the found HDT
     * id annotated with {@code dictId} and the observed role.
     *
     * @param dictId The id assigned to {@code dict} by a previous {@link #register(Dictionary)} call.
     * @param dict The {@link Dictionary} where {@code term} will be looked up
     * @param term {@code null}, a SPARQL var or a N-Triples term.
     * @return {@code 0} if {@code term} is null, empty or a variable. {@link #NOT_FOUND} if
     *         {@code term} was not found in any section of {@code dict}. Else returns
     *         the {@code dictId} the located ID and the role of such ID bundled in a
     *         {@code long} built by {@link #encode(long, int, TripleComponentRole)}.
     */
    public static long encode(int dictId, Dictionary dict, CharSequence term) {
        if (term == null || term.isEmpty()) return 0;
        char f = term.charAt(0);
        if (f == '?' || f == '$') return 0;
        var role = SUBJECT;
        String string = toHdtString(term);
        long id = dict.getShared().locate(string);
        if (id == 0) {
            if ((id = dict.getSubjects().locate(string)) == 0) {
                role = OBJECT;
                if ((id = dict.getObjects().locate(string)) == 0) {
                    role = PREDICATE;
                    id = dict.getPredicates().locate(string);
                }
            }
            if (role != PREDICATE) id += dict.getNshared();
        }
        return id == 0 ? NOT_FOUND : encode(id, dictId, role);
    }

    /**
     * Lookup {@code term} in any section of {@code dict} and returns the found HDT
     * id annotated with {@code dictId} and the observed role.
     *
     * @param dictId The id assigned to {@code dict} by a previous {@link #register(Dictionary)} call.
     * @param dict The {@link Dictionary} where {@code term} will be looked up
     * @param role The role at which term must appear in the {@link Dictionary}
     * @param term {@code null}, a SPARQL var or a N-Triples term.
     * @return {@code 0} if {@code term} is null, empty or a variable. {@link #NOT_FOUND} if
     *         {@code term} was not found in any section of {@code dict}. Else returns
     *         the {@code dictId} the located ID and the role of such ID bundled in a
     *         {@code long} built by {@link #encode(long, int, TripleComponentRole)}.
     */
    public static long encode(int dictId, Dictionary dict, TripleComponentRole role,
                              CharSequence term) {
        long id = plain(dict, term, role);
        if      (id == 0) return 0;
        else if (id <  0) return NOT_FOUND;
        else              return encode(id, dictId, role);
    }

    /**
     * Convert a {@link CharSequence} (including {@link Term}) to a {@link String} that can be
     * used with {@link Dictionary#stringToId(CharSequence, TripleComponentRole)} and
     * {@link DictionarySection#locate(CharSequence)}.
     *
     * <p>HDT tools store IRIs without surrounding brackets and {@link Dictionary} implementations
     * require either a {@link String} or specific {@link CharSequence} implementations</p>
     */
    private static String toHdtString(CharSequence term) {
        if (term instanceof Term t)
            return t.isIri() ? t.toString(1, t.len-1) : t.toString();
        else if (term.charAt(0) == '<')
            return term.toString().substring(1, term.length()-1);
        else
            return term.toString();
    }

    /**
     * Create a {@code sourcedId} by annotating the given HDT id as originating from the
     * given dictionary and role.
     *
     * @param id An HDT id originating from a section within a {@link Dictionary}.
     * @param dictId value of {@code register(dict)} for the {@code dict} that originated {@code id}
     * @param role the position of id within a triple. This determines the origin sections within
     *             {@code dict}.
     * @return a {@code sourcedId}.
     */
    public static long encode(long id, int dictId, TripleComponentRole role) {
        if ((id & ~PLAIN_MASK) != 0)
            throw new UnsupportedOperationException("Id is too big");
        if (dictId <= 0 || dictId >= MAX_DICT)
            throw new IllegalArgumentException("dictId does not originate from register()");
        return role.ordinal()+1L << ROLE_BIT | (long)dictId << DICT_BIT | id;
    }

    /**
     * Register a {@link Dictionary} for use with {@link #encode(long, int, TripleComponentRole)}
     *
     * <p>The caller is responsible for calling {@link #release(int)} only one time after it
     * is certain that no more ids referring to the dictionary exist.</p>
     *
     * @param dictionary An HDT dictionary.
     * @return An id to use with {@link #encode(long, int, TripleComponentRole)}
     */
    public static int register(Dictionary dictionary) {
        while (!LOCK.weakCompareAndSetAcquire(0, 1)) Thread.onSpinWait();
        try {
            int id = nextDictId++;
            if (id > dicts.length) {
                --nextDictId;
                id = 1 + BS.nextSetOrLen(freeDictSlots, 0);
                if (id > dicts.length)
                    throw new NoSpaceForDictException();
            }
            dicts[id-1] = dictionary;
            log.debug("Registered Dictionary {}", id);
            return id;
        } finally { LOCK.setRelease(0); }
    }

    /**
     * Get the {@link Dictionary} given to the {@link #register(Dictionary)} call that
     * returned {@code dictId}.
     *
     * @param dictId a dictionary id created with {@link #register(Dictionary)}.
     * @return a non-null {@link Dictionary}
     * @throws NoDictException if {@link #release(int)} was called for {@code dictId}
     */
    public static Dictionary dict(int dictId) {
        Dictionary d = dicts[dictId-1];
        if (d == null)
            throw new NoDictException("Dictionary with id "+dictId+" not found");
        return d;
    }


    /**
     * Releases the Dictionary previously bound to {@code dictId} via {@link #register(Dictionary)}.
     *
     * <p>This will not {@link Dictionary#close()}, but will allow that id to be used in a
     * future {@link #register(Dictionary)} call. After this call attempts to access
     * the dictionary via {@link #dictOf(long)} or {@link #dictId(long)} MAY raise exceptions. Note
     * that due to the possibility of id reuse, those calls might silently return incorrect
     * data.</p>
     *
     * @param dictId an id obtained from {@link #register(Dictionary)}
     */
    public static void release(int dictId) {
        while (!LOCK.weakCompareAndSetAcquire(0, 1)) Thread.onSpinWait();
        try{
            log.debug("Releasing dict {}", dictId);
            BS.set(freeDictSlots, dictId);
            dicts[dictId-1] = null;
        } finally { LOCK.setRelease(0); }
    }

}
