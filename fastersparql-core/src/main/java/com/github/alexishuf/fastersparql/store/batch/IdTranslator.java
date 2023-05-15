package com.github.alexishuf.fastersparql.store.batch;

import com.github.alexishuf.fastersparql.store.index.dict.Dict;
import com.github.alexishuf.fastersparql.store.index.dict.LocalityCompositeDict;
import com.github.alexishuf.fastersparql.store.index.dict.LocalityCompositeDict.Lookup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

import static java.lang.Runtime.getRuntime;

public class IdTranslator {

    /* --- --- --- sourced id --- --- --- */

    public static final long DICT_MASK = 0xffc0000000000000L;
    public static final long   ID_MASK = 0x003fffffffffffffL;
    public static final int DICT_BIT = Long.numberOfTrailingZeros(DICT_MASK);
    private static final int DICT_I_MASK = (int)(DICT_MASK >>> DICT_BIT);

    /**
     * Create a sourced id tying an id to a dict.
     * @param id the plain id of {@code dict(dictId)}
     * @param dictId the registered id for the originating {@link Dict}.
     * @return a {@code long} that stores both {@code id} and {@code dictId}
     * @throws LargeIdException if {@code id} is too large to encode
     * @throws BadDictIdException if {@code dictId} is not valid
     */
    public static long source(long id, int dictId) {
        if (id == Dict.NOT_FOUND) return Dict.NOT_FOUND;
        if ((id & ~ID_MASK) != 0) throw new LargeIdException(id);
        if ((dictId & ~DICT_I_MASK) != 0) throw new BadDictIdException(dictId);
        return id | ((long) dictId << DICT_BIT);
    }

    /**
     * Extract the plain Dict id from a sourced id.
     * @param sourcedId a sourced id created with {@link #source(long, int)}
     * @return the {@code id} given to {@code source(id, dictId)}.
     * @throws NotSourcedIdException if {@code sourcedId} has no {@code dictId}
     */
    public static long unsource(long sourcedId) {
        if ((sourcedId & DICT_MASK) == 0) throw new NotSourcedIdException(sourcedId);
        return sourcedId & ID_MASK;
    }

    /**
     * Extract the dictId from the sourced id.
     * @param sourcedId A sourced id created with {@link #source(long, int)}
     * @return the {@code dictId} given to {@code source(id, dictId)} or 0 {@code sourcedId}
     *         is not a sourced id.
     */
    public static int dictId(long sourcedId) {
        return (int) ((sourcedId&DICT_MASK) >>> DICT_BIT);
    }

    public static final class BadDictIdException extends IllegalArgumentException {
        public BadDictIdException(int dictId) {super(dictId+" is not a valid dictId");}
    }
    public static final class LargeIdException extends IllegalArgumentException {
        public LargeIdException(long id) {super(id+" is too large");}
    }
    public static final class NotSourcedIdException extends IllegalArgumentException {
        public NotSourcedIdException(long id) { super(id+" is not a sourcedId (no dictId)"); }
    }

    /* --- --- --- dict list --- --- --- */

    static final int MAX_DICT = DICT_I_MASK;
    private static final LocalityCompositeDict[] plainDicts = new LocalityCompositeDict[MAX_DICT+1];
    private static final VarHandle DICTS = MethodHandles.arrayElementVarHandle(LocalityCompositeDict[].class);
    private static final VarHandle NEXT_DICT_ID;
    static {
        try {
            NEXT_DICT_ID = MethodHandles.lookup().findStaticVarHandle(IdTranslator.class, "plainNextDictId", int.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    @SuppressWarnings("unused") private static int plainNextDictId;

    /**
     * Assigns a {@code dictId} to {@code dict}.
     *
     * <p>The id MUST be later release with {@link #deregister(int, Dict)}</p>
     *
     * @param dict A {@link LocalityCompositeDict} to register
     * @return an {@code dictId} that can be used {@link #source(long, int)} and
     * {@link #dict(int)}
     */
    public static int register(LocalityCompositeDict dict) {
        int dictId = (int) NEXT_DICT_ID.getAndAdd(1);
        if (dictId <= MAX_DICT) {
            if (DICTS.compareAndExchangeAcquire(plainDicts, dictId, null, dict) == null) {
                if (DICTS.compareAndExchange(plainDicts, dictId, null, dict) == null)
                    return dictId;
            }
        } else {
            NEXT_DICT_ID.setRelease(MAX_DICT+1);
        }
        for (dictId = 1; dictId < plainDicts.length; dictId++) {
            if (DICTS.compareAndExchange(plainDicts, dictId, null, dict) == null)
                return dictId;
        }
        throw new NoDictSpaceException();
    }

    /**
     * Get the {@link LocalityCompositeDict} previously assigned to the given {@code dictId}
     * @param dictId the result of a previous {@link #register(LocalityCompositeDict)} call
     * @return the {@code dict} given to a previous {@code register(dict)} call.
     */
    public static LocalityCompositeDict dict(int dictId) {
        return (LocalityCompositeDict) DICTS.getAcquire(plainDicts, dictId);
    }

    public static final class NoDictSpaceException extends IllegalStateException {
        public NoDictSpaceException() {
            super("There is no space left for registering a new Dict");
        }
    }

    /**
     * Signals the given {@link LocalityCompositeDict} previously
     * {@link #register(LocalityCompositeDict)}ed will not be used anymore.
     *
     * <p>This will drop all caches related to the {@code dictId} and further attempts to
     * use</p>
     *
     * @param dictId the value returned by {@code register(dict)}
     * @param dict the dict registered for {@code dictId}
     * @throws IllegalStateException if {@code dictId} does not correspond to
     *                               {@code dict}. This could happen in case {@code release}
     *                               is called twice for the same dict or a bug allows
     *                               two dicts to up with the same id.
     */
    public static void deregister(int dictId, Dict dict) {
        Dict ac = (Dict) DICTS.compareAndExchangeRelease(plainDicts, dictId, dict, null);
        if (ac != dict)
            throw new IllegalStateException("Currently dictId="+dictId+" belongs to "+ac+", not "+dict+" did not remove");
        dropLookups(dictId);
    }

    /* --- --- --- lookup cache --- --- --- */

    private static final int CPU_BITS, CPU_MASK;
    static {
        int cpus = getRuntime().availableProcessors();
        int ceilCpus = 1<<(32-Integer.numberOfLeadingZeros((2*cpus)-1));
        int slots = Math.min(4, ceilCpus);
        assert Integer.bitCount(slots) == 1;
        CPU_MASK = slots-1;
        CPU_BITS = Integer.numberOfTrailingZeros(~CPU_MASK);
        assert 4*slots*(MAX_DICT+1) < 1_000_000_000;
    }
    private static final Lookup[] lookups
            = new Lookup[(MAX_DICT+1) << CPU_BITS];

    /**
     * Get a {@link Lookup} exclusively for the calling thread.
     *
     * <p>The Lookup instance will be kept for this thread until another collides with
     * the slot where the reference is stored or the Dict associated to {@code dictId} is
     * de-registered.</p>
     *
     * @param dictId id of the {@link Dict} that will be used to instantiate the {@link Lookup} object
     * @return A Lookup for the {@link #dict(int)} of {@code dictId}
     * @throws BadDictIdException if the dicId is invalid or if it has been de-registered.
     */
    public static Lookup lookup(int dictId) {
        Thread thread = Thread.currentThread();
        int slot = (dictId << CPU_BITS) | ((int) thread.threadId() & CPU_MASK);
        var l = lookups[slot];
        var dict = plainDicts[dictId];
        //noinspection resource
        if (l != null && l.owner == thread && l.dict() == dict)
            return l;
        if (dict == null)
            throw new BadDictIdException(dictId);
        l = dict.lookup(thread);
        lookups[slot] = l;
        return l;
    }

    /** Remove all Lookup instances for the given dict. */
    private static void dropLookups(int dictId) {
        int begin = dictId << CPU_BITS, end = (dictId+1) << CPU_BITS;
        for (int i = begin; i < end; i++)
            lookups[i] = null;
    }

}
