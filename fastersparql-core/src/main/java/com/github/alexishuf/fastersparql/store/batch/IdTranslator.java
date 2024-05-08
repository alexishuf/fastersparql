package com.github.alexishuf.fastersparql.store.batch;

import com.github.alexishuf.fastersparql.store.index.dict.Dict;
import com.github.alexishuf.fastersparql.store.index.dict.LocalityCompositeDict;
import com.github.alexishuf.fastersparql.store.index.dict.LocalityCompositeDict.Lookup;
import com.github.alexishuf.fastersparql.util.BS;
import com.github.alexishuf.fastersparql.util.owned.StaticMethodOwner;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

import static com.github.alexishuf.fastersparql.store.index.dict.Dict.NOT_FOUND;

public class IdTranslator {

    /* --- --- --- sourced id --- --- --- */

    public static final long DICT_MASK = 0xffc0000000000000L;
    public static final long ID_MASK = 0x003fffffffffffffL;
    public static final int DICT_BIT = Long.numberOfTrailingZeros(DICT_MASK);
    private static final int DICT_I_MASK = (int) (DICT_MASK >>> DICT_BIT);

    /**
     * Create a sourced id tying an id to a dict.
     *
     * @param id     the plain id of {@code dict(dictId)}
     * @param dictId the registered id for the originating {@link Dict}.
     * @return a {@code long} that stores both {@code id} and {@code dictId}
     * @throws LargeIdException   if {@code id} is too large to encode
     * @throws BadDictIdException if {@code dictId} is not valid
     */
    public static long source(long id, int dictId) {
        if (id == NOT_FOUND) return NOT_FOUND;
        if ((id & ~ID_MASK) != 0) throw new LargeIdException(id);
        if ((dictId & ~DICT_I_MASK) != 0) throw new BadDictIdException(dictId);
        return id | ((long) dictId << DICT_BIT);
    }

    /**
     * Extract the plain Dict id from a sourced id.
     *
     * @param sourcedId a sourced id created with {@link #source(long, int)}
     * @return the {@code id} given to {@code source(id, dictId)}.
     * @throws NotSourcedIdException if {@code sourcedId} has no {@code dictId}
     */
    public static long unsource(long sourcedId) {
        if (sourcedId == NOT_FOUND) return NOT_FOUND;
        if ((sourcedId & DICT_MASK) == 0) throw new NotSourcedIdException(sourcedId);
        return sourcedId & ID_MASK;
    }

    /***
     * Get an unsourced id {@code i} that represents the same string in {@code targetDictId}
     * as {@code sourcedId} represents in its embedded dict. If the string is not present
     * in the target dict, or if {@code sourcedId == NOT_FOUND}, {@link Dict#NOT_FOUND}
     * will be returned.
     *
     * @param sourcedId {@link Dict#NOT_FOUND} or an id sourced to a specific dict
     *                  (see {@link #source(long, int)})
     * @param targetDictId The id from {@link #register(LocalityCompositeDict)} for the
     *                     destination dict where an identical string will be looked up.
     * @param targetLookup {@link Lookup} for {@code targetDictId}
     * @return If the target dict does not contain the string or if {@code sourcedId == NOT_FOUND},
     *         {@link Dict#NOT_FOUND}. Else the unsourced id {@code i} such that
     *         {@code lookup(targetDictId).get(i).equals(lookup(dictId).get(unsource(sourcedId)))}.
     * @throws BadDictIdException if {@link #dictId(long)} for {@code sourcedId} is
     *                            invalid or points to a {@link #deregister(int, Dict)}ed dict.
     * @throws NotSourcedIdException if {@code sourcedId != } {@link Dict#NOT_FOUND} and does not
     *                               embed a dict id.
     */
    public static long translate(long sourcedId, int targetDictId, Lookup targetLookup) {
        if (sourcedId == NOT_FOUND) return NOT_FOUND;
        int sourceDictId = (int) ((sourcedId & DICT_MASK) >>> DICT_BIT);
        if (sourceDictId == 0) throw new NotSourcedIdException(sourcedId);
        if (sourceDictId == targetDictId) return sourcedId & ID_MASK;
        var srcLookup = dicts[sourceDictId].lookup().takeOwnership(TRANSLATE);
        try {
            return targetLookup.find(srcLookup.get(sourcedId&ID_MASK));
        } finally {
            srcLookup.recycle(TRANSLATE);
        }
    }
    
    private static final StaticMethodOwner TRANSLATE = new StaticMethodOwner("IdTranslator.translate");

    /**
     * Extract the dictId from the sourced id.
     *
     * @param sourcedId A sourced id created with {@link #source(long, int)}
     * @return the {@code dictId} given to {@code source(id, dictId)} or 0 {@code sourcedId}
     * is not a sourced id.
     */
    public static int dictId(long sourcedId) {
        return (int) ((sourcedId & DICT_MASK) >>> DICT_BIT);
    }

    public static final class BadDictIdException extends IllegalArgumentException {
        public BadDictIdException(int dictId) {
            super(dictId + " is not a valid dictId");
        }
    }

    public static final class LargeIdException extends IllegalArgumentException {
        public LargeIdException(long id) {
            super(id + " is too large");
        }
    }

    public static final class NotSourcedIdException extends IllegalArgumentException {
        public NotSourcedIdException(long id) {
            super(id + " is not a sourcedId (no dictId)");
        }
    }

    /* --- --- --- dict list --- --- --- */

    static final int MAX_DICT = DICT_I_MASK;
    static final int N_DICTS = MAX_DICT + 1;
    private static final LocalityCompositeDict[] dicts = new LocalityCompositeDict[N_DICTS];
    private static final int[] freeDictIds = BS.init(null, N_DICTS);
    @SuppressWarnings("unused") private static int plainDictsLock;
    private static final VarHandle DICTS_LOCK;

    static {
        BS.set(freeDictIds, 1, freeDictIds.length << 5);
        try {
            DICTS_LOCK = MethodHandles.lookup().findStaticVarHandle(IdTranslator.class, "plainDictsLock", int.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }


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
        while ((int) DICTS_LOCK.compareAndExchangeAcquire(0, 1) != 0) Thread.onSpinWait();
        try {
            for (int scan = 0; scan < 2; scan++) {
                for (int i = 1; (i = BS.nextSet(freeDictIds, i)) >= 0; ++i) {
                    if (dicts[i] != null) continue;
                    dicts[i] = dict;
                    BS.clear(freeDictIds, i);
                    return i;
                }
                if (scan == 0) {
                    for (int i = 1; i < dicts.length; i++) {
                        if (dicts[i] == null) BS.set(freeDictIds, i);
                    }
                }
            }
        } finally {
            DICTS_LOCK.setRelease(0);
        }
        throw new NoDictSpaceException();
    }

    /**
     * Get the {@link LocalityCompositeDict} previously assigned to the given {@code dictId}
     *
     * @param dictId the result of a previous {@link #register(LocalityCompositeDict)} call
     * @return the {@code dict} given to a previous {@code register(dict)} call.
     */
    public static LocalityCompositeDict dict(int dictId) {
        return dicts[dictId];
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
     * @param dict   the dict registered for {@code dictId}
     * @throws IllegalStateException if {@code dictId} does not correspond to
     *                               {@code dict}. This could happen in case {@code release}
     *                               is called twice for the same dict or a bug allows
     *                               two dicts to up with the same id.
     */
    public static void deregister(int dictId, Dict dict) {
        var ac = dicts[dictId];
        if (ac != dict)
            throw new IllegalStateException("Currently dictId=" + dictId + " belongs to " + ac + ", not " + dict + " did not remove");
        dicts[dictId] = null;
    }
}
