package com.github.alexishuf.fastersparql.emit.async;

import com.github.alexishuf.fastersparql.emit.Rebindable;
import com.github.alexishuf.fastersparql.emit.exceptions.RebindReleasedException;
import com.github.alexishuf.fastersparql.emit.exceptions.RebindStateException;
import com.github.alexishuf.fastersparql.sparql.binding.BatchBinding;
import com.github.alexishuf.fastersparql.util.concurrent.LongRenderer;
import com.github.alexishuf.fastersparql.util.owned.AbstractOwned;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

import static com.github.alexishuf.fastersparql.util.concurrent.LongRenderer.HEX;
import static com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal.ENABLED;
import static com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal.journal;
import static java.lang.Integer.*;
import static java.lang.Thread.onSpinWait;
import static java.util.Arrays.copyOf;

@SuppressWarnings("PointlessBitwiseExpression")
public abstract class Stateful<S extends Stateful<S>> extends AbstractOwned<S> {
    private static final Logger log = LoggerFactory.getLogger(Stateful.class);
    private static final VarHandle S;
    static {
        try {
            S = MethodHandles.lookup().findVarHandle(Stateful.class, "plainState", int.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }
//    public static final ConcurrentHashMap<Integer, Stateful> INSTANCES = new ConcurrentHashMap<>();

    /* --- --- --- static constants and their static functions --- --- --- */

    /** The producer/task/emitter was created and is not yet producing items */
    public static final int IS_INIT           = 0x00000004;
    /** Items are being produced or production/delivery is temporarily paused. */
    public static final int IS_LIVE           = 0x00000008;
    /** There was a request to cancel production */
    public static final int IS_CANCEL_REQ     = 0x00000010;
    /** The notification of termination to downstream is pending */
    public static final int IS_PENDING_TERM   = 0x00000020;
    /** The notification of termination is being sent */
    public static final int IS_TERM           = 0x00000040;
    /** The notification of termination has been send (downstream handlers already returned). */
    public static final int IS_TERM_DELIVERED = 0x00000080;

    protected static final int GRP_MASK       = 0x000000fc;
    protected static final int STATE_MASK     = -1 >>> numberOfLeadingZeros(GRP_MASK);
    protected static final int GRP_BIT        = numberOfTrailingZeros(GRP_MASK);
    protected static final int SUB_STATE_MASK = -1 >>> -GRP_BIT;
    protected static final int FLAGS_MASK     = ~STATE_MASK;

    /** A non-reentrant lock. See {@link #lock()}/{@link #unlock()} */
    protected static final int LOCKED_MASK         = 0x00000100;
    private   static final int UNLOCKED_MASK       = ~LOCKED_MASK;
    private   static final int UNLOCKED_FLAGS_MASK = FLAGS_MASK&UNLOCKED_MASK;
    /** Marks that {@link #doRelease()} has already been called for this instance. */
    protected static final int RELEASED_MASK        = 0x00000200;
    protected static final int PENDING_RELEASE_MASK = 0x00000400;

    static {
        int flags = LOCKED_MASK | RELEASED_MASK;
        assert bitCount(flags) == 2 : "overlapping bits";
        assert bitCount(flags & STATE_MASK) == 0 : "state and flags overlap";
    }


    public static final int CREATED              = 0x00000000 | IS_INIT;

    public static final int ACTIVE               = 0x00000000 | IS_LIVE;
    public static final int PAUSED               = 0x00000001 | IS_LIVE;

    public static final int CANCEL_REQUESTING    = 0x00000001 | IS_CANCEL_REQ;
    public static final int CANCEL_REQUESTED     = 0x00000002 | IS_CANCEL_REQ;

    private static final int CANCELLED_SUB_STATE = 0x00000000;
    private static final int COMPLETED_SUB_STATE = 0x00000001;
    private static final int    FAILED_SUB_STATE = 0x00000002;

    public static final int PENDING_CANCELLED    = CANCELLED_SUB_STATE | IS_PENDING_TERM;
    public static final int PENDING_COMPLETED    = COMPLETED_SUB_STATE | IS_PENDING_TERM;
    public static final int PENDING_FAILED       =    FAILED_SUB_STATE | IS_PENDING_TERM;

    public static final int CANCELLED            = CANCELLED_SUB_STATE | IS_TERM;
    public static final int COMPLETED            = COMPLETED_SUB_STATE | IS_TERM;
    public static final int FAILED               =    FAILED_SUB_STATE | IS_TERM;

    public static final int CANCELLED_DELIVERED  = CANCELLED_SUB_STATE | IS_TERM | IS_TERM_DELIVERED;
    public static final int COMPLETED_DELIVERED  = COMPLETED_SUB_STATE | IS_TERM | IS_TERM_DELIVERED;
    public static final int FAILED_DELIVERED     =    FAILED_SUB_STATE | IS_TERM | IS_TERM_DELIVERED;


    /**
     * Whether {@code succ} is a valid successor of the state {@code pred}
     *
     * @param pred a predecessor state
     * @param succ a tentative successor state
     * @return whether the transition from {@code pred} to {@code succ} is valid
     */
    public static boolean isSuccessor(int pred, int succ) {
        if (pred == succ) return false;
        int predGrp = (pred&STATE_MASK)>>>GRP_BIT, grpDiff = (succ>>>GRP_BIT) - predGrp;
        return grpDiff > 0 || (grpDiff == 0 && predGrp < IS_TERM);
    }

    public static boolean isCancelled(int state) {
        return (state&IS_TERM) != 0 && (state&SUB_STATE_MASK) == CANCELLED_SUB_STATE;
    }
    public static boolean isCompleted(int state) {
        return (state&IS_TERM) != 0 && (state&SUB_STATE_MASK) == COMPLETED_SUB_STATE;
    }
    @SuppressWarnings("unused") public static boolean isFailed(int state) {
        return (state&IS_TERM) != 0 && (state&SUB_STATE_MASK) == FAILED_SUB_STATE;
    }

    /* --- --- --- instance fields --- --- --- */

    @SuppressWarnings("FieldMayBeFinal") private int plainState;
    protected final Flags flags;

    protected Stateful(int initState, Flags flags) {
        this.plainState = initState;
        this.flags = flags;
//        INSTANCES.put(System.identityHashCode(this), this);
    }

    /* --- --- --- observers --- --- --- */

    /** Read current state&flags with <strong>plain</strong> read semantics. */
    protected int statePlain()  { return plainState; }

    /** Read current state&flags with <strong>opaque</strong> semantics: there will certainly
     * be a read operation but there will be no ordering guarantees. */
    protected int state() { return (int)S.getOpaque(this); }

    /** Read current state&flags with <strong>acquire</strong> semantics: any writes by the
     *  thread that did a <strong>release</strong> write to the state will be visible if they
     *  preceded the release write in program order. */
    @SuppressWarnings("unused") protected int stateAcquire() { return (int)S.getAcquire(this); }

    /** Get a readable {@link String} for the current {@link #state()} */
    @SuppressWarnings("unused")
    public String stateName() { return flags.render(state()); }

    /* --- --- --- release management --- --- --- */

    @Override public @Nullable S recycle(Object currentOwner) {
        internalMarkGarbage(currentOwner);
        for (int i = 0; i < 2; ++i) {
            int st = stateAcquire();
            if ((st&CAN_RELEASE_MASK) == CAN_RELEASE_VALUE) {
                if (compareAndSetFlagRelease(RELEASED_MASK)) {
                    clearFlagsAcquire(PENDING_RELEASE_MASK);
                    doRelease();
                }
                break;
            } else if ((st&FORBID_PENDING_RELEASE_MASK) == 0
                    && compareAndSetFlagRelease(PENDING_RELEASE_MASK)) {
                onPendingRelease();
            } else {
                break;
            }
        }
        return null;
    }
    private static final int CAN_RELEASE_MASK = IS_TERM_DELIVERED|RELEASED_MASK;
    private static final int CAN_RELEASE_VALUE = IS_TERM_DELIVERED;
    private static final int FORBID_PENDING_RELEASE_MASK = RELEASED_MASK|PENDING_RELEASE_MASK|IS_TERM_DELIVERED;

    /* --- --- --- release management --- --- --- */

    /**
     * This method will be called <strong>once</strong> per instance when both conditions
     * are satisfied:
     *
     * <ol>
     *     <li>{@link #recycle(Object)} has been called with the correct owner</li>
     *     <li>The current {@link #state()} has the {@link #IS_TERM_DELIVERED} bit</li>
     * </ol>
     *
     * <p>Given the above conditions, this method will be invoked from within a
     * {@link #recycle(Object)} or {@link #markDelivered(int)} call. Therefore implementations
     * should tolerate being called from any thread. However, given the terminal nature of
     * this method, no state-changing method calls should be happening concurrently once the
     * above conditions are met. Note that {@link Rebindable#rebind(BatchBinding)} will fail on
     * a {@code _DELIVERED} state.</p>
     */
    protected void doRelease() { /* pass */ }

    /**
     * This method will be called at most one time, when {@link #recycle(Object)} is invoked
     * with the correct owner but the current {@link #state()} is not a {@code _DELIVERED} state
     * ({@link #IS_TERM_DELIVERED}).
     *
     * <p>This will be directly invoked from within {@link #recycle(Object)}. Implementations
     * should be prepared for this to be invoked from any thread, even if there will ever be
     * one (and thus non-concurrent) invocation.</p>
     */
    protected void onPendingRelease() { /* pass */ }

    private void tryRelease(int current) {
        if ((current&CAN_TRY_RELEASE) != CAN_TRY_RELEASE)
            return; // not _DELIVERED or no previous recycle()
        int ex = current&~RELEASED_MASK;
        if ((int)S.compareAndExchangeRelease(this, ex, ex|RELEASED_MASK) == ex) {
            if (ENABLED)
                journal("doRelease", this);
            try {
                doRelease();
            } catch (Throwable t) {
                log.error("Ignoring doRelease() failure for {}", this, t);
            }
        }
    }
    private static final int CAN_TRY_RELEASE = IS_TERM_DELIVERED|PENDING_RELEASE_MASK;

    /* --- --- --- counters --- --- --- --- */

    protected int addToCounterRelease(int state, int mask, int shiftedAdd) {
        int ex, next;
        do {
            next = ((ex=state)&mask) + shiftedAdd;
            if ((next&mask) != next) {
                if (ENABLED)
                    journal("overflow/underflow for mask=", mask, HEX, "state=", ex, flags, "this=", this);
                return ex;
            }
            next = (ex&~mask) | next;
        } while ((state=(int)S.compareAndExchangeRelease(this, ex, next)) != ex);
        if (ENABLED) {
            int begin = numberOfTrailingZeros(mask);
            journal("added", shiftedAdd>>>begin, "now=",
                    (next&mask)>>>begin, "to counter=", flags.counterName(mask));
            journal("on", this);
        }
        return next;
    }

    /* --- --- --- setters for state and flags --- --- --- */

    /**
     * Atomically sets current state to {@code next}, without modifying other flags if
     * {@link #isSuccessor(int, int)} reports {@code next} as a successor of {@link #state()}.
     *
     * <p>If the state is locked ({@link #LOCKED_MASK}), this call will spin until the state is
     * is unlocked before attempting to atomically check and set the state to {@code next}.</p>
     *
     * @param current the likely current value of {@link #state()}
     * @param nextState the next state (must not include flags)
     * @return {@code true} iff the state was set to next, {@code false} if next was not a
     *         successor of the current state.
     */
    protected boolean moveStateRelease(int current, int nextState) {
        boolean transitioned = false;
        int n = 0, a, e = current&UNLOCKED_MASK;
        nextState &= STATE_MASK;
        Thread self = null;
        while (isSuccessor(e, nextState)) {
            n = (e&FLAGS_MASK)|nextState;
            if ((a=(int)S.compareAndExchangeRelease(this, e, n)) == e) {
                transitioned = true;
                break;
            } else if ((a&LOCKED_MASK) != 0) {
                if (self == null)
                    self = Thread.currentThread();
                else
                    EmitterService.yieldWorker(self);
            } else {
                e = a;
            }
        }
        if (ENABLED && transitioned)
            journal("trans", n, flags, "on", this);
        return transitioned;
    }

    /**
     * Atomically sets the state from a {@link #IS_TERM} state to its {@link #IS_TERM_DELIVERED}
     * variant, and if conditions allow, sets {@link #RELEASED_MASK} and calls {@link #doRelease()}.
     *
     * @param termState A un-flagged {@link #IS_TERM} state or the likely current
     *                  {@link #state()} which must be an {@link #IS_TERM} state.
     * @return {@code true} iff this call observed a {@link #IS_TERM} {@link #state()}
     *         and transitioned to a {@link #IS_TERM_DELIVERED} state.
     */
    protected boolean markDelivered(int termState) {
        if ((termState&IS_TERM) == 0) {
            assert false : "termState is not a TERM state";
            return false;
        }
        int ex, upd, tgt = (termState&STATE_MASK)|IS_TERM_DELIVERED;
        termState &= ~IS_TERM_DELIVERED;
        do {
            if ((termState&IS_TERM_DELIVERED) != 0)
                return false; // already delivered
            if ((termState&IS_TERM) == 0)
                return false; // do not transition if rebind() happened
            ex = termState&UNLOCKED_MASK;
            upd = (termState&UNLOCKED_FLAGS_MASK)|tgt;
        } while((termState=(int)S.compareAndExchangeRelease(this, ex, upd)) != ex);
        tryRelease(upd);
        return true;
    }

    /**
     * Equivalent to {@link #markDelivered(int)} on {@code (current&STATE_MASK)
     * | (termState&STATE_MASK)}, which can save an addition read-compute-compare-swap
     * cycle if {@code current} and {@code termState} are correct.
     *
     * @param current the likely current value for {@link #state()}{@code &}{@link #FLAGS_MASK}
     * @param termState the likely current value for {@link #state()}{@code &}{@link #STATE_MASK}
     */
    protected boolean markDelivered(int current, int termState) {
        return markDelivered((current&FLAGS_MASK) | (termState&STATE_MASK));
    }

    protected int resetForRebind(int clearFlags, int setFlags) {
        int st = lock();
        try {
            if ((st&(IS_INIT|IS_TERM)) == 0)
                throw new RebindStateException(this);
            if ((st&RELEASED_MASK) != 0)
                throw new RebindReleasedException(this);
            clearFlags |= STATE_MASK;
            setFlags   |= CREATED;
        } catch (Throwable t) {
            clearFlags = LOCKED_MASK;
            setFlags   = 0;
            throw t;
        } finally {
            int ex = st, ac, clMask = ~(clearFlags|(~setFlags&LOCKED_MASK));
            while ((ac=(int)S.compareAndExchangeRelease(this, ex, st=(ex&clMask)|setFlags)) != ex)
                ex = ac;
            if (ENABLED) {
                if (clearFlags == STATE_MASK && setFlags == (CREATED|LOCKED_MASK)) {
                    journal("resetFR+lock", this);
                } else if (clearFlags == STATE_MASK && setFlags == CREATED) {
                    journal("resetFR", this);
                } else {
                    String op = (setFlags&LOCKED_MASK) == 0 ? "resetFR, cl="
                              : "resetFR+lock, cl=";
                    journal(op, clearFlags, flags, "set=", setFlags, flags, "on", this);
                }
            }
        }
        return st;
    }

    /**
     * Atomically sets the 1-bits of {@code flag} in the current state.
     *
     * @param flags An {@code int} to be {@code |}-ed with the {@link #state()}
     * @return the new value for {@link #state()}
     */
    public int setFlagsRelease(int flags) {
        int witness = (int)S.getAndBitwiseOrRelease(this, flags);
        if (ENABLED) journal("set", flags, this.flags, this);
        return witness|flags;
    }

    /**
     * Atomically clears the 1-bits of {@code flags} in {@link #state()}.
     *
     * @param flags the bits to be cleared, i.e., {@link #state()}{@code &= ~flags}
     * @return the new value for {@link #state()}
     */
    @SuppressWarnings("UnusedReturnValue")
    public int clearFlagsRelease(int flags) {
        int witness = (int)S.getAndBitwiseAndRelease(this, ~flags);
        if (ENABLED) journal("clr", flags, this.flags, this);
        return witness&~flags;
    }

    /**
     * Equivalent to {@link #clearFlagsRelease(int)}, but with acquire semantics on the read
     * of the current value and without release semantic on the write operation.
     */
    @SuppressWarnings("UnusedReturnValue") public int clearFlagsAcquire(int flags) {
        int witness = (int)S.getAndBitwiseAndAcquire(this, ~flags);
        if (ENABLED) journal("clr", flags, this.flags, this);
        return witness&~flags;
    }

    /**
     * Atomically clears all flags in {@code clear} and sets all flags in {@code set} with
     * release semantics: future reads by threads that read the state with acquire semantics
     * will observe writes the caller thread made before this method.
     *
     * @param clear bit mask of flags that will be set to {@code 0}
     * @param set bit mas of flags that will be set to {@code 1}
     * @return the new {@link #state()}.
     */
    public int changeFlagsRelease(int clear, int set) {
        int ex = plainState, ac, n;
        while ((ac=(int)S.compareAndExchangeRelease(this, ex, n=(ex&~clear)|set)) != ex)
            ex = ac;
        return n;
    }

    /**
     * Atomically reads the flag and sets it if unset.
     *
     * @param flag the bit mask to be set
     * @return {@code true} iff the bit was seen unset and this call did set it in {@link #state()}.
     */
    public boolean compareAndSetFlagRelease(int flag) {
        int a;
        if (((a=(int)S.getAndBitwiseOrRelease(this, flag))&flag) == flag)
            return false; // already set
        if (ENABLED)
            journal("CAS", flag, flags, "on", a, flags, "on", this);
        return true;
    }

    /**
     * Equivalent to {@link #compareAndSetFlagRelease(int)}, but instead with acquire memory
     * ordering on the read of the current state.
     *
     * @param flag the bit mask to be set in {@link #state()}
     * @return {@code true} iff all bits in {@code flag} were witnessed as unset and this
     *         call did set them in {@link #state()}
     */
    public boolean compareAndSetFlagAcquire(int flag) {
        int a;
        if (((a=(int)S.getAndBitwiseOrAcquire(this, flag))&flag) == flag)
            return false; // already set
        if (ENABLED)
            journal("CAS", flag, flags, "on", a, flags, "on", this);
        return true;
    }

    /**
     * Similar to {@link #lock()}, but uses {@code flag} instead of {@link #LOCKED_MASK}
     */
    public int lockFlag(int flag) {
        int a;
        if (((a=(int)S.getAndBitwiseOrAcquire(this, flag))&flag) != 0)
            a = lockFlagContended(flag);
        if (ENABLED)
            journal("CAS", flag, flags, "on", a, flags, "on", this);
        return a|flag;
    }

    /**
     * Similar to {@link #unlock()}, but uses {@code flag} instead of {@link #LOCKED_MASK}
     */
    public int unlockFlag(int flag) {
        int st = (int)S.getAndBitwiseAndRelease(this, ~flag);
        if ((st&flag) == 0)
            throw new NotLocked(this);
        return st&~flag;
    }

    private int lockFlagContended(int flag) {
        Thread self = Thread.currentThread();
        int a;
        while (((a=(int)S.getAndBitwiseOrAcquire(this, flag))&flag) != 0)
            EmitterService.yieldWorker(self);
        return a|flag;
    }

    /* --- ---- --- lock/unlock --- --- --- */

    /**
     * Atomically sets the {@link #LOCKED_MASK} bit in {@link #state()} if such bit is unset,
     * else retry. This implements a <strong>non-reentrant</strong> lock.
     *
     * @return the current state, with {@link #LOCKED_MASK} set
     */
    public int lock() {
        for (int i = 0, st; i < 8; i++) {
            if (((st=(int)S.getAndBitwiseOrAcquire(this, LOCKED_MASK))&LOCKED_MASK) == 0)
                return st|LOCKED_MASK;
            onSpinWait();
        }
        return lockContended();
    }


    /**
     * Tries to acquire the lock with absolute minimal effort
     * @return {@code true} if the lock was acquired, requiring a future {@link #unlock()}.
     */
    protected boolean tryLock() {
        return ((int)S.getAndBitwiseOrAcquire(this, LOCKED_MASK)&LOCKED_MASK) == 0;
    }

    protected boolean tryLockFlag(int mask) {
        int ex = plainState&~mask;
        return S.weakCompareAndSetAcquire(this, ex, ex|mask);
    }

    private int lockContended() {
        Thread self = Thread.currentThread();
        int st;
        while (((st=(int)S.getAndBitwiseOrAcquire(this, LOCKED_MASK))&LOCKED_MASK) != 0)
            EmitterService.yieldWorker(self);
        return st|LOCKED_MASK;
    }

    /**
     * Atomically clears the {@link #LOCKED_MASK} bit from {@link #state()}, if set
     *
     * <p><strong>Important:</strong>This must be called <strong>exactly</strong> once per
     * {@link #lock()} call by the same thread that called
     * {@link #lock()}. These constraints are NOT checked at runtime.</p>
     *
     * @return the updated {@link #state()}
     * @throws IllegalStateException If assertions are enabled and the locked bit was not set
     */
    public int unlock() {
        int st = (int)S.getAndBitwiseAndRelease(this, UNLOCKED_MASK);
        if ((st&LOCKED_MASK) == 0)
            throw new NotLocked(this);
        return st&UNLOCKED_MASK;
    }

    /**
     * Atomically clears the {@link #LOCKED_MASK} bit, clears all bits set in {@code clear}
     * and sets any bits set in {@code set}.
     *
     * <p><strong>Important:</strong>This must be called <strong>exactly</strong> once per
     * {@link #lock()} call by the same thread that called
     * {@link #lock()}. These constraints are NOT checked at runtime.</p>
     *
     * @param clear Bitset whose set bits will be cleared in {@code holder}'s {@code int} field
     * @param set Bitset whose set bits will be set in {@code holder}'s {@code int} field
     */
    public int unlock(int clear, int set) {
        if (ENABLED && (clear|set) != 0)
            journal("unlck cl=", clear, flags, "set=", set, flags, "on", this);
        int a, e = plainState, n, mask = ~(clear|LOCKED_MASK);
        while ((a=(int)S.compareAndExchangeRelease(this, e, n=(e&mask)|set)) != e)
            e = a;
        if ((e&LOCKED_MASK) == 0)
            throw new NotLocked(this);
        return n;
    }

    public static final class NotLocked extends IllegalStateException {
        public NotLocked(Stateful<?> s) {
            super("Attempt to unlock not locked "+s.journalName()+", st="+
                  s.flags.render(s.state()));
        }
    }

    /* --- --- --- state flags customization --- --- --- */

    public static final class Flags implements LongRenderer {
        public static final Flags DEFAULT = new Builder()
                .flag(LOCKED_MASK, "LOCKED")
                .flag(RELEASED_MASK, "RELEASED")
                .flag(PENDING_RELEASE_MASK, "PENDING_RELEASED")
                .build();
        private final String[] flagNames;
        private final String[] counterNames;
        private final    int[] counterBegins;
        private final    int[] counterWidths;
        private final    int   used;

        private Flags(String[] flagNames, String[] counterNames,
                      int[] counterBegins, int[] counterWidths, int used) {
            this.flagNames     = flagNames;
            this.counterNames  = counterNames;
            this.counterBegins = counterBegins;
            this.counterWidths = counterWidths;
            this.used = used;
        }

        public Builder toBuilder() {
            var b = new Builder();
            for (int i = 0; i < flagNames.length; i++) {
                if (flagNames[i] != null)
                    b.flag(1 << i, flagNames[i]);
            }
            for (int i = 0; i < counterNames.length; i++) {
                String name = counterNames[i];
                if (name != null)
                    b.counter((-1 >>> -counterWidths[i]) << counterBegins[i], name);
            }
            return b;
        }

        public static final class Builder {
            private int used = STATE_MASK;
            private int counters;
            private final String[] flagNames     = new String[32];
            private final String[] counterNames  = new String[32];
            private final    int[] counterBegins = new    int[32];
            private final    int[] counterWidths = new    int[32];

            private Builder() { }

            public @This Builder flag(int flag, String name) {
                if (Integer.bitCount(flag) != 1)
                    throw new IllegalArgumentException("Flags must have exactly one bit set");
                if ((used & flag) != 0)
                    throw new IllegalArgumentException("The flag "+name+" overlaps");
                used |= flag;
                flagNames[numberOfTrailingZeros(flag)] = name;
                return this;
            }

            public @This Builder counter(int mask, String name) {
                if (mask < 0)
                    throw new IllegalArgumentException("counter mask cannot include MSB");
                counterNames [counters] = name;
                counterBegins[counters] = numberOfTrailingZeros(mask);
                counterWidths[counters] = bitCount(mask);
                if ((used & mask) != 0)
                    throw new IllegalArgumentException("Counter "+name+" overlaps");
                used |= mask;
                if ((mask >>> numberOfTrailingZeros(mask)) != -1 >>> -bitCount(mask))
                    throw new IllegalArgumentException("Counter "+name+" mask has gaps");
                ++counters;
                return this;
            }

            public Flags build() {
                return new Flags(flagNames, copyOf(counterNames, counters),
                                 copyOf(counterBegins, counters),
                                 copyOf(counterWidths, counters), used);
            }
        }

        public boolean contains(Flags other) {
            // check counters
            outer:
            for (int i = 0; i < other.counterNames.length; i++) {
                String name = other.counterNames[i];
                for (int j = 0; j < counterNames.length; j++) {
                    if (counterNames[j].equals(name)) {
                        if (counterBegins[j] != other.counterBegins[i]
                                || counterWidths[j] != other.counterWidths[i]) {
                            return false; // same name different mask
                        }
                        continue outer;
                    }
                }
                return false; // no counter with same name
            }

            // check flags
            for (int i = 0; i < other.flagNames.length; i++) {
                String name = other.flagNames[i];
                if (name != null && !name.equals(flagNames[i]))
                    return false; // missing flag
            }
            return true; // no issues detected
        }

        private String stateName(int state) {
            String name = switch (state&STATE_MASK) {
                case 0                   -> "";
                case CREATED             -> "CREATED";
                case ACTIVE              -> "ACTIVE";
                case PAUSED              -> "PAUSED";
                case CANCEL_REQUESTING   -> "CANCEL_REQUESTING";
                case CANCEL_REQUESTED    -> "CANCEL_REQUESTED";
                case PENDING_CANCELLED   -> "PENDING_CANCELLED";
                case PENDING_COMPLETED   -> "PENDING_COMPLETED";
                case PENDING_FAILED      -> "PENDING_FAILED";
                case CANCELLED           -> "CANCELLED";
                case COMPLETED           -> "COMPLETED";
                case FAILED              -> "FAILED";
                case CANCELLED_DELIVERED -> "CANCELLED_DELIVERED";
                case COMPLETED_DELIVERED -> "COMPLETED_DELIVERED";
                case FAILED_DELIVERED    -> "FAILED_DELIVERED";
                default                  -> {
                    String grp = switch (state&GRP_MASK) {
                        case IS_INIT                   -> "INIT";
                        case IS_LIVE                   -> "LIVE";
                        case IS_TERM                   -> "TERM";
                        case IS_TERM|IS_TERM_DELIVERED -> "TERM_DELIVERED";
                        default -> null;
                    };
                    yield grp == null ? null : grp+"0x"+Integer.toHexString(state&SUB_STATE_MASK);
                }
            };
            if (name == null)
                name = String.format("0x%02x", state);
            return name;
        }

        public String counterName(int mask) {
            int begin = Integer.numberOfTrailingZeros(mask);
            for (int i = 0; i < counterBegins.length; i++) {
                if (counterBegins[i] == begin) {
                    return counterNames[i];
                }
            }
            return "UNKNOWN_COUNTER";
        }

        @Override public String render(long state) { return render((int)state); }

        public String render(int state) {
            String name = stateName(state);
            int flags = state & ~STATE_MASK;
            if (flags == 0)
                return name;

            var sb = new StringBuilder().append(name).append('[');
            for (int i = 0; i < flagNames.length; i++) {
                if (flagNames[i] != null && ((1 << i) & flags) != 0)
                    sb.append(flagNames[i]).append(", ");
            }
            for (int i = 0; i < counterNames.length; i++) {
                int value = (flags >>> counterBegins[i]) & (-1 >>> -counterWidths[i]);
                if (value != 0) {
                    sb.append(counterNames[i]).append('=')
                            .append(value)
                            .append(", ");
                }
            }
            if ((flags & ~used) != 0)
                sb.append(String.format("0x%08x", flags));
            if (sb.charAt(sb.length()-1) == ' ')
                sb.setLength(sb.length()-2);
            return sb.append(']').toString();
        }
    }
}