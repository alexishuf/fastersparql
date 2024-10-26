package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.FS;
import com.github.alexishuf.fastersparql.model.rope.*;
import com.github.alexishuf.fastersparql.sparql.PrefixAssigner;
import com.github.alexishuf.fastersparql.sparql.expr.FinalTerm;
import com.github.alexishuf.fastersparql.sparql.expr.InvalidTermException;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import com.github.alexishuf.fastersparql.sparql.expr.TermView;
import com.github.alexishuf.fastersparql.util.BS;
import com.github.alexishuf.fastersparql.util.concurrent.Bytes;
import com.github.alexishuf.fastersparql.util.concurrent.LIFOPool;
import com.github.alexishuf.fastersparql.util.concurrent.Primer;
import com.github.alexishuf.fastersparql.util.owned.AbstractOwned;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;

import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Arrays;

import static com.github.alexishuf.fastersparql.model.rope.FinalSegmentRope.EMPTY;
import static com.github.alexishuf.fastersparql.model.rope.Rope.FNV_BASIS;
import static com.github.alexishuf.fastersparql.model.rope.SharedRopes.MIN_INTERNED_LEN;
import static com.github.alexishuf.fastersparql.model.rope.SharedRopes.SHARED_ROPES;
import static com.github.alexishuf.fastersparql.sparql.expr.Term.isNumericDatatype;
import static com.github.alexishuf.fastersparql.util.LowLevelHelper.U;
import static java.lang.Thread.onSpinWait;
import static java.lang.foreign.ValueLayout.JAVA_BYTE;

public class ScopedIds {
    /*
     *   +----------+------------+------------+----------------+---------+-------------+
     *   | scope_id | shared_id  | segment_id | segment_off>>2 | is_lit  | segment_len |
     * 64|63      56|55        51|50        39|38            21|20     20|19          0|
     *   |  8 bits  |   5 bits   |   12 bits  |    18 bits     |  1 bit  |    20 bits  |
     *   +----------+------------+------------+----------------+---------+-------------+
     */

    private static final int SCOPE_ID_BITS      =  8;
    private static final int SHARED_ID_BITS     =  5;
    private static final int SEG_ID_BITS        = 12;
    private static final int SEG_OFF_BITS       = 18;
    private static final int SEG_LEN_BITS       = 20;
    private static final int MAX_SCOPE_ID      = (1 <<  SCOPE_ID_BITS)-1;
    private static final int MAX_SHARED_ID     = (1 << SHARED_ID_BITS)-1;
    private static final int MAX_SEG_ID        = (1 <<    SEG_ID_BITS)-1;
    private static final int MAX_SEG_OFF       = (1 <<   SEG_OFF_BITS)-1;
    private static final int MAX_SEG_LEN       = (1 <<   SEG_LEN_BITS)-1;
    private static final int POOLED_SEG_LEN    = MAX_SEG_LEN+1;
    static {//noinspection ConstantValue
        assert POOLED_SEG_LEN != MAX_SEG_LEN : "POOLED_SEG_LEN must be != MAX_SEG_LEN";
    }
    private static final long IS_LIT_MASK      =  1 << 20;
    private static final int MAX_SEG_OFF_MUL   = MAX_SEG_OFF<<2;
    private static final long EMPTY_MASK = 0x00f80000000fffffL;
    private static int     scp_id (long scopedId) { return (int)(scopedId>>>56); }
    private static int     shr_id (long scopedId) { return (int)(scopedId>>>51)&MAX_SHARED_ID; }
    private static boolean is_lit(long scopedId) { return (scopedId&IS_LIT_MASK) != 0; }
    private static int     seg_id (long scopedId) { return (int)(scopedId>>>39)&MAX_SEG_ID; }
    private static int     seg_off(long scopedId) { return (int)(scopedId>>>19)&MAX_SEG_OFF_MUL; }
    private static int     seg_len(long scopedId) { return (int)(scopedId     )&MAX_SEG_LEN; }
    private static long make(int scope_id, int shared_id, int seg_id, int seg_off, int seg_len,
                             boolean is_lit) {
        return seg_len | (is_lit ? IS_LIT_MASK : 0L)
                | ((long) scope_id   <<56)
                | ((long) shared_id  <<51)
                | ((long) seg_id     <<39)
                | ((long)(seg_off>>2)<<21);
        //if (VALIDATE_ON_MAKE) validate(seg_off, id);
    }
//    private static void validate(int seg_off, long scopedId) {
//        if ((seg_off&3) != 0)
//            throw new IllegalArgumentException("Non-aligned seg_off");
//        if ((scopedId&EMPTY_MASK) == 0) {
//            if (scopedId != 0)
//                throw new IllegalArgumentException("Non-standard null ID");
//        } else {
//            var term = asTerm(scopedId); // will validate N-Triples string
//            if (term == null)
//                throw new IllegalArgumentException("non-empty ID yielded null Term");
//            if ((term.type() == Term.Type.LIT) != is_lit(scopedId))
//                throw new IllegalArgumentException("is_lit bit does not match actual term");
//        }
//    }
    static { //self-test
        assert  Integer.bitCount(MAX_SCOPE_ID)+
                Integer.bitCount(MAX_SHARED_ID)+
                Integer.bitCount(MAX_SEG_ID)+
                Integer.bitCount(MAX_SEG_OFF)+
                Integer.bitCount(MAX_SEG_LEN) == 63 : "wasted/overlapping bits";
        assert seg_len(EMPTY_MASK) == MAX_SEG_LEN;
        assert shr_id (EMPTY_MASK) == MAX_SHARED_ID;

        int begin = 0;
        assert  seg_len((long)MAX_SEG_LEN<<begin) == MAX_SEG_LEN;
        assert  seg_len((long)MAX_SEG_LEN<<begin|IS_LIT_MASK) == MAX_SEG_LEN;
        assert  seg_off((long)MAX_SEG_LEN<<begin|IS_LIT_MASK) == 0;
        assert  seg_off((long)MAX_SEG_LEN<<begin) == 0;
        assert   seg_id((long)MAX_SEG_LEN<<begin) == 0;
        assert   shr_id((long)MAX_SEG_LEN<<begin) == 0;
        assert !is_lit((long)MAX_SEG_LEN<<begin);
        assert  is_lit((long)MAX_SEG_LEN<<begin|IS_LIT_MASK);
        assert   seg_id((long)MAX_SEG_LEN<<begin) == 0;
        begin += Integer.bitCount(MAX_SEG_LEN);

        begin++; // is_lit bit

        assert  seg_len((long)MAX_SEG_OFF<<begin) == 0;
        assert  seg_len((long)MAX_SEG_OFF<<begin|IS_LIT_MASK) == 0;
        assert  seg_off((long)MAX_SEG_OFF<<begin|IS_LIT_MASK) == MAX_SEG_OFF*4;
        assert  seg_off((long)MAX_SEG_OFF<<begin) == MAX_SEG_OFF*4;
        assert   seg_id((long)MAX_SEG_OFF<<begin) == 0;
        assert   shr_id((long)MAX_SEG_OFF<<begin) == 0;
        assert !is_lit((long)MAX_SEG_OFF<<begin);
        assert  is_lit((long)MAX_SEG_OFF<<begin|IS_LIT_MASK);
        assert   seg_id((long)MAX_SEG_OFF<<begin) == 0;
        begin += Integer.bitCount(MAX_SEG_OFF);

        assert  seg_len((long)MAX_SEG_ID<<begin) == 0;
        assert  seg_off((long)MAX_SEG_ID<<begin) == 0;
        assert   seg_id((long)MAX_SEG_ID<<begin|IS_LIT_MASK) == MAX_SEG_ID;
        assert   seg_id((long)MAX_SEG_ID<<begin) == MAX_SEG_ID;
        assert   shr_id((long)MAX_SEG_ID<<begin) == 0;
        assert !is_lit((long)MAX_SEG_ID<<begin);
        assert  is_lit((long)MAX_SEG_ID<<begin|IS_LIT_MASK);
        assert   scp_id((long)MAX_SEG_ID<<begin) == 0;
        begin += Integer.bitCount(MAX_SEG_ID);

        assert  seg_len((long)MAX_SHARED_ID<<begin) == 0;
        assert  seg_off((long)MAX_SHARED_ID<<begin) == 0;
        assert   seg_id((long)MAX_SHARED_ID<<begin) == 0;
        assert   shr_id((long)MAX_SCOPE_ID<<begin) == MAX_SHARED_ID;
        assert   shr_id((long)MAX_SCOPE_ID<<begin|IS_LIT_MASK) == MAX_SHARED_ID;
        assert !is_lit((long)MAX_SCOPE_ID<<begin);
        assert  is_lit((long)MAX_SHARED_ID<<begin|IS_LIT_MASK);
        assert   scp_id((long)MAX_SHARED_ID<<begin) == 0;
        begin += Integer.bitCount(MAX_SHARED_ID);

        assert  seg_len((long)MAX_SCOPE_ID<<begin) == 0;
        assert  seg_off((long)MAX_SCOPE_ID<<begin) == 0;
        assert   seg_id((long)MAX_SCOPE_ID<<begin) == 0;
        assert   shr_id((long)MAX_SCOPE_ID<<begin) == 0;
        assert !is_lit((long)MAX_SCOPE_ID<<begin);
        assert  is_lit((long)MAX_SCOPE_ID<<begin|IS_LIT_MASK);
        assert   scp_id((long)MAX_SCOPE_ID<<begin|IS_LIT_MASK) == MAX_SCOPE_ID;
        assert   scp_id((long)MAX_SCOPE_ID<<begin) == MAX_SCOPE_ID;
        assert begin + Integer.bitCount(MAX_SCOPE_ID) == 64;
    }

    public static final class OutOfScopeIds extends IllegalStateException {
        public OutOfScopeIds() {super("There are no more Scope IDs available");}
    }
    public static final class StringTooLarge extends IllegalArgumentException {
        public StringTooLarge() {super("String is too large to fit in a segment"); }
    }
    public static final class ClosedScope extends IllegalStateException {
        public ClosedScope() {super("ID Scope was already closed"); }
    }

    private static final class ScopeIdAllocator {
        @SuppressWarnings("unused") private static int plainLock;
        private static final VarHandle LOCK;
        static {
            try {
                LOCK = MethodHandles.lookup().findStaticVarHandle(ScopeIdAllocator.class, "plainLock", int.class);
            } catch (NoSuchFieldException | IllegalAccessException e) {
                throw new ExceptionInInitializerError(e);
            }
        }
        private static final long[] ACTIVE_IDS = new long[BS.longsFor(MAX_SCOPE_ID+1)];

        public static short acquireId() {
            while ((int)LOCK.compareAndExchangeAcquire(0, 1) != 0) onSpinWait();
            try {
                int free = BS.nextClear(ACTIVE_IDS, 0);
                if (free > MAX_SCOPE_ID)
                    throw new OutOfScopeIds();
                BS.set(ACTIVE_IDS, free& MAX_SCOPE_ID);
                return (short)free;
            } finally { LOCK.setRelease(0); }
        }

        public static void releaseId(int id) {
            if (id == 0) throw new IllegalArgumentException("Cannot release null ID");
            if ((id & ~MAX_SCOPE_ID) != 0)
                throw new IllegalArgumentException("ID overflow");
            while ((int)LOCK.compareAndExchangeAcquire(0, 1) != 0) onSpinWait();
            try {
                BS.clear(ACTIVE_IDS, id& MAX_SCOPE_ID);
            } finally { LOCK.setRelease(0); }
        }
    }

    private static final class Scope0 {
        private static final int KEEP_SEGMENTS_ON_RELEASE = 1;
        private static final int HASH_CACHE_MASK = (1<<10)-1;
        private static final int GEN_INACTIVE_MASK = 0x8000;
        private static final LIFOPool<Bytes> SEG_POOL;
        @SuppressWarnings("unused") private static int plainPooledSegPermits;
        @SuppressWarnings("unused") private static int plainUnpooledSegments;
        private static final VarHandle POOLED_SEG_PERMITS, UNPOOLED_SEGS;
        static {
            // spend at most 5% of max heap with this pool
            int segBytes = Bytes.BYTES + MAX_SEG_LEN;
            int segCap = (int)(Runtime.getRuntime().maxMemory()/20/segBytes);
            SEG_POOL = new LIFOPool<>(Bytes.class, "ScopedIds.SEG_POOL", segCap, segBytes);
            try {
                POOLED_SEG_PERMITS = MethodHandles.lookup().findStaticVarHandle(Scope0.class, "plainPooledSegPermits", int.class);
                UNPOOLED_SEGS      = MethodHandles.lookup().findStaticVarHandle(Scope0.class, "plainUnpooledSegments", int.class);
            } catch (NoSuchFieldException|IllegalAccessException e) {
                throw new ExceptionInInitializerError(e);
            }
            POOLED_SEG_PERMITS.setRelease(segCap);
            FS.addShutdownHook(() -> {
                int permits  = (int)POOLED_SEG_PERMITS.getOpaque();
                int unpooled = (int) UNPOOLED_SEGS.getOpaque();
                if (permits < segCap || unpooled > 0) {
                    System.err.printf("""
                            ScopedIds   pooled segments created: %,d
                            ScopedIds unpooled segments created: %,d
                            """, segCap-Math.max(0, permits), unpooled);
                }
            });
            Primer.INSTANCE.sched(() -> {
                int upperBound = Math.max(8, Runtime.getRuntime().availableProcessors());
                int freeCapacity = segCap-SEG_POOL.sharedObjects();
                int n = Math.min(upperBound, freeCapacity);
                for (int i = 0; i < n; i++) {
                    if ((int)POOLED_SEG_PERMITS.getAndAddRelease(-1) <= 0)
                        break; // no more permits
                    byte[] arr = new byte[POOLED_SEG_LEN];
                    SEG_POOL.offer(Bytes.createUnpooled(arr).takeOwnership(SEG_POOL));
                }
            });
        }
        private final FinalSegmentRope[] shared   = new FinalSegmentRope[MAX_SHARED_ID+1];
        private final  byte[]         [] arrays   = new byte            [MAX_SEG_ID+1][];
        private final  MemorySegment  [] segments = new MemorySegment   [MAX_SEG_ID+1];
        private final byte scopeId;
        private short generation = (short)GEN_INACTIVE_MASK;
        private boolean internAuthority;
        private final long            [] hashCache = new long           [HASH_CACHE_MASK+1];
        private final  Bytes          [] bytes     = new Bytes          [MAX_SEG_ID+1];
        private final  SegmentRopeView[] ropes     = new SegmentRopeView[MAX_SEG_ID+1];

        private Scope0(int scopeId) {
            if ((scopeId&~0xff) != 0)
                throw new IllegalArgumentException("scopeId too big");
            this.scopeId = (byte)scopeId;
            setSegment(0, Bytes.createUnpooled(new byte[MAX_SEG_LEN]).takeOwnership(this));
        }

        short addSegment(int segCount) {
            Bytes b = SEG_POOL.get();
            if (b == null) {
                int size = plainPooledSegPermits > 0
                                && (int)POOLED_SEG_PERMITS.getAndAddRelease(-1) > 0
                         ? POOLED_SEG_LEN : MAX_SEG_LEN;
                b = Bytes.createUnpooled(new byte[size]).takeOwnership(this);
            } else {
                b.transferOwnership(SEG_POOL, this);
            }
            setSegment(segCount, b);
            return (short)segCount;
        }

        private void setSegment(int segCount, Bytes b) {
            var view = new SegmentRopeView();
            view.wrap(b.segment, b.arr, 0, b.arr.length);
            bytes   [segCount] = b;
            arrays  [segCount] = b.arr;
            segments[segCount] = b.segment;
            ropes   [segCount] = view;
        }

        @This Scope0 check() {
            if (generation < 0)
                throw new ClosedScope();
            return this;
        }

        private void storeCachedHash(long scopedId, int hash) {
            int bucket = (int)scopedId;
            bucket = ((bucket ^ (bucket>>>SEG_LEN_BITS))<<1) & HASH_CACHE_MASK;
            hashCache[bucket  ] = (scopedId<<32)                 | (hash&0xffffffffL);
            hashCache[bucket+1] = (scopedId&0xffffffff00000000L) | (hash&0xffffffffL);
        }
        private int loadCachedHash(long scopedId) {
            int bucket = (int)scopedId;
            bucket = ((bucket ^ (bucket>>>SEG_LEN_BITS))<<1) & HASH_CACHE_MASK;
            long lo = hashCache[bucket  ];
            long hi = hashCache[bucket+1];
            return  (int)lo == (int)hi && // not corrupted
                    ((hi&0xffffffff00000000L) | (lo>>>32)) == scopedId // matching ID
                    ? (int)hi : 0;
        }

        short acquire() {
            assert (generation&GEN_INACTIVE_MASK) != 0 : "acquire() on active Scope0";
            generation      = (short)((generation+1)&~GEN_INACTIVE_MASK);
            internAuthority = false;
            shared[0]       = EMPTY;
            return generation;
        }

        void release() {
            assert (generation&GEN_INACTIVE_MASK) == 0 : "release() on released Scope0";
            internAuthority = false;
            Arrays.fill(shared, 1, shared.length, null);
            Arrays.fill(hashCache, 0L);
            int unpooledSegments = 0;
            for (int i = KEEP_SEGMENTS_ON_RELEASE; i < segments.length; i++) {
                var h = bytes[i];
                if (h == null)
                    break;
                arrays  [i] = null;
                segments[i] = null;
                ropes   [i] = null;
                bytes   [i] = null;
                if (h.arr.length == POOLED_SEG_LEN) {
                    h.transferOwnership(this, SEG_POOL);
                    if (SEG_POOL.offer(h) != null)
                        h.recycle(SEG_POOL);
                } else {
                    unpooledSegments++;
                    h.recycle(this);
                }
            }
            UNPOOLED_SEGS.getAndAddRelease(unpooledSegments);
            generation |= (short)GEN_INACTIVE_MASK;
        }
    }
    private static final Scope0[] scopes = new Scope0[MAX_SCOPE_ID+1];
    private static final Scope ZERO;
    static {
        ZERO = allocScope().takeOwnership(ScopedIds.class);
        if (ZERO.inner.scopeId != 0)
            throw new ExceptionInInitializerError("Broken id allocation");
        Scope[] prime = new Scope[Runtime.getRuntime().availableProcessors()/4];
        for (int i = 0; i < prime.length; i++)
            prime[i] = allocScope().takeOwnership(prime);
        for (int i = 0; i < prime.length; i++)
            prime[i] = prime[i].recycle(prime);
        assert BS.get(ScopeIdAllocator.ACTIVE_IDS, 0) : "ID zero MUST be reserved";
        assert BS.cardinality(ScopeIdAllocator.ACTIVE_IDS) == 1 : "Unexpected IDs in use";
    }

    /**
     * Represents the lifetime of a set of IDs.
     *
     * <p>This object allow acquiring 64-bit IDs for strings. While this object is alive,
     * IDs can be dereferenced through  static methods on {@link ScopedIds}.
     * Once {@link #recycle(Object)} is called, all IDs become invalid and will cause a
     * {@link ClosedScope} exception when dereferenced.</p>
     *
     * <p>There is a <strong>VERY</strong> small limit on the number of active scopes.
     * Leaking this object (not calling {@link #recycle(Object)} will quickly lead to
     * {@link OutOfScopeIds} exceptions</p>
     */
    public static sealed class Scope extends AbstractOwned<Scope> {
        private static final VarHandle LOCK;
        static {
            try {
                LOCK = MethodHandles.lookup().findVarHandle(Scope.class, "plainLock", int.class);
            } catch (NoSuchFieldException | IllegalAccessException e) {
                throw new ExceptionInInitializerError(e);
            }
        }
        private final Scope0 inner;
        public final int generation;
        private int segId;
        private int nextOff;
        private byte[] segU8;
        @SuppressWarnings("unused") private int plainLock;

        /* --- --- --- Lifecycle --- --- --- */
        public Scope() {
            int scopeId = ScopeIdAllocator.acquireId();
            var inner = scopes[scopeId];
            if (inner == null)
                scopes[scopeId] = inner = new Scope0(scopeId);
            this.generation = inner.acquire();
            this.segU8 = inner.arrays[0];
            this.inner      = inner;
            this.segId      = 0;
        }

        public int id() { return (generation << Integer.bitCount(MAX_SCOPE_ID)) + inner.scopeId; }

        @Override public @Nullable Scope recycle(Object currentOwner) {
            internalMarkGarbage(currentOwner);
            if (inner.generation != generation)
                throw new IllegalStateException("Scope was released elsewhere, this is a bug");
            inner.release();
            ScopeIdAllocator.releaseId(inner.scopeId);
            return null;
        }

        private void check() {
            requireAlive();
            assert inner.generation == generation : "Scope concurrently released, this is a bug";
        }

        private static final class Concrete extends Scope implements Orphan<Scope> {
            @Override public Scope takeOwnership(Object o) {return takeOwnership0(o);}
        }

        /* --- --- --- helpers --- --- --- */

        private void lock() {
            while ((int)LOCK.compareAndExchangeAcquire(this, 0, 1) != 0) onSpinWait();
        }
        private void unlock() {LOCK.setRelease(this, 0);}

        private int reserveBytes(int n) {
            int off = nextOff;
            if (off+n > MAX_SEG_LEN) {
                if (n > MAX_SEG_LEN)
                    throw new StringTooLarge();
                segId = inner.addSegment(segId+1);
                segU8 = inner.arrays[segId];
                off = 0;
            }
            nextOff = ((off+n)&~3) + 4;
            return off;
        }

        private int findSharedPrefixByRef(FinalSegmentRope sh) {
            if (sh == null || sh.len == 0)
                return 0;
            int i;
            for (i = 1; i < END_PREFIX; i++) {
                if (inner.shared[i] == null)
                    break;
                if (inner.shared[i] == sh)
                    return (short)i;
            }
            if (i >= END_PREFIX)
                return 0; // no space left
            if (i == BEGIN_INTERN_AUTH)
                inner.internAuthority = true;
            if (inner.internAuthority)
                return findSharedPrefix(sh);
            lock();
            if (inner.shared[i] == null)
                inner.shared[i] = sh;
            else
                i = 0; // lost race
            unlock();
            return i;
        }

        private static final FinalSegmentRope RESERVED_SLOT
                = FinalSegmentRope.asFinal("~x-fastersparql-reserved");
        private int findSharedPrefix(PlainRope nt) {
            if (nt.len == 0)
                return 0;
            int i;
            if (nt.get(0) != '<')
                return 0; // not an IRI
            for (i = 1; i < END_PREFIX; i++) {
                var sh = inner.shared[i];
                if (sh == null)
                    break; // no match found, create a shared rope at index i
                if (i >= BEGIN_DT && sh.get(0) == '"')
                    return 0; // no match and no space left
                if (sh.len >= nt.len)
                    continue; // sh is not a prefix
                if (nt.has(0, sh))
                    return i; // found a match
            }

            // no match, select a prefix of the given input
            int shLen = nt.len-1;
            if (shLen < 12) // <https://xxx
                return 0; // too short to intern
            if (i == BEGIN_INTERN_AUTH)
                inner.internAuthority = true;
            if (inner.internAuthority) shLen = nt.skipUntil(12, shLen, (byte)'/')+1;
            else                       shLen = nt.skipUntilLastNear(12, shLen, (byte)'/')+1;
            if (shLen >= nt.len-1 || shLen < 12)
                return 0; // prefix too short or too long/specific

            // i points to a free slot, attempt to reserve it
            lock();
            if (inner.shared[i] == null)
                inner.shared[i] = RESERVED_SLOT; // reserve slot
            else
                i = 0; // lost race
            unlock();
            if (i != 0)  // owns the slot at i, create a shared prefix
                inner.shared[i] = FinalSegmentRope.asFinal(nt, 0, shLen);
            return i;
        }


        static {//noinspection ConstantValue
            assert MAX_SHARED_ID < 256/4 : "MAX_SHARED_ID too large will be slow";
        }
        private static final int BEGIN_INTERN_AUTH = (MAX_SHARED_ID>>1) - 1;
        private static final int END_PREFIX = MAX_SHARED_ID-2;
        private static final int BEGIN_DT = (MAX_SHARED_ID+1)>>1;

        private int findDatatypeSuffix(FinalSegmentRope dtSuffix) {
            if (dtSuffix == null || dtSuffix.len == 0)
                return 0;
            int i;
            for (i = inner.shared.length-1; i >= BEGIN_DT; i--) {
                if (inner.shared[i] == null)
                    break;
                if (inner.shared[i] == dtSuffix)
                    return (short) i;
            }
            if (i < BEGIN_DT)
                return 0; // full
            lock();
            if (inner.shared[i] == null)
                inner.shared[i] = dtSuffix; // reserve slot
            else
                i = 0; // lost race
            unlock();
            return i;
        }

        /* --- --- --- Minting --- --- --- */

        /** Turn an N-Triples string into an ID. String data will be copied (except if
         *  creating a shared prefix and {@code nt} is a {@link FinalSegmentRope}). */
        public long put(@Nullable SegmentRope nt) {
            if (nt == null || nt.len == 0)
                return 0;
            check();
            short shId = 0;
            boolean isLit = nt.get(0) == '"';
            if (isLit) {
                shId  = (short)findDatatypeSuffix(SHARED_ROPES.internDatatypeOf(nt));
            } else if (nt.len >= MIN_INTERNED_LEN) {
                shId = (short)findSharedPrefix(nt);
            }
            int shLen = inner.shared[shId].len, localBegin = isLit ? 0 : shLen;
            int localLen = nt.len - shLen;
            final int    off, segId;
            final byte[] segU8;
            lock();
            try {
                off   = reserveBytes(localLen);
                segU8 = this.segU8;
                segId = this.segId;
            } finally { unlock(); }
            nt.copy(localBegin, localBegin+localLen, segU8, off);
            return make(inner.scopeId, shId, segId, off, localLen, isLit);
        }
        /** Turn an N-Triples string into an ID */
        public long put(@Nullable PlainRope nt) {
            return nt instanceof SegmentRope sr ? put(sr) : put((TwoSegmentRope)nt);
        }
        /** Turn an N-Triples string into an ID */
        public long put(@Nullable TwoSegmentRope nt) {
            if (nt == null || nt.len == 0)
                return 0;
            check();
            short shId = 0;
            boolean isLit = nt.get(0) == '"';
            if (isLit)
                shId = (short)findDatatypeSuffix(SHARED_ROPES.internDatatypeOf(nt));
            else if (nt.fstLen >= MIN_INTERNED_LEN)
                shId = (short)findSharedPrefix(nt);
            int shLen = inner.shared[shId].len, localBegin = isLit ? 0 : shLen;
            int localLen = nt.len - shLen;
            final int    off, segId;
            final byte[] segU8;
            try {
                off   = reserveBytes(localLen);
                segId = this.segId;
                segU8 = this.segU8;
            } finally { unlock(); }
            nt.copy(localBegin, localBegin+localLen, segU8, off);
            return make(inner.scopeId, shId, segId, off, localLen, isLit);
        }

        /**
         * Turns an N-Triples string into an ID.
         *
         * @param shared a shared prefix or suffix of the string
         * @param localSeg mandatory {@link MemorySegment} with the local part of the string
         * @param localU8 {@link MemorySegment#heapBase()} of {@code localSeg}
         * @param localOff where the local part starts inside {@code localSeg}
         * @param localLen length of the local part
         * @param sharedKind a value from {@link SharedKind}
         * @return the new ID
         */
        public long put(@Nullable FinalSegmentRope shared, MemorySegment localSeg,
                        byte @Nullable[] localU8, long localOff, int localLen,
                        byte sharedKind) {
            if (shared == null)
                shared = EMPTY;
            if (shared.len == 0 && localLen == 0)
                return 0;
            check();
            boolean isLit = sharedKind == SharedKind.WHOLE_UNKNOWN
                          ? localSeg.get(JAVA_BYTE, localOff) == '"'
                          : SharedKind.isLit(sharedKind);
            int shId = SharedKind.isLit(sharedKind) ? findDatatypeSuffix(shared)
                                                    : findSharedPrefixByRef(shared);
            int copyShLen = shared.len-inner.shared[shId].len;
            final int    dst, segId;
            final byte[] segU8;
            lock();
            try {
                dst   = reserveBytes(localLen + copyShLen);
                segU8 = this.segU8;
                segId = this.segId;
            } finally { unlock(); }
            int localDst  = dst + (isLit ? 0 : copyShLen);
            if (copyShLen > 0) {
                shared.copy(shared.len-copyShLen, shared.len, segU8,
                        dst+(isLit ? localLen : 0));
            }
            if (localU8 != null)
                System.arraycopy(localU8, (int)localOff, segU8, localDst, localLen);
            else
                MemorySegment.copy(localSeg, JAVA_BYTE, localOff, segU8, localDst, localLen);
            return make(inner.scopeId, shId, segId, dst, localLen+copyShLen, isLit);

        }

        /** Turns {@code shared+local} (or {@code local+shared}, if {@code isLit}) into an ID.
         *  Data from {@code local will be copied}. */
        public long put(@Nullable FinalSegmentRope shared, SegmentRope local, byte sharedKind) {
            return put(shared, local.segment, local.utf8, local.offset, local.len, sharedKind);
        }

        /** Turns a {@link FinalTerm} into an ID. Bytes of {@link Term#local()} will be copied */
        public long put(@Nullable FinalTerm term) {
            if (term == null)
                return 0L;
            return put(term.finalShared(), term.local(), term.sharedKind());
        }

        /** Turns a {@link Term} into an ID. Will avoid copying bytes from {@link Term#shared()}
         * if it is a {@link FinalSegmentRope}. */
        public long put(@Nullable Term term) {
            if (term == null)
                return 0L;
            return term.shared() instanceof FinalSegmentRope f
                    ? put(f, term.local(), term.sharedKind())
                    : putCold(term);
        }
        private long putCold(Term term) {
            try (var view = PooledTwoSegmentRope.ofEmpty()) {
                view.wrapFirst(term.shared());
                view.wrapSecond(term.local());
                if (term.sharedSuffixed())
                    view.flipSegments();
                return put(view);
            }
        }
    }

    /**
     * Start a new scope.
     *
     * <p><strong>Important:</strong> there is capacity for few scopes, thus the returned
     * {@link Scope} object <strong>MUST</strong> be released via {@link Scope#recycle(Object)}.
     * </p>
     *
     * @return a new {@link Scope}
     */
    public static Orphan<Scope> allocScope() {
        return new Scope.Concrete();
    }

    /** Gets a shared prefix or suffix of the N-Triples string represented by {@code scopedId} */
    public static FinalSegmentRope shared(long scopedId) {
        return scopes[scp_id(scopedId)].shared[shr_id(scopedId)];
    }
    /** Whether {@link #shared(long)} is a suffix */
    public static boolean sharedSuffix(long scopedId) {return is_lit(scopedId);}
    /** Get the segment where the local part of the N-Triples representation of
     * {@code scopeId} is stored */
    public static MemorySegment localSeg(long scopedId) {
        return scopes[scp_id(scopedId)].check().segments[seg_id(scopedId)];
    }

    /** {@link MemorySegment#heapBase()} of {@link #localSeg(long)} */
    public static byte[] localU8(long scopedId) {
        return scopes[scp_id(scopedId)].check().arrays[seg_id(scopedId)];
    }

    /** A {@link SegmentRope} wrapping the whole {@link #localSeg(long)}*/
    public static SegmentRope localRope(long scopedId) {
        return scopes[scp_id(scopedId)].check().ropes[seg_id(scopedId)];
    }

    /** Index of the first byte of the local part of the N-Triples representation of
     * {@code scopedId} starts in {@link #localSeg(long)}. */
    public static int localOff(long scopedId) {return seg_off(scopedId);}

    /** Length of the local part of the N-Triples string associated with {@code scopedId} */
    public static int localLen(long scopedId) {return seg_len(scopedId);}

    /** Get the total length of the N-Triples serialization {@link #shared(long)}
     * + {@link #localLen(long)}. */
    public static int len(long scopedId) {
        int sid = shr_id(scopedId);
        return seg_len(scopedId) + (sid == 0 ? 0 : scopes[scp_id(scopedId)].shared[sid].len);
    }

    /**
     * Whether the N-Triples serialization is empty. All empty terms SHOULD be
     * represented by {@code 0}. This function tolerates potential bugs where that ceases
     * to be the case. Implementation is virtually the same cost as checkign for {@code == 0}.
     */
    public static boolean isEmpty(long scopedId) { return (scopedId&EMPTY_MASK) == 0;}

    /** {@link Term#toString()}  on {@link #asTerm(long)}, without making garbage */
    public static String toString(long scopedId) {
        if (isEmpty(scopedId))
            return "";
        try (var view = PooledTwoSegmentRope.ofEmpty()) {
            view(scopedId, view);
            return view.toString();
        }
    }

    /**
     * Get a {@link FinalTerm} representation of the N-Triples string associated with
     * {@code scopedId}
     */
    public static @Nullable FinalTerm asTerm(long scopedId) {
        if (isEmpty(scopedId))
            return null;
        var s = scopes[scp_id(scopedId)].check();
        var view = new SegmentRopeView();
        int si = seg_id(scopedId);
        view.wrap(s.segments[si], s.arrays[si], seg_off(scopedId), seg_len(scopedId));
        return new FinalTerm(s.shared[shr_id(scopedId)], view, is_lit(scopedId));
    }

    /**
     * Update {@code view} to represent the local part of the N-Triples serialization
     * identified by {@code scopedId}.
     * @return whether {@code view.len != 0}
     */
    public static boolean localView(long scopedId, SegmentRopeView view) {
        var s = scopes[scp_id(scopedId)].check();
        int si = seg_id(scopedId);
        view.wrap(s.segments[si], s.arrays[si], seg_off(scopedId), seg_len(scopedId));
        return (scopedId&EMPTY_MASK) != 0;
    }

    /**
     * Update {@code view} to represent the N-Triples serialization identified by {@code scopedId}.
     * @return whether {@code view.len != 0}
     */
    public static boolean view(long scopedId, TwoSegmentRope view) {
        var s = scopes[scp_id(scopedId)].check();
        int si = seg_id(scopedId);
        view.wrapFirst(s.shared[shr_id(scopedId)]);
        view.wrapSecond(s.segments[si], s.arrays[si], seg_off(scopedId), seg_len(scopedId));
        if (is_lit(scopedId))
            view.flipSegments();
        return (scopedId&EMPTY_MASK) != 0;
    }

    /**
     * Update {@code view} to represent the N-Triples serialization identified by {@code scopedId}.
     * @return whether {@code view.len != 0}
     */
    public static boolean view(long scopedId, TermView view) {
        if (isEmpty(scopedId))
            return false;
        var s = scopes[scp_id(scopedId)].check();
        int si = seg_id(scopedId);
        view.wrap(s.shared[shr_id(scopedId)], s.segments[si], s.arrays[si], seg_off(scopedId), seg_len(scopedId), is_lit(scopedId));
        return true;
    }

    /**
     * Update {@code view} to represent the N-Triples serialization identified by {@code scopedId}.
     * @return whether {@code view.len != 0}
     */
    public static TermInfo.Type info(long scopedId, TermInfo out) {
        var s = scopes[scp_id(scopedId)].check();
        int si = seg_id(scopedId);
        return out.setSharedAndSegment(false,  s.shared[shr_id(scopedId)],
                s.segments[si], s.arrays[si],
                seg_off(scopedId), seg_len(scopedId),
                is_lit(scopedId) ? SharedKind.SUFF_LIT : SharedKind.PREF_IRI_OR_BLANK);
    }

    /** {@link Term#endLex()} of {@link #asTerm(long)}, without making garbage. */
    public static int lexEnd(long scopedId) {
        var s = scopes[scp_id(scopedId)].check();
        SegmentRopeView local = s.ropes[seg_id(scopedId)];
        int off = seg_off(scopedId), len = seg_len(scopedId);
        if (len == 0 || !is_lit(scopedId) || local.get(off) != '"')
            return 0; // not a literal
        if (shr_id(scopedId) != 0) { // has datatype suffix
            assert s.shared[shr_id(scopedId)].len > 0 : "empty shared with shr_id() != 0";
            return len; // shared is not empty,
        }
        return local.skipUntilLastNear(off, off+len, (byte)'"')-off; // no datatype
    }

    /** {@link Term#type()} of {@link #asTerm(long)}, without making garbage. */
    public static Term.@Nullable Type termType(long scopedId) {
        if (isEmpty(scopedId))
            return null;
        var s = scopes[scp_id(scopedId)].check();
        if (is_lit(scopedId))
            return Term.Type.LIT;
        if (shr_id(scopedId) != 0) {
            assert s.shared[shr_id(scopedId)].get(0) == '<' : "not an IRI";
            return Term.Type.IRI;
        }
        return switch (s.arrays[seg_id(scopedId)][seg_off(scopedId)]) {
            case '"' -> Term.Type.LIT;
            case '<' -> Term.Type.IRI;
            case '_' -> Term.Type.BLANK;
            case '?', '$'  -> Term.Type.VAR;
            default -> throw new InvalidTermException(toString(scopedId), 0, "bad start");
        };
    }

    /**
     * {@link Term#toSparql(ByteSink, PrefixAssigner)} on {@link #asTerm(long)} without the garbage.
     */
    public static int writeSparql(ByteSink<?, ?> dest, long scopedId,
                                  PrefixAssigner prefixAssigner) {
        var s  = scopes[scp_id(scopedId)].check();
        int si = seg_id(scopedId), off = seg_off(scopedId), len = seg_len(scopedId);
        var sh = s.shared[shr_id(scopedId)];
        if (is_lit(scopedId) && sh.len == 0) {
            sh = SHARED_ROPES.internDatatypeOf(s.ropes[si], off, off+len);
            if (sh.len != 0)
                len -= sh.len;
        }
        return Term.toSparql(dest, prefixAssigner, sh, s.segments[si], s.arrays[si],
                             off, len, is_lit(scopedId));
    }

    /**
     * Append the N-Triples representation of {@code scopedId} to {@code dest}.
     * @return The number of bytes written, see {@link #len(long)}
     */
    public static <S extends ByteSink<S, T>, T> ByteSink<S, T>
    writeNT(ByteSink<S, T> dest, long scopedId, byte[] nullValue) {
        if (isEmpty(scopedId)) {
            dest.append(nullValue);
        } else {
            Scope0 s = scopes[scp_id(scopedId)].check();
            int si = seg_id(scopedId);
            SegmentRope sh = s.shared[shr_id(scopedId)];
            if (!is_lit(scopedId))
                dest.append(sh);
            dest.append(s.segments[si], s.arrays[si], seg_off(scopedId), seg_len(scopedId));
            if (is_lit(scopedId))
                dest.append(sh);
        }
        return dest;
    }

    /** {@link ByteSink#append(SegmentRope, int, int)} on {@link #asTerm(long)}
     *  with given {@code begin} and {@code end}, but without any allocation */
    public static <S extends ByteSink<S, T>, T> ByteSink<S, T>
    write(ByteSink<S, T> dest, long scopedId, int begin, int end) {
        if (end > len(scopedId) || begin < 0)
            throw new IndexOutOfBoundsException("[begin, end) is out of bounds");
        if (end <= begin)
            return dest; // no-op
        Scope0 s = scopes[scp_id(scopedId)].check();
        int si = seg_id(scopedId), localLen = seg_len(scopedId);
        SegmentRope sh = s.shared[shr_id(scopedId)];
        if (!is_lit(scopedId)) {
            if (begin < sh.len)
                dest.append(sh, begin, Math.min(sh.len, end));
            begin = Math.max(0, begin - sh.len);
            end   = Math.max(0, end   - sh.len);
            if (end <= begin)
                return dest;
        }
        if (begin < localLen) {
            dest.append(s.segments[si], s.arrays[si],
                    seg_off(scopedId)+begin, Math.min(end, localLen)-begin);
        }
        if (is_lit(scopedId)) {
            begin = Math.max(0, begin - localLen);
            end   = Math.max(0, end   - localLen);
            if (end > begin)
                dest.append(sh, begin, Math.min(end, sh.len));
        }
        return dest;
    }

    private static FinalSegmentRope datatypeSuffix0(Scope0 s, long scopedId) {
        if (!is_lit(scopedId))
            return EMPTY;
        var sh = s.shared[shr_id(scopedId)];
        if (sh == EMPTY) {
            int begin = seg_off(scopedId), end = begin+seg_len(scopedId);
            sh = SHARED_ROPES.internDatatypeOf(s.ropes[seg_id(scopedId)], begin, end);
        }
        return sh;
    }

    /** {@link java.util.Comparator#compare(Object, Object)} that compares by numeric value
     *  or by N-Triples serialization */
    public static int compare(long lId, long rId) {
        if (lId == rId) return 0;
        if ((lId&EMPTY_MASK) == 0)
            return (rId&EMPTY_MASK) == 0 ? 0 : -1;
        if ((rId&EMPTY_MASK) == 0)
            return 1;
        Scope0 lsc = scopes[scp_id(lId)].check(), rsc = scopes[scp_id(rId)].check();
        short lsi = (short)seg_id(lId), rsi = (short)seg_id(rId);
        if (isNumericDatatype(datatypeSuffix0(lsc, lId)) &&
                isNumericDatatype(datatypeSuffix0(rsc, rId))) {
            return SegmentRope.compareNumbers(
                    lsc.segments[lsi], lsc.arrays[lsi], seg_off(lId)+1, seg_len(lId)-1,
                    rsc.segments[rsi], rsc.arrays[rsi], seg_off(rId)+1, seg_len(rId)-1);
        }
        FinalSegmentRope lsh = lsc.shared[shr_id(lId)], rsh = rsc.shared[shr_id(rId)];
        long lo0 = lsh.offset, lo1 = seg_off(lId), ro0 = rsh.offset, ro1 = seg_off(rId);
        int  ll0 = lsh.len,    ll1 = seg_len(lId), rl0 = rsh.len,    rl1 = seg_len(rId);
        if (U != null) {
            lo0 += lsh.segment.address();
            ro0 += rsh.segment.address();
        }
        if (is_lit(lId)) { lo0 = lo1; ll0 = ll1; lo1 = lsh.offset; ll1 = lsh.len; }
        if (is_lit(rId)) { ro0 = ro1; rl0 = rl1; ro1 = lsh.offset; rl1 = lsh.len; }
        if (U == null) {
            MemorySegment l0 =  lsh.segment, l1 = lsc.segments[lsi];
            MemorySegment r0 =  rsh.segment, r1 = rsc.segments[rsi];
            if (is_lit(lId)) { l0 = l1; l1 = lsh.segment; }
            if (is_lit(rId)) { r0 = r1; r1 = rsh.segment; }
            return SegmentRope.compare2_2(l0, lo0, ll0, l1, lo1, ll1,
                    r0, ro0, rl0, r1, ro1, rl1);
        } else {
            byte[] l0 =  lsh.utf8, l1 = lsc.arrays[lsi];
            byte[] r0 =  rsh.utf8, r1 = rsc.arrays[rsi];
            if (is_lit(lId)) { l0 = l1; l1 = lsh.utf8; }
            if (is_lit(rId)) { r0 = r1; r1 = rsh.utf8; }
            return SegmentRope.compare2_2(l0, lo0, ll0, l1, lo1, ll1,
                    r0, ro0, rl0, r1, ro1, rl1);
        }
    }

    public static boolean equals(long lId, long rId) { return compare(lId, rId) == 0; }

    /** Calls {@link Term#hashCode(int, SegmentRope, MemorySegment, byte[], long, int, boolean)}
     * for the given {@code scopedId}. */
    public static int hash(int prefixHash, long scopedId) {
        return prefixHash == FNV_BASIS ? hash(scopedId) : hash0(prefixHash, scopedId);
    }
    private static int hash0(int prefixHash, long scopedId) {
        var s        = scopes[scp_id(scopedId)].check();
        int segId    = seg_id(scopedId), lOff = seg_off(scopedId), lLen = seg_len(scopedId);
        var sh       = s.shared[shr_id(scopedId)];
        if (is_lit(scopedId) && sh.len == 0) {
            sh = SHARED_ROPES.internDatatypeOf(s.ropes[segId], lOff, lOff+lLen);
            if (sh.len != 0)
                lLen -= sh.len;
        }
        return Term.hashCode(prefixHash, sh, s.segments[segId], s.arrays[segId],
                             lOff, lLen, is_lit(scopedId));
    }
    /** {@link #hash(int, long)} with {@code prefixHash = FNV_BASIS} */
    public static int hash(long scopedId) {
        Scope0 s = scopes[scp_id(scopedId)];
        int h = s.loadCachedHash(scopedId);
        if (h != 0)
            return h; // answer from cache
        s.storeCachedHash(scopedId, h = hash0(FNV_BASIS, scopedId));
        return h; // computed hash
    }
}
