package com.github.alexishuf.fastersparql.util.concurrent;

import com.github.alexishuf.fastersparql.util.owned.AbstractOwned;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.foreign.MemorySegment;
import java.util.Arrays;
import java.util.function.IntFunction;

import static com.github.alexishuf.fastersparql.util.concurrent.PoolStackSupport.THREADS;
import static com.github.alexishuf.fastersparql.util.owned.SpecialOwner.CONSTANT;
import static com.github.alexishuf.fastersparql.util.owned.SpecialOwner.RECYCLED;
import static java.lang.System.arraycopy;

public abstract sealed class Bytes extends AbstractOwned<Bytes> {
    private static final byte[]        EMPTY_ARRAY   = new byte[0];
    private static final MemorySegment EMPTY_SEGMENT = MemorySegment.ofArray(EMPTY_ARRAY);

    public final byte[] arr;
    public final MemorySegment segment;
    private final @Nullable LevelAlloc<Bytes> pool;

    public static Orphan<Bytes> createPooled(byte[] u8) {
        return new Concrete(u8, ALLOC);
    }
    public static Orphan<Bytes> createUnpooled(byte[] u8) {
        return new Concrete(u8, null);
    }

    protected Bytes(byte[] arr, @Nullable LevelAlloc<Bytes> pool) {
        this.arr     = arr;
        this.segment = arr == EMPTY_ARRAY ? EMPTY_SEGMENT : MemorySegment.ofArray(arr);
        this.pool    = pool;
    }

    /** Equivalent to {@link #recycle(Object)}, but will behave as if the
     *  {@link Thread#threadId()} of {@link Thread#currentThread()} were {@code threadId}. */
    public @Nullable Bytes recycle(int threadId, Object currentOwner) {
        if (pool != null) {
            internalMarkRecycled(currentOwner);
            currentOwner = RECYCLED;
            if (INVALIDATE_RECYCLED)
                Arrays.fill(arr, (byte)0);
            if (pool.offer(threadId, this, arr.length) == null)
                return null;
        }
        return internalMarkGarbage(currentOwner);
    }


    @Override public @Nullable Bytes recycle(Object currentOwner) {
        return recycle((int)Thread.currentThread().threadId(), currentOwner);
    }

    public Bytes recycleAndGetEmpty(Object currentOwner) {
        recycle(currentOwner);
        return EMPTY;
    }

    /**
     * Whether this {@link Bytes} originated from a pool and thus should eventually return
     * to it via {@link #recycle(Object)}.
     *
     * @return {@code true} iff this was obtained from a pool.
     */
    public boolean isPooled() { return pool != null; }

    private static final class Concrete extends Bytes implements Orphan<Bytes> {
        private Concrete(byte[] arr, LevelAlloc<Bytes> pool) {super(arr, pool);}
        @Override public Bytes takeOwnership(Object o) {return takeOwnership0(o);}
    }

    public static final Bytes EMPTY;
    public static final LevelAlloc<Bytes> ALLOC;
    private static final boolean INVALIDATE_RECYCLED = Bytes.class.desiredAssertionStatus();
    private static final IntFunction<Bytes> FAC = new IntFunction<>() {
        @Override public Bytes apply(int len) {
            byte[] arr = len == 0 ? EMPTY_ARRAY : new byte[len];
            return new Bytes.Concrete(arr, ALLOC).takeOwnership(RECYCLED);
        }
        @Override public String toString() {return "Bytes.FAC";}
    };
    private static final Runnable PRIME = new Runnable() {
        @Override public void run() {
            for (int level = 0; level <= 10; level++)
                ALLOC.primeLevel(FAC, level, 4, 0);
            for (int level = 11; level <= 15; level++)
                ALLOC.primeLevel(FAC, level, 1, 0);
        }
        @Override public String toString() {return "Bytes.PRIME";}
    };
    static {
        int fixed = 16            /* header */
                  + 2*4           /* fields */
                  + 20            /* byte[] header */
                  + 16 + 8 + 2*4; /* MemorySegment */
        int perByte = 1;
        var caps = new LevelAlloc.Capacities()
                .set(0, 3, THREADS*256)
                .set(4, 6, THREADS*2048)
                .setSameBytesUsage(7, 15,
                        THREADS*2048*(20+(1<<6)),
                        20, 1);
        ALLOC = new LevelAlloc<>(Bytes.class, "BYTES", fixed, perByte, FAC, caps);
        //noinspection StaticInitializerReferencesSubClass
        EMPTY = new Concrete(new byte[0], ALLOC).takeOwnership(CONSTANT);
        Primer.INSTANCE.sched(PRIME);
    }

    public static Orphan<Bytes> atLeastElse(int preferredLen, int fallbackLen) {
        Bytes b = ALLOC.pollAtLeast(preferredLen);
        if (b == null)
            b = ALLOC.createAtLeast(fallbackLen);
        return b.releaseOwnership(RECYCLED);
    }

    public static Orphan<Bytes> atLeast(int len) {
        return atLeast((int)Thread.currentThread().threadId(), len);
    }

    public static Orphan<Bytes> atLeast(int threadId, int len) {
        return ALLOC.createAtLeast(threadId, len).releaseOwnership(RECYCLED);
    }

    /**
     * Get {@code offer} or a new {@link Bytes} that has a length {@code >= minLen} and is owned
     * by {@code owner}.
     *
     * @param minLen required minimum length for the returned {@link Bytes}
     * @param offer {@code null} or a {@link Bytes} owned by {@code owner}. If its length is
     *              {@code >= minLen}, {@code offer} will be returned, else it will be recycled
     * @param owner current owner of the returned {@code Bytes} and of {@code offer} (if not null)
     * @return a {@link Bytes}, with uninitialized bytes that has a length of at least
     *         {@code minLen} and is owned by {@code owner}
     */
    public static Bytes atLeast(int minLen, @Nullable Bytes offer, Object owner) {
        if (offer != null) {
            if (offer.arr.length >= minLen)
                return offer;
            offer.recycle(owner);
        }
        return atLeast(minLen).takeOwnership(owner);
    }

    /**
     * Similar to {@link #atLeast(int)}, but will always return a zero-filled array, even if
     * it was fetched from the pool.
     *
     * @param len desired minimum length
     * @param offer {@code null} or a {@link Bytes} instance owned by {@code owner}
     * @param owner owner of {@code offer} and of the returned {@link Bytes} instance
     * @return a {@link Bytes} owned by {@code owner} with {@code arr.length >= len} and all bytes set to zero.
     */
    public static Bytes cleanAtLeast(int len, @Nullable Bytes offer, Object owner) {
        Bytes bytes;
        boolean dirty = true;
        if (offer == null || offer.arr.length < len) {
            if (offer != null)
                offer.recycle(owner);
            int level = LevelAlloc.len2level(len);
            bytes = ALLOC.pollFromLevel(level);
            if (bytes != null) {
                bytes.transferOwnership(RECYCLED, owner);
            } else {
                var pool = ALLOC.isLevelPooled(level) ? ALLOC : null;
                if (pool != null)
                    len = 1<<level;
                dirty = false;
                bytes = new Concrete(new byte[len], pool).takeOwnership(owner);
            }
        } else {
            bytes = offer;
        }
        if (dirty)
            Arrays.fill(bytes.arr, (byte)0);
        return bytes;
    }


    /**
     * Get a {@link Bytes} object with length {@code >= newLen} containing the first
     * {@code oldValidLen} bytes of {@code orphan} and {@link #recycle(Object)} {@code orphan}.
     *
     * @param orphan The current {@link Bytes} object with valid data in the range
     *               {@code [0, oldValidLen)}. Once this method returns, {@code orphan} will
     *               be pooled.
     * @param oldValidLen up until which byte {@code orphan} contains valid data that is to be
     *                    present the returned {@link Bytes}
     * @param newLen desired minimum length of the returned {@link Bytes} object. This must be
     *               {@code > oldValidLen}.
     * @return A {@link Bytes} object, as an {@link Orphan}, containing the first
     *         {@code oldValidLen} bytes of the given {@code orphan} but with a length
     *         {@code >= newLen}
     */
    public static Orphan<Bytes> grow(Orphan<Bytes> orphan, int oldValidLen, int newLen) {
        Bytes dst = ALLOC.createAtLeast(newLen);
        Bytes src = orphan.takeOwnership(RECYCLED);
        arraycopy(src.arr, 0, dst.arr, 0, oldValidLen);
        if (ALLOC.offer(src, src.arr.length) != null)
            src.internalMarkGarbage(RECYCLED);
        return dst.releaseOwnership(RECYCLED);
    }

    /**
     * Similar to {@link #grow(Orphan, int, int)} but instead receives a {@link Bytes} owned
     * by {@code owner} and returns a {@link Bytes} object already owned by owner.
     */
    public static Bytes grow(@Nullable Bytes src, Object owner, int oldValidLen, int newLen) {
        Bytes dst = ALLOC.createAtLeast(newLen);
        if (src != null && src != EMPTY) {
            arraycopy(src.arr, 0, dst.arr, 0, oldValidLen);
            src.recycle(owner);
        }
        return dst.transferOwnership(RECYCLED, owner);
    }

    /**
     * Equivalent to {@link #copy(byte[], int, int)} with {@code offset=0} and
     * {@code len=src.length}
     */
    public static Orphan<Bytes> copy(byte[] src) {
        return copy(src, 0, src.length);
    }

    /**
     * Get a {@link Bytes} object containing a copy of the first {@code len} bytes after
     * {@code offset} in {@code src}.
     *
     * @param src source of bytes to copy from
     * @param offset index of first byte to copy from {@code src}
     * @param len number of bytes to copy from {@code src}
     * @return A {@link Bytes} with length {@code >= len} containing the {@code len} bytes of
     *         {@code src} that start at {@code offset}
     */
    public static Orphan<Bytes> copy(byte[] src, int offset, int len) {
        Bytes dst = ALLOC.createAtLeast(len);
        arraycopy(src, offset, dst.arr, 0, len);
        return dst.releaseOwnership(RECYCLED);
    }

    /** Equivalent to {@link #copy(byte[], int, Bytes, int, Object, int)} with {@code dstPos=0}. */
    public static Bytes copy(byte[] src, int srcPos,
                             @Nullable Bytes dst, Object owner, int len) {
        if (dst == null || dst.arr.length < len) {
            Bytes bigger = atLeast(len).takeOwnership(owner);
            if (dst != null) dst.recycle(owner);
            dst = bigger;
        }
        arraycopy(src, srcPos, dst.arr, 0, len);
        return dst;
    }

    /**
     * Copy {@code len} bytes from {@code src} starting at index {@code srcPos} into {@code dst}
     * at index {@code dstPos}, swapping {@code dst} for a bigger {@link Bytes} object if its
     * length is below {@code dstPos+len} or if it is {@code null}
     *
     * @param src the source of the bytes to copy
     * @param srcPos index of the first byte to copy from {@code src}
     * @param dst destination of bytes, if not null, must be owned by {@code owner}
     * @param dstPos destination of copied bytes within {@code dst.arr}. If {@code dst == null},
     *               bytes below this index will be zero-filled in the created {@link Bytes}
     * @param owner The current owner of {@code dst} and also the owner of the returned
     *              {@link Bytes}, if a new object is created to receive the bytes
     * @param len number of bytes to copy from {@code src}
     * @return a {@link Bytes} object owned by {@code owner} and containing the bytes of
     *         {@code dst} until index {@code dstPos} (or zeros, if {@code dst == null})
     *         followed by the {@code len} bytes from {@code src} (starting at {2code srcPos}).
     */
    public static Bytes copy(byte[] src, int srcPos,
                             @Nullable Bytes dst, int dstPos, Object owner, int len) {
        if (dst == null || dst.arr.length-dstPos < len) {
            Bytes bigger = atLeast(dstPos + len).takeOwnership(owner);
            if (dst != null) {
                arraycopy(dst.arr, 0, bigger.arr, 0, dstPos);
                dst.recycle(owner);
            } else {
                Arrays.fill(bigger.arr, 0, dstPos, (byte)0);
            }
            dst = bigger;
        }
        arraycopy(src, srcPos, dst.arr, dstPos, len);
        return dst;
    }
}
