package com.github.alexishuf.fastersparql.batch.dedup;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.batch.type.RowBucket;
import com.github.alexishuf.fastersparql.util.ThrowingConsumer;
import com.github.alexishuf.fastersparql.util.concurrent.ArrayPool;

import java.util.Arrays;

import static java.lang.Math.max;

public class WeakCrossSourceDedup<B extends Batch<B>> extends Dedup<B> {
    private final RowBucket<B> table;
    private int[] hashesAndSources; // [hash for rows[0], sources for [0], hash for [1], ...]
    private byte[] bucketInsertion; // values are 0 <= bucketInsertion[i] < 8
    private short buckets, bucketWidth;

    public WeakCrossSourceDedup(BatchType<B> batchType, int cols) {
        super(batchType, cols);
        if (cols < 0)
            throw new IllegalArgumentException();
        int rowsCapacity = max(batchType.preferredTermsPerBatch()/max(cols, 1), 1);
        this.table = batchType.createBucket(rowsCapacity, cols);
        this.table.maximizeCapacity();
        clear0(table.capacity());
    }

    private void clear0(int rowsCapacity) {
        bucketWidth = (short)(rowsCapacity > 16 ? 4 : 1);
        buckets = (short)(rowsCapacity/bucketWidth);
        rowsCapacity = buckets*bucketWidth;
        this.bucketInsertion = ArrayPool.bytesAtLeast(buckets); // each bucket has 8 items
        this.hashesAndSources = ArrayPool.intsAtLeast(rowsCapacity<<1);
        Arrays.fill(bucketInsertion, (byte)0);
        Arrays.fill(hashesAndSources, 0);
    }

    @Override public void clear(int cols) {
        int rowsCapacity = max(table.batchType().preferredTermsPerBatch()/cols, 1);
        table.clear(rowsCapacity, cols);
        table.maximizeCapacity();
        rowsCapacity = table.capacity();
        clear0(rowsCapacity);
    }

    @Override public void recycleInternals() {
        hashesAndSources = ArrayPool.INT.offer(hashesAndSources, hashesAndSources.length);
        bucketInsertion = ArrayPool.BYTE.offer(bucketInsertion, bucketInsertion.length);
        table.recycleInternals();
    }

    @Override public int capacity() { return table.capacity(); }

    @Override public boolean isWeak() { return true; }

    /**
     * Tests whether there was a previous {@code isDuplicate(r, another)} call with
     * {@code another != source} and {@code r} {@link RowBucket#equals(int, Batch, int)}
     * {@code row}.
     *
     * <p>For performance reasons, false negatives are allowed. That is, this method may return
     * {@code false} when {@code row} is in fact a duplicate of another row produced by a source
     * other than {@code source}. The inverse (returning {@code true} for a non-duplicate row)
     * does not occur.</p>
     *
     * @param row the row to check
     * @param source index of the source within the set of sources. This will be ignored unless
     *               the implementation is doing cross-source deduplication, where only a row
     *               previously output by <strong>another</strong> source will be considered
     *               duplicate.
     * @return whether {@code row} is a duplicate of a row output by a source other
     *         than {@code source}
     */
    @Override public boolean isDuplicate(B batch, int row, int source) {
        if (DEBUG) checkBatchType(batch);
        int hash = enhanceHash(batch.hash(row));
        short bucket = (short)((hash&Integer.MAX_VALUE)%buckets), bucketWidth = this.bucketWidth;
        int begin = bucket*bucketWidth, end = begin+bucketWidth, match = -1;
        for (int i = begin; match == -1 && i < end; i++) {
            if (hashesAndSources[i<<1] == hash && table.equals(i, batch, row))
                match = i;
        }
        lock();
        try {
            int sources, sourcesIdx;
            if (match == -1) { // add row to the table, consuming an insertion point
                sources = 0;
                match = begin + bucketInsertion[bucket]; //get insertion index
                bucketInsertion[bucket] = (byte) ((bucketInsertion[bucket] + 1) % bucketWidth);
                table.set(match, batch, row);
                int hashIdx = match << 1;
                sourcesIdx = hashIdx+1;
                hashesAndSources[hashIdx] = hash;
            } else if (!table.equals(match, batch, row)) {
                // race: row was overwritten in the table before this thread got the lock.
                // Treat rows as non-duplicate. An eventual new call with row will not suffer
                // this race and will add it to the table.
                return false; // treat as not duplicate. subsequent call for same row will not suffer
            } else {
                sources = hashesAndSources[sourcesIdx = (match<<1) + 1];
            }
            int mask = 1 << source;
            boolean duplicate = (sources |= mask) != mask; //mark and test if only source
            hashesAndSources[sourcesIdx] = sources;
            return duplicate;
        } finally { unlock(); }
    }

    @Override public boolean contains(B batch, int row) {
        if (DEBUG) checkBatchType(batch);
        int hash = enhanceHash(batch.hash(row));
        int i = ((hash&Integer.MAX_VALUE)%buckets)*bucketWidth, e = i+bucketWidth;
        while (i < e && (hashesAndSources[i<<1] != hash || !table.equals(i, batch, row)))
            ++i;
        return i < e;
    }

    @Override public <E extends Throwable> void forEach(ThrowingConsumer<B, E> consumer) throws E {
        for (B b : table)
            if (b != null) consumer.accept(b);
    }
}
