package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.model.rope.SharedRopes;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import com.github.alexishuf.fastersparql.util.owned.Guard;
import com.github.alexishuf.fastersparql.util.owned.Guard.BatchGuard;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.stream.IntStream.range;
import static org.junit.jupiter.api.Assertions.*;

class RowBucketTest {
    private static final Term i1 = Term.typed("1", SharedRopes.DT_integer);
    private static final Term i2 = Term.typed("2", SharedRopes.DT_integer);
    private static final Term i3 = Term.typed("3", SharedRopes.DT_integer);
    private static final Term i4 = Term.typed("4", SharedRopes.DT_integer);

    static Stream<Arguments> test() {
        return Stream.of(TermBatchType.TERM, CompressedBatchType.COMPRESSED).map(Arguments::arguments);
    }

    @ParameterizedTest @MethodSource
    <B extends Batch<B>, RB extends RowBucket<B, RB>>
    void test(BatchType<B> bt) {
        try (var bucketG = new Guard<RB>(this);
             var b12TG   = new BatchGuard<TermBatch>(this);
             var b__12TG = new BatchGuard<TermBatch>(this);
             var b1234TG = new BatchGuard<TermBatch>(this);
             var b3412TG = new BatchGuard<TermBatch>(this);
             var b12G    = new BatchGuard<B>(this);
             var b__12G  = new BatchGuard<B>(this);
             var b1234G  = new BatchGuard<B>(this);
             var b3412G  = new BatchGuard<B>(this)) {
            var b12T   = b12TG.set(TermBatch.of(List.of(i1, i2)));
            var b__12T = b__12TG.set(TermBatch.of(asList(null, null), List.of(i1, i2)));
            var b1234T = b1234TG.set(TermBatch.of(List.of(i1, i2), List.of(i3, i4)));
            var b3412T = b3412TG.set(TermBatch.of(List.of(i3, i4), List.of(i1, i2)));
            @SuppressWarnings("unchecked")
            RB bucket = bucketG.set((Orphan<RB>)bt.createBucket(57, 2));
            assertTrue(bucket.capacity() <= 128);
            assertTrue(bucket.capacity() >= 57);
            // else: other implementations hold a Batch<?>, which may be bigger due to offerToNearest()
            bucket.grow(128 - bucket.capacity());
            assertEquals(128, bucket.capacity());
            assertEquals(List.of(), range(0, bucket.capacity()).filter(bucket::has).boxed().toList());

            B b12   = b12G  .set(bt.convertOrCopy(b12T  ));
            B b__12 = b__12G.set(bt.convertOrCopy(b__12T));
            B b1234 = b1234G.set(bt.convertOrCopy(b1234T));
            B b3412 = b3412G.set(bt.convertOrCopy(b3412T));
            bucket.set(1, b12, 0);
            assertTrue(bucket.equals(1, b12, 0));
            assertTrue(bucket.equals(1, b__12, 1));
            assertEquals(b12.hash(0), bucket.hashCode(1));

            bucket.set(0, 1);
            assertTrue(bucket.equals(0, b12, 0));
            assertTrue(bucket.equals(0, b__12, 1));
            assertTrue(bucket.equals(1, b12, 0));
            assertTrue(bucket.equals(1, b__12, 1));
            assertEquals(bucket.hashCode(1), bucket.hashCode(0));

            bucket.set(bucket.capacity() - 1, b3412, 0);
            bucket.set(bucket.capacity() - 2, b3412, 1);
            assertTrue(bucket.equals(bucket.capacity() - 1, b1234, 1));
            assertTrue(bucket.equals(bucket.capacity() - 2, b12, 0));
            assertEquals(b3412.hash(0), bucket.hashCode(bucket.capacity() - 1));
            assertEquals(b3412.hash(1), bucket.hashCode(bucket.capacity() - 2));

            bucket.set(0, b__12, 1);
            bucket.set(1, b__12, 0);
            assertTrue(bucket.equals(0, b12, 0));
            assertTrue(bucket.equals(1, b__12, 0));
            assertFalse(bucket.equals(0, b__12, 0));
            assertFalse(bucket.equals(31, b__12, 0)); // ambiguous: row of nulls or unset?
            assertFalse(bucket.equals(31, b__12, 0)); // ambiguous: row of nulls or unset?
            assertEquals(b__12.hash(0), bucket.hashCode(1));
            assertEquals(b__12.hash(1), bucket.hashCode(0));
        }
    }

}