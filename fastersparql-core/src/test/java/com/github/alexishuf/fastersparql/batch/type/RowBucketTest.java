package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.model.rope.RopeDict;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.stream.IntStream.range;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RowBucketTest {
    private static final Term i1 = Term.typed("1", RopeDict.DT_integer);
    private static final Term i2 = Term.typed("2", RopeDict.DT_integer);
    private static final Term i3 = Term.typed("3", RopeDict.DT_integer);
    private static final Term i4 = Term.typed("4", RopeDict.DT_integer);

    static Stream<Arguments> test() {
        return Stream.of(Batch.TERM, Batch.COMPRESSED).map(Arguments::arguments);
    }

    @ParameterizedTest @MethodSource() <B extends Batch<B>> void test(BatchType<B> bt) {
        RowBucket<B> bucket = bt.createBucket(77, 2);
        assertEquals(77, bucket.capacity());
        bucket.grow(128-bucket.capacity());
        assertEquals(128, bucket.capacity());
        assertEquals(List.of(), range(0, bucket.capacity()).filter(bucket::has).boxed().toList());

        B b12  = bt.convert(TermBatch.of(List.of(i1, i2)));
        B b__12 = bt.convert(TermBatch.of(asList(null, null), List.of(i1, i2)));
        B b1234  = bt.convert(TermBatch.of(List.of(i1, i2), List.of(i3, i4)));
        B b3412 = bt.convert(TermBatch.of(List.of(i3, i4), List.of(i1, i2)));
        bucket.set(1, b12, 0);
        assertTrue(bucket.equals(1, b12, 0));
        assertTrue(bucket.equals(1, b__12, 1));

        bucket.set(0, 1);
        assertTrue(bucket.equals(0, b12, 0));
        assertTrue(bucket.equals(0, b__12, 1));
        assertTrue(bucket.equals(1, b12, 0));
        assertTrue(bucket.equals(1, b__12, 1));

        bucket.set(bucket.capacity()-1, b3412, 0);
        bucket.set(bucket.capacity()-2, b3412, 1);
        assertTrue(bucket.equals(bucket.capacity()-1, b1234, 1));
        assertTrue(bucket.equals(bucket.capacity()-2, b12, 0));

        assertTrue(Objects.requireNonNull(bucket.batchOf(1)).equals(bucket.batchRow(1), List.of(i1, i2)));
        assertTrue(Objects.requireNonNull(bucket.batchOf(0)).equals(bucket.batchRow(1), List.of(i1, i2)));
        assertTrue(Objects.requireNonNull(bucket.batchOf(bucket.capacity()-1)).equals(bucket.batchRow(bucket.capacity()-1), List.of(i3, i4)));
        assertTrue(Objects.requireNonNull(bucket.batchOf(bucket.capacity()-2)).equals(bucket.batchRow(bucket.capacity()-2), List.of(i1, i2)));

        bucket.set(0, b__12, 1);
        bucket.set(1, b__12, 0);
        assertTrue(bucket.equals(0, b12, 0));
        assertTrue(bucket.equals(1, b__12, 0));
    }

}