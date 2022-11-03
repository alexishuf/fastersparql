package com.github.alexishuf.fastersparql.batch.adapters;

import com.github.alexishuf.fastersparql.client.util.VThreadTaskSet;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.params.provider.Arguments;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import static java.util.stream.IntStream.range;

public abstract class AbstractBItTest {
    static Stream<Arguments> batchGetters() {
        return BatchGetter.all().stream().map(Arguments::arguments);
    }
    static Stream<Arguments> timingReliableBatchGetters() {
        return BatchGetter.all().stream().filter(BatchGetter::isTimingReliable)
                                         .map(Arguments::arguments);
    }

    public static class Scenario {
        protected final int size, minBatch, maxBatch;
        protected final BItDrainer drainer;
        protected final @Nullable Throwable error;

        public Scenario(int size, int minBatch, int maxBatch, BItDrainer drainer,
                        @Nullable Throwable error) {
            this.size = size;
            this.minBatch = minBatch;
            this.maxBatch = maxBatch;
            this.drainer = drainer;
            this.error = error;
        }

        public Scenario(Scenario other) {
            this(other.size, other.minBatch, other.maxBatch, other.drainer, other.error);
        }

        public int                 size()     { return size; }
        public int                 minBatch() { return minBatch; }
        public int                 maxBatch() { return maxBatch; }
        public BItDrainer          drainer()  { return drainer; }
        public @Nullable Throwable error()    { return error; }
        public List<Integer>       expected() { return range(0, size()).boxed().toList(); }

        @Override public String toString() {
            return "BaseScenario{"+"size=" + size+", minBatch=" + minBatch
                    +", maxBatch=" + maxBatch+", drainer=" + drainer+", error=" + error+'}';
        }

        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Scenario rhs)) return false;
            return size == rhs.size && minBatch == rhs.minBatch && maxBatch == rhs.maxBatch
                    && drainer.equals(rhs.drainer) && Objects.equals(error, rhs.error);
        }

        @Override public int hashCode() {
            return Objects.hash(size, minBatch, maxBatch, drainer, error);
        }
    }

    protected record BatchSizes(int min, int max) {}

    protected static List<BatchSizes> batchSizes() {
        return List.of(new BatchSizes(1, Integer.MAX_VALUE),
                       new BatchSizes(2, 2),
                       new BatchSizes(2, 32),
                       new BatchSizes(1, 3));
    }

    protected static List<Scenario> baseScenarios() {
        List<Scenario> list = new ArrayList<>();
        for (Integer size : List.of(0, 1, 2, 4, 5, 6, 7, 8, 1024)) {
            for (BatchSizes batchSizes : batchSizes()) {
                int min = batchSizes.min(), max = batchSizes.max();
                if (max > 2 && max < Integer.MAX_VALUE && max > size)
                    continue; // skip since batch will never be filled
                for (Exception error : Arrays.asList(null, new Exception("on purpose"))) {
                    for (var d : BItDrainer.all())
                        list.add(new Scenario(size, min, max, d, error));
                }
            }
        }
        return list;
    }

    /** Get a list of {@link Scenario}s to execute. */
    protected abstract List<? extends Scenario> scenarios();
    /** Run the given scenario and make all required {@code assert*()} calls. */
    protected abstract void run(Scenario scenario);

    /** High-precision sleep */
    public static void busySleepMillis(double millis) {
        long end = System.nanoTime() + (long)Math.ceil(millis*1_000_000.0);
        while (System.nanoTime() < end)
            Thread.yield();
    }

    @RepeatedTest(3)
    void test() throws Exception {
        try (var tasks = new VThreadTaskSet(getClass().getSimpleName())) {
            scenarios().forEach(s -> tasks.add(() -> run(s)));
        }
    }
}
