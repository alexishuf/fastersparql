package com.github.alexishuf.fastersparql.client.model.batch.operators;

import com.github.alexishuf.fastersparql.client.model.batch.BIt;
import com.github.alexishuf.fastersparql.client.model.batch.adapters.*;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.client.model.batch.adapters.BatchItDrainer.byItem;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public abstract class AbstractMergeBItTest extends AbstractBItTest {
    protected abstract static class ItGenerator {
        protected String name(int begin, int end, @Nullable Throwable error) {
            String errorString = error == null ? "" : "!" + error.getClass().getSimpleName();
            return String.format("[%d:%d]%s", begin, end, errorString);
        }
        public BIt<Integer> get(int begin, int end) { return get(begin, end, null); }
        public abstract BIt<Integer> get(int begin, int end, @Nullable Throwable error);
    }

    protected static ItGenerator listGenerator = new ItGenerator() {
        @Override public String toString() { return "ListGenerator"; }
        @Override public BIt<Integer> get(int begin, int end, @Nullable Throwable error) {
            var list = IntStream.range(begin, end).boxed().toList();
            var it = ThrowingIterator.andThrow(list.iterator(), error);
            return new IteratorBIt<>(it, Integer.class, name(begin, end, error));
        }
    };

    protected static ItGenerator threadedGenerator = new ItGenerator() {
        @Override public String toString() { return "ThreadedGenerator"; }
        @Override public BIt<Integer> get(int begin, int end, @Nullable Throwable error) {
            var cb = new CallbackBIt<>(Integer.class, name(begin, end, error));
            Thread.ofVirtual().start(() -> {
                for (int i = begin; i < end; i++)
                    cb.feed(i);
                cb.complete(error);
            });
            return cb;
        }
    };

    protected static ItGenerator recursivePublisherGenerator = new ItGenerator() {
        @Override public String toString() { return "RecursivePublisherGenerator"; }
        @Override public BIt<Integer> get(int begin, int end, @Nullable Throwable error) {
            Flux<Integer> flux = Flux.range(begin, end - begin);
            if (error != null)
                flux = Flux.concat(flux, Mono.error(error));
            return new PublisherBIt<>(flux, Integer.class, name(begin, end, error));
        }
    };

    protected static ItGenerator offloadedPublisherGenerator = new ItGenerator() {
        @Override public String toString() { return "RecursivePublisherGenerator"; }
        @Override public BIt<Integer> get(int begin, int end, @Nullable Throwable error) {
            Flux<Integer> flux = Flux.range(begin, end - begin);
            if (error != null)
                flux = Flux.concat(flux, Mono.error(error));
            flux.subscribeOn(Schedulers.boundedElastic()).publishOn(Schedulers.boundedElastic());
            return new PublisherBIt<>(flux, Integer.class, name(begin, end, error));
        }
    };

    protected static final List<ItGenerator> generators = List.of(
            listGenerator,
            threadedGenerator,
            recursivePublisherGenerator,
            offloadedPublisherGenerator
    );

    protected static final class MergeScenario extends Scenario {
        private final List<BIt<Integer>> operands;
        private final List<Integer> expected;

        public MergeScenario(Scenario other, List<BIt<Integer>> operands,
                             List<Integer> expected) {
            super(other);
            this.operands = operands;
            this.expected = expected;
        }

        public List<BIt<Integer>> operands() { return operands; }
        public List<Integer>          expected() { return expected; }

        @Override public String toString() {
            return "AbstractMergeScenario{"+", size="+size+", minBatch="+minBatch
                    +", maxBatch="+maxBatch+", drainer="+drainer+", error="+error
                    +", operands="+operands+", expected="+expected+'}';
        }

        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof MergeScenario that)) return false;
            if (!super.equals(o)) return false;
            return operands.equals(that.operands) && expected.equals(that.expected);
        }

        @Override public int hashCode() {
            return Objects.hash(super.hashCode(), operands, expected);
        }
    }

    @Override protected List<? extends Scenario> scenarios() {
        List<MergeScenario> scenarios = new ArrayList<>();

        // no-operand
        scenarios.add(new MergeScenario(new Scenario(0, 1, 1,
                                                     byItem(), null),
                                        List.of(), List.of()));
        //single operand
        for (ItGenerator generator : generators) {
            for (Scenario base : baseScenarios()) {
                var operands = List.of(generator.get(0, base.size(), base.error()));
                scenarios.add(new MergeScenario(base, operands, base.expected()));
            }
        }

        // long chain of operators using all generators
        for (Integer step : List.of(0, 1, 2, 3, 7)) {
            for (BatchSizes bSizes : batchSizes()) {
                for (BatchItDrainer drainer : BatchItDrainer.all()) {
                    for (Exception error : Arrays.asList(null, new Exception("test"))) {
                        int nOperands = 3*generators.size();
                        List<BIt<Integer>> operands = new ArrayList<>();
                        List<Integer> expected = new ArrayList<>();
                        for (int i = 0, start = 0; i < nOperands; i++) {
                            Throwable opError = i == nOperands - 1 ? error : null;
                            operands.add(generators.get(i % generators.size())
                                    .get(start, step, opError));
                            IntStream.range(start, start+step).forEach(expected::add);
                        }
                        var base = new Scenario(step, bSizes.min(), bSizes.max(), drainer, error);
                        scenarios.add(new MergeScenario(base, operands, expected));
                    }
                }
            }
        }

        //test change in sizes of operators
        for (BatchItDrainer drainer : BatchItDrainer.all()) {
            for (BatchSizes bs : batchSizes()) {
                List<BIt<Integer>> ops = new ArrayList<>();
                ops.add(threadedGenerator.get(0, 4, null));          // [0,1,2,3]
                ops.add(offloadedPublisherGenerator.get(4, 4, null));// []
                ops.add(recursivePublisherGenerator.get(4, 5, null));// [4]
                ops.add(listGenerator.get(5, 7, null));              // [5,6]
                var expected = IntStream.range(0, 7).boxed().toList();
                var base = new Scenario(4, bs.min(), bs.max(), drainer, null);
                scenarios.add(new MergeScenario(base, ops, expected));
            }
        }

        return scenarios;
    }

    static Stream<Arguments> selfTestGenerator() {
        List<Arguments> list = new ArrayList<>();
        for (var gen : generators) {
            for (BatchGetter getter : BatchGetter.all()) {
                for (Integer minBatch : List.of(1, 2)) {
                    for (Number minWait : List.of(0, 1_000_000_000L)) {
                        for (var range : List.of(List.of(0, 1), List.of(0, 2), List.of(1, 5),
                                List.of(0, 0), List.of(5, 5))) {
                            int begin = range.get(0), end = range.get(1);
                            var expected = IntStream.range(begin, end).boxed().toList();
                            list.add(arguments(gen, begin, end, getter,
                                               minBatch, minWait, expected));
                        }
                    }
                }
            }
        }
        return list.stream();
    }

    @ParameterizedTest @MethodSource
    void selfTestGenerator(ItGenerator generator, int begin, int end, BatchGetter getter,
                           int minBatch, long minWait, List<Integer> expected) {
        var it = generator.get(begin, end, null)
                          .minBatch(minBatch).minWait(minWait, TimeUnit.NANOSECONDS);
        var actual = new ArrayList<Integer>();
        for (var batch = getter.getBatch(it); !batch.empty(); batch = getter.getBatch(it))
            batch.drainTo(actual);
        assertEquals(expected, actual);
    }


    protected static Stream<Arguments> generatorData() {
        List<Arguments> list = new ArrayList<>();
        for (ItGenerator gen : generators) {
            for (BatchGetter getter : BatchGetter.all())
                list.add(arguments(gen, getter));
        }
        return list.stream();
    }

    @ParameterizedTest @MethodSource("generatorData")
    void testDoNotSplitBatches(ItGenerator gen, BatchGetter getter) {
        int delay = 10;
        //noinspection resource
        var sources = List.of(gen.get(0, 2).minBatch(2).maxBatch(2),
                gen.get(5, 5).minBatch(3).maxBatch(3),
                gen.get(10, 12).minBatch(2).maxBatch(2));
        try (MergeBIt<Integer> it = new MergeBIt<>(sources, Integer.class)) {
            it.minBatch(2).minWait(delay, TimeUnit.MILLISECONDS);
            List<List<Integer>> batches = new ArrayList<>();
            for (var batch = getter.getList(it); !batch.isEmpty(); batch = getter.getList(it))
                batches.add(batch);
            for (int i = 0; i < 3; i++)
                assertEquals(List.of(), getter.getList(it));
            assertTrue(batches.stream().allMatch(l -> l.size() >= 2));
        }
    }

}
