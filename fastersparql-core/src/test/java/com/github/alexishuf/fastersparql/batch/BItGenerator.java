package com.github.alexishuf.fastersparql.batch;

import com.github.alexishuf.fastersparql.batch.adapters.IteratorBIt;
import com.github.alexishuf.fastersparql.batch.base.SPSCBIt;
import com.github.alexishuf.fastersparql.batch.type.TermBatch;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Consumer;

import static com.github.alexishuf.fastersparql.batch.IntsBatch.*;
import static com.github.alexishuf.fastersparql.batch.type.Batch.TERM;

public sealed interface BItGenerator {
    /** Equivalent to {@link #asBIt(Consumer, RuntimeException, int...)} with {@code err=null}  */
    default BIt<TermBatch> asBIt(Consumer<BIt<TermBatch>> batchingSetup, int... ints) {
        return asBIt(batchingSetup, null, ints);
    }

    /** Equivalent to {@link #asBIt(Consumer, RuntimeException, int...)} with a
     *  no-op {@code batchingSetup} */
    default BIt<TermBatch> asBIt(@Nullable RuntimeException err, int... ints) {
        return asBIt(it -> {}, err, ints);
    }

    /** Equivalent to {@link #asBIt(Consumer, RuntimeException, int...)} with
     *  {@code err=null} and a no-op {@code batchingSetup} */
    default BIt<TermBatch> asBIt(int... ints) {
        return asBIt(it -> {}, null, ints);
    }

    /**
     * Create a {@link BIt} that will output the given ints, each int wrapped in a single
     * {@link TermBatch}, which may be merged with others, depending on the {@link BIt}
     * implementation.
     *
     * @param batchingSetup code that sets parameter in the returned {@link BIt} before the
     *                      iterator has access to the batches
     * @param err If not null, this will be thrown by the {@link BIt} (possibly wrapped in another)
     *            exception once the original items in {@code ints} have been exhausted
     * @param ints items to be iterated. Each {@code int i} will yield {@code intsBatch(i)},
     *             but note that such batches may be merged
     * @return a {@link BIt} that will output the given integers as single-column rows of
     *         {@code xsd:integer}s.
     */
    BIt<TermBatch> asBIt(Consumer<BIt<TermBatch>> batchingSetup, @Nullable RuntimeException err,
                         int... ints);

    /* --- --- --- singletons --- --- --- */

    BItGenerator IT_GEN = new IteratorBItGenerator();
    BItGenerator CB_GEN = new CallbackBItGenerator();
    List<BItGenerator> GENERATORS = List.of(IT_GEN, CB_GEN);

    /* --- --- --- implementations --- --- --- */

    final class IteratorBItGenerator implements BItGenerator {
        @Override public BIt<TermBatch> asBIt(Consumer<BIt<TermBatch>> batchingSetup,
                                              RuntimeException err,
                                              int... ints) {
            var it = new Iterator<Term>() {
                private int i = 0;
                @Override public boolean hasNext() {
                    return i < ints.length + (err == null ? 0 : 1);
                }
                @Override public Term next() {
                    if (!hasNext()) throw new NoSuchElementException();
                    if (i == ints.length)
                        throw err;
                    return term(ints[i++]);
                }
            };
            var bit = new IteratorBIt<>(it, TERM, IntsBatch.X) {
                @Override public String toString() {
                    return "IteratorBIt{err="+err+"}"+ Arrays.toString(ints);
                }
            };
            batchingSetup.accept(bit);
            return bit;
        }
        @Override public String toString() { return getClass().getSimpleName(); }
    }

    final class CallbackBItGenerator implements BItGenerator {
        static final class GeneratedSPSCBIt extends SPSCBIt<TermBatch> {
            private final int[] ints;
            public GeneratedSPSCBIt(int[] ints, int maxBatches) { super(TERM, X, maxBatches);
                this.ints = ints;
            }

            @Override public String toString() {
                return "SPSCBIt(" + Arrays.toString(ints) + ")";
            }
        }

        @Override public BIt<TermBatch> asBIt(Consumer<BIt<TermBatch>> batchingSetup,
                                              @Nullable RuntimeException err, int... ints) {
            var cb = new GeneratedSPSCBIt(ints, 4);
            batchingSetup.accept(cb);
            Thread.startVirtualThread(() -> {
                for (int i : ints)
                    TERM.recycle(cb.offer(intsBatch(i)));
                cb.complete(err);
            });
            return cb;
        }
        @Override public String toString() { return getClass().getSimpleName(); }
    }
}
