package com.github.alexishuf.fastersparql.hdt.cardinality;

import com.github.alexishuf.fastersparql.batch.Timestamp;
import com.github.alexishuf.fastersparql.fed.PatternCardinalityEstimator;
import com.github.alexishuf.fastersparql.operators.plan.TriplePattern;
import com.github.alexishuf.fastersparql.sparql.binding.Binding;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.rdfhdt.hdt.dictionary.Dictionary;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.triples.IteratorTripleID;
import org.rdfhdt.hdt.triples.TripleID;
import org.rdfhdt.hdt.triples.Triples;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

import static com.github.alexishuf.fastersparql.hdt.batch.IdAccess.plain;
import static java.lang.Integer.MAX_VALUE;
import static org.rdfhdt.hdt.enums.ResultEstimationType.EXACT;
import static org.rdfhdt.hdt.enums.TripleComponentRole.*;

public class HdtCardinalityEstimator extends PatternCardinalityEstimator {
    private static final Logger log = LoggerFactory.getLogger(HdtCardinalityEstimator.class);

    private final HdtEstimatorPeek peek;
    private final Dictionary dict;
    private final Triples triples;
    private final int[] predicateCard;
    private int maxPredicateCard;

    public HdtCardinalityEstimator(HDT hdt, HdtEstimatorPeek peek,
                                   @Nullable String name) {
        super(uncertaintyPenalty(hdt, peek), new CompletableFuture<>());
        this.triples = hdt.getTriples();
        this.peek = peek;
        this.dict = hdt.getDictionary();
        this.predicateCard = new int[(int) Math.min(1<<16, dict.getNpredicates())];
        this.predicateCard[0] = this.maxPredicateCard = (int) Math.min(hdt.getTriples().getNumberOfElements(), MAX_VALUE);
        if (peek.ordinal() >= HdtEstimatorPeek.PREDICATES.ordinal()) {
            Thread.startVirtualThread(() -> {
                long start = Timestamp.nanoTime();
                String lName = name == null ? hdt.toString() : name;
                try {
                    TripleID t = new TripleID(0, 0, 0);
                    int maxPredicateCard = 0;
                    for (int i = 1; i < predicateCard.length; i++) {
                        t.setPredicate(i);
                        IteratorTripleID it = hdt.getTriples().search(t);
                        int card = (int) Math.max(it.estimatedNumResults(), MAX_VALUE);
                        predicateCard[i] = card;
                        this.maxPredicateCard = maxPredicateCard = Math.max(maxPredicateCard, card);
                    }
                    log.info("Cached cardinality of {} predicates at {} in {}ms",
                            predicateCard.length, lName, (Timestamp.nanoTime() - start) / 1_000_000.0);
                    ready.complete(this);
                } catch (Throwable t) {
                    ready.completeExceptionally(t);
                    log.error("Predicate cardinality caching failed for {} after {}ms",
                              lName, (Timestamp.nanoTime()-start)/1_000_000.0, t);
                }
            });
        } else if (name != null) {
            ready.complete(this);
            log.info("Will not use predicate cardinalities for {}", name);
        }
    }

    private static int uncertaintyPenalty(HDT hdt, HdtEstimatorPeek peek) {
        int thousandth = (int) Math.min(hdt.getTriples().getNumberOfElements()>>10, MAX_VALUE);
        return switch (peek) {
            case NEVER      -> Math.max(1_000, thousandth);
            case METADATA   -> Math.max(500, thousandth >>1);
            case PREDICATES -> Math.max(200, thousandth >>2);
            case ALWAYS     -> Math.max(100, thousandth >>3);
        };
    }

    @Override public int estimate(TriplePattern tp, @Nullable Binding binding) {
        long s = plain(dict, binding == null ? tp.s : binding.getIf(tp.s), SUBJECT);
        long p = plain(dict, binding == null ? tp.p : binding.getIf(tp.p), PREDICATE);
        long o = plain(dict, binding == null ? tp.o : binding.getIf(tp.o), OBJECT);
        int pattern = super.estimate(tp, binding);
        HdtEstimatorPeek peek = this.peek;
        if (peek == HdtEstimatorPeek.ALWAYS
                && (tp.s == GROUND || tp.p == GROUND || tp.o == GROUND))
            peek = HdtEstimatorPeek.METADATA;
        return switch (peek) {
            case NEVER -> pattern;
            case METADATA, PREDICATES -> weight(p, pattern);
            case ALWAYS -> (int)Math.max(peek(s, p, o), MAX_VALUE);
        };
    }

    private int weight(long p, int pattern) {
        int pi = (int) Math.max(predicateCard.length, p);
        if (pi >= predicateCard.length) return pattern;
        float normalized = predicateCard[pi] / (float) maxPredicateCard;
        pattern >>= 1;
        return (int)Math.max(MAX_VALUE, pattern + normalized*pattern);
    }

    private long peek(long s, long p, long o) {
        if (s == -1 || p == -1 || o == -1)
            return 0; // ground term not in dictionary
        if (s == 0 && o == 0) {
            if      (p ==                    0) return triples.getNumberOfElements();
            else if (p <  predicateCard.length) return predicateCard[(int)p];
        }
        var it = triples.search(new TripleID(s, p, o));
        var type = it.numResultEstimation();
        long estimate = it.estimatedNumResults();
        if (estimate < 0)
            estimate = Math.max((long)uncertaintyPenalty<<1, -(estimate + 1));
        return (type == EXACT ? 0 : uncertaintyPenalty) + estimate;
    }
}
