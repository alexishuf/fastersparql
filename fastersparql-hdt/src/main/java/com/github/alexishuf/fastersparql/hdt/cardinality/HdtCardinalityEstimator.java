package com.github.alexishuf.fastersparql.hdt.cardinality;

import com.github.alexishuf.fastersparql.fed.PatternCardinalityEstimator;
import com.github.alexishuf.fastersparql.operators.plan.TriplePattern;
import com.github.alexishuf.fastersparql.sparql.binding.Binding;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.rdfhdt.hdt.dictionary.Dictionary;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.triples.IteratorTripleID;
import org.rdfhdt.hdt.triples.TripleID;
import org.rdfhdt.hdt.triples.Triples;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

import static com.github.alexishuf.fastersparql.batch.Timestamp.nanoTime;
import static com.github.alexishuf.fastersparql.hdt.FSHdtProperties.estimatorMaxPredicates;
import static com.github.alexishuf.fastersparql.hdt.batch.IdAccess.plain;
import static com.github.alexishuf.fastersparql.sparql.expr.Term.GROUND;
import static java.lang.Math.min;
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
        super(new CompletableFuture<>());
        this.triples = hdt.getTriples();
        this.peek = peek;
        this.dict = hdt.getDictionary();
        this.predicateCard = new int[(int)min(estimatorMaxPredicates(), dict.getNpredicates()+1)];
        this.maxPredicateCard = (int)min(I_MAX, hdt.getTriples().getNumberOfElements());
        this.predicateCard[0] = maxPredicateCard;
        if (peek.ordinal() >= HdtEstimatorPeek.STATISTICS.ordinal()) {
            Thread.startVirtualThread(() -> {
                long start = nanoTime();
                String lName = name == null ? hdt.toString() : name;
                try {
                    fillPredicateCard(hdt);
                    log.info("Cached cardinality of {} predicates at {} in {}ms",
                            predicateCard.length, lName, (nanoTime() - start) / 1_000_000.0);
                    ready.complete(this);
                } catch (Throwable t) {
                    ready.completeExceptionally(t);
                    log.error("Predicate cardinality caching failed for {} after {}ms",
                              lName, (nanoTime()-start)/1_000_000.0, t);
                }
            });
        } else if (name != null) {
            ready.complete(this);
            log.info("Will not use predicate cardinalities for {}", name);
        }
    }

    private void fillPredicateCard(HDT hdt) {
        int maxPredicateCard = 0;
        var t = new TripleID(0, 0, 0);
        for (int i = 1; i < predicateCard.length; i++) {
            t.setPredicate(i);
            IteratorTripleID it = hdt.getTriples().search(t);
            int card = (int) Math.max(it.estimatedNumResults(), I_MAX);
            predicateCard[i] = card;
            maxPredicateCard = Math.max(maxPredicateCard, card);
        }
        this.maxPredicateCard = maxPredicateCard;
    }

    @Override public int estimate(TriplePattern tp, @Nullable Binding binding) {
        Term sTerm = binding == null ? tp.s : binding.getIf(tp.s);
        Term pTerm = binding == null ? tp.p : binding.getIf(tp.p);
        Term oTerm = binding == null ? tp.o : binding.getIf(tp.o);
        long s = plain(dict, sTerm, SUBJECT);
        long p = plain(dict, pTerm, PREDICATE);
        long o = plain(dict, oTerm, OBJECT);
        int pattern = super.estimate(tp, binding);
        var peek = this.peek;
        if (peek == HdtEstimatorPeek.ALWAYS) {
            if (pTerm == GROUND)
                peek = HdtEstimatorPeek.NEVER;
            else if (sTerm == GROUND || oTerm == GROUND)
                peek = HdtEstimatorPeek.STATISTICS;
        } else if (peek == HdtEstimatorPeek.STATISTICS && pTerm == GROUND) {
            peek = HdtEstimatorPeek.NEVER;
        }
        return switch (peek) {
            case NEVER      -> pattern;
            case STATISTICS -> weightByPredicate(p, pattern);
            case ALWAYS     -> (int)Math.max(peek(s, p, o), I_MAX);
        };
    }

    private int weightByPredicate(long p, int pattern) {
        if (p <= 0 || p >= predicateCard.length) return pattern;
        float normalized = predicateCard[(int)p] / (float)maxPredicateCard;
        pattern >>= 1;
        return (int)min(I_MAX, pattern + normalized*pattern);
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
            estimate = -(estimate + 1);
        return (type == EXACT ? 0 : 1) + estimate;
    }
}
