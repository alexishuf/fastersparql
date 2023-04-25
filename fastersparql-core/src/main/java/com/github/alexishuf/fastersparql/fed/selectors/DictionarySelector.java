package com.github.alexishuf.fastersparql.fed.selectors;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.TermBatch;
import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import com.github.alexishuf.fastersparql.exceptions.BadSerializationException;
import com.github.alexishuf.fastersparql.fed.Selector;
import com.github.alexishuf.fastersparql.fed.Spec;
import com.github.alexishuf.fastersparql.operators.plan.TriplePattern;
import com.github.alexishuf.fastersparql.sparql.OpaqueSparqlQuery;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static com.github.alexishuf.fastersparql.fed.selectors.IriImmutableSet.EMPTY;
import static java.lang.System.nanoTime;
import static java.nio.charset.StandardCharsets.UTF_8;

public class DictionarySelector extends Selector {
    private static final Logger log = LoggerFactory.getLogger(DictionarySelector.class);
    public static final String NAME = "dictionary";
    private static final byte[] TYPE_LINE_U8 = (NAME+'\n').getBytes(UTF_8);
    public static final String FETCH_PREDICATES = "fetch-predicates";
    public static final String FETCH_CLASSES = "fetch-classes";

    private IriImmutableSet predicates;
    private IriImmutableSet classes;

    public static class DictionaryLoader implements Loader  {
        @Override public String name() { return NAME; }

        @Override public Selector create(SparqlClient client, Spec spec) {
            return new DictionarySelector(client, spec);
        }

        @Override
        public Selector load(SparqlClient client, Spec spec, InputStream in) throws IOException, BadSerializationException {
            var predicates = IriImmutableSet.load(in);
            var classes = IriImmutableSet.load(in);
            return new DictionarySelector(client.endpoint(), spec, predicates, classes);
        }
    }

    public DictionarySelector(SparqlClient client, Spec spec) {
        super(client.endpoint(), spec);
        this.predicates = this.classes = EMPTY;
        boolean fetchPredicates = spec.getOr(FETCH_PREDICATES, true);
        boolean fetchClasses = spec.getBool(FETCH_CLASSES);
        Thread.ofPlatform().name("DictionarySelector-init-"+endpoint)
              .start(() -> init(client, fetchPredicates, fetchClasses));
    }

    public DictionarySelector(SparqlEndpoint endpoint, Spec spec,
                              IriImmutableSet predicates, IriImmutableSet classes) {
        super(endpoint, spec);
        this.predicates = predicates;
        this.classes = classes;
        notifyInit(InitOrigin.LOAD, null);
    }

    private static final OpaqueSparqlQuery PREDICATES = new OpaqueSparqlQuery("SELECT DISTINCT ?p WHERE {?s ?p ?o}");
    private static final OpaqueSparqlQuery CLASSES = new OpaqueSparqlQuery("SELECT DISTINCT ?c WHERE {?s a ?c}");

    private void init(SparqlClient client, boolean fetchPredicates, boolean fetchClasses) {
        try {
            long start = nanoTime();
            predicates = fetchPredicates ? fetch(client, PREDICATES) : EMPTY;
            classes    = fetchClasses    ? fetch(client, CLASSES)    : EMPTY;
            log.info("{} indexed {} predicates and {} classes in {}s",
                     DictionarySelector.this, predicates.size(), classes.size(),
                     String.format("%.3f", (nanoTime()-start)/1_000_000_000.0));
            notifyInit(InitOrigin.QUERY, null);
        } catch (Throwable t) {
            notifyInit(null, t);
        }
    }

    private IriImmutableSet fetch(SparqlClient client, OpaqueSparqlQuery query) {
        List<Term> iris = new ArrayList<>();
        Semaphore done = new Semaphore(0);
        Thread progress = Thread.startVirtualThread(() -> {
            String what = query == PREDICATES ? "predicates" : (query == CLASSES) ? "classes" : "?";
            try {
                while (!done.tryAcquire(4, TimeUnit.SECONDS)) {
                    log.info("{} loading {}: {}/?", this, what, iris.size());
                }
            } catch (InterruptedException ignored) {
            }
        });
        try (var it = client.query(Batch.TERM, query)) {
            for (TermBatch b = null; (b = it.nextBatch(b)) != null; ) {
                for (int r = 0, rows = b.rows; r < rows; r++) {
                    Term term = b.get(r, 0);
                    if (term != null && term.isIri())
                        iris.add(term);
                }
            }
        } finally {
            done.release();
            try {
                progress.join();
            } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
        return new IriImmutableSet(iris);
    }

    @Override public void save(OutputStream out) throws IOException {
        out.write(TYPE_LINE_U8);
        predicates.save(out);
        classes.save(out);
    }

    @Override public boolean has(TriplePattern tp) {
        if (tp.p == Term.RDF_TYPE && tp.o.isIri() && classes.size() > 0)
            return classes.has(tp.o);
        return predicates.has(tp.p);
    }
}
