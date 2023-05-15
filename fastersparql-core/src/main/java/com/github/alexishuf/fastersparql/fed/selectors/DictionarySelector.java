package com.github.alexishuf.fastersparql.fed.selectors;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.TermBatch;
import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.exceptions.BadSerializationException;
import com.github.alexishuf.fastersparql.fed.Selector;
import com.github.alexishuf.fastersparql.fed.Spec;
import com.github.alexishuf.fastersparql.operators.plan.TriplePattern;
import com.github.alexishuf.fastersparql.sparql.OpaqueSparqlQuery;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import com.github.alexishuf.fastersparql.store.index.dict.Dict;
import com.github.alexishuf.fastersparql.store.index.dict.DictSorter;
import com.github.alexishuf.fastersparql.store.index.dict.LocalityStandaloneDict;
import com.github.alexishuf.fastersparql.util.IOUtils;
import com.github.alexishuf.fastersparql.util.concurrent.CheapThreadLocal;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.System.nanoTime;
import static java.nio.charset.StandardCharsets.UTF_8;

public class DictionarySelector extends Selector {
    private static final Logger log = LoggerFactory.getLogger(DictionarySelector.class);
    public static final String NAME = "dictionary";
    public static final String FETCH_PREDICATES = "fetch-predicates";
    public static final String FETCH_CLASSES = "fetch-classes";
    public static final String STATE_DIR = "dir";
    public static final List<String> STATE_DIR_P = List.of(STATE, STATE_DIR);

    private @Nullable LocalityStandaloneDict predicates, classes;
    private final CheapThreadLocal<Lookup> lookup = new CheapThreadLocal<>(Lookup::new);

    private class Lookup {
        final LocalityStandaloneDict.Lookup predicates, classes;

        public Lookup() {
            var p = DictionarySelector.this.predicates;
            var c = DictionarySelector.this.classes;
            predicates = p == null ? null : p.lookup();
            classes    = c == null ? null : c.lookup();
        }
    }

    public static class DictionaryLoader implements Loader  {
        @Override public String name() { return NAME; }

        @Override
        public Selector load(SparqlClient client, Spec spec) throws IOException, BadSerializationException {
            File dirFile = spec.getFile(STATE_DIR_P, null);
            if (dirFile == null)
                return new DictionarySelector(client, spec);
            Path dir = dirFile.toPath();
            Path path = dir.resolve("predicates");
            LocalityStandaloneDict predicates = null, classes = null;
            if (Files.exists(path)) {
                predicates = new LocalityStandaloneDict(path);
                try {
                    predicates.validate();
                } catch (IOException e) { throw new BadSerializationException(e.getMessage()); }
            }
            path = dir.resolve("classes");
            if (Files.exists(path)) {
                classes = new LocalityStandaloneDict(path);
                try {
                    classes.validate();
                } catch (IOException e) { throw new BadSerializationException(e.getMessage()); }
            }
            return new DictionarySelector(client, spec, predicates, classes);
        }
    }

    public DictionarySelector(SparqlClient client, Spec spec) {
        super(client.endpoint(), spec);
        this.predicates = this.classes = null;
        boolean fetchPredicates = spec.getOr(FETCH_PREDICATES, true);
        boolean fetchClasses = spec.getBool(FETCH_CLASSES);
        Thread.ofPlatform().name("DictionarySelector-init-"+endpoint)
              .start(() -> init(client, fetchPredicates, fetchClasses));
    }

    public DictionarySelector(SparqlClient client, Spec spec,
                              @Nullable LocalityStandaloneDict predicates,
                              @Nullable LocalityStandaloneDict classes) {
        super(client.endpoint(), spec);
        this.predicates = predicates;
        this.classes = classes;
        boolean fetchPredicates = spec.getOr(FETCH_PREDICATES, true) && predicates == null;
        boolean fetchClasses = spec.getOr(FETCH_CLASSES, false) && classes == null;
        if (fetchPredicates || fetchClasses) {
            Thread.ofPlatform().name("DictionarySelector-init-"+endpoint)
                    .start(() -> init(client, fetchPredicates, fetchClasses));
        } else {
            notifyInit(InitOrigin.LOAD, null);
        }
    }

    private static final OpaqueSparqlQuery PREDICATES = new OpaqueSparqlQuery("SELECT DISTINCT ?p WHERE {?s ?p ?o}");
    private static final OpaqueSparqlQuery CLASSES = new OpaqueSparqlQuery("SELECT DISTINCT ?c WHERE {?s a ?c}");

    private void init(SparqlClient client, boolean fetchPredicates, boolean fetchClasses) {
        try {
            long start = nanoTime();
            predicates = fetchPredicates ? fetch(client, PREDICATES) : null;
            classes    = fetchClasses    ? fetch(client, CLASSES)    : null;
            log.info("{} indexed {} predicates and {} classes in {}s",
                     DictionarySelector.this, predicates == null ? 0 : predicates.strings(),
                     classes == null ? 0 : classes.strings(),
                     String.format("%.3f", (nanoTime()-start)/1_000_000_000.0));
            notifyInit(InitOrigin.QUERY, null);
        } catch (Throwable t) {
            notifyInit(null, t);
        }
    }

    private LocalityStandaloneDict
    fetch(SparqlClient client, OpaqueSparqlQuery query) throws IOException {
        String what = query == PREDICATES ? "predicates" : (query == CLASSES) ? "classes" : "?";
        Path destFile;
        File dir = spec.getFile(STATE_DIR_P, null);
        if (dir == null) {
            destFile = Files.createTempFile("fastersparql-DictionarySelector", "." + what);
        } else {
            IOUtils.ensureDir(dir);
            destFile = dir.toPath().resolve(what);
        }
        Path tempDir = Files.createTempDirectory("fastersparql-DictionarySelector");
        AtomicInteger received = new AtomicInteger();
        Semaphore done = new Semaphore(0);
        Thread progress = Thread.startVirtualThread(() -> {
            try {
                while (!done.tryAcquire(4, TimeUnit.SECONDS)) {
                    log.info("{} loading {}: {}/?", this, what, received.getOpaque());
                }
            } catch (InterruptedException ignored) { }
        });
        try (DictSorter sorter = new DictSorter(tempDir, false, true);
             var it = client.query(Batch.TERM, query)) {
            for (TermBatch b = null; (b = it.nextBatch(b)) != null; ) {
                for (int r = 0, rows = b.rows; r < rows; r++) {
                    Term term = b.get(r, 0);
                    if (term != null && term.isIri()) {
                        received.getAndIncrement();
                        sorter.copy(term.first(), term.second());
                    }
                }
            }
            sorter.writeDict(destFile);
        } finally {
            done.release();
            try {
                progress.join();
            } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
        return new LocalityStandaloneDict(destFile);
    }

    @Override public void saveIfEnabled() {
        // do nothing since we always save during construction.
    }

    @Override public boolean has(TriplePattern tp) {
        var l = lookup.get();
        if (l.classes != null && tp.p == Term.RDF_TYPE && tp.o.isIri())
            return l.classes.find(tp.o) != Dict.NOT_FOUND;
        return l.predicates == null || l.predicates.find(tp.p) != Dict.NOT_FOUND;
    }
}
