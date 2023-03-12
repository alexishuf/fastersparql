package com.github.alexishuf.fastersparql.fed.selectors;

import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import com.github.alexishuf.fastersparql.exceptions.BadSerializationException;
import com.github.alexishuf.fastersparql.fed.Selector;
import com.github.alexishuf.fastersparql.fed.Spec;
import com.github.alexishuf.fastersparql.model.row.RowType;
import com.github.alexishuf.fastersparql.operators.plan.TriplePattern;
import com.github.alexishuf.fastersparql.sparql.OpaqueSparqlQuery;
import com.github.alexishuf.fastersparql.sparql.expr.Term;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import static com.github.alexishuf.fastersparql.fed.selectors.IriImmutableSet.EMPTY;
import static java.nio.charset.StandardCharsets.UTF_8;

public class DictionarySelector extends Selector {
    public static final String NAME = "dictionary";
    private static final byte[] TYPE_LINE_U8 = (NAME+'\n').getBytes(UTF_8);

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
        boolean fetchPredicates = spec.getOr("fetch-predicates", true);
        boolean fetchClasses = spec.getBool("fetch-classes");
        Thread.startVirtualThread(() -> init(client, fetchPredicates, fetchClasses));
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
            Thread.currentThread().setName("DictionarySelector.init("+client+")");
            predicates = fetchPredicates ? fetch(client, PREDICATES) : EMPTY;
            classes    = fetchClasses    ? fetch(client, CLASSES)    : EMPTY;
            notifyInit(InitOrigin.QUERY, null);
        } catch (Throwable t) {
            notifyInit(null, t);
        }
    }

    private IriImmutableSet fetch(SparqlClient client, OpaqueSparqlQuery query) {
        List<Term> iris = new ArrayList<>();
        try (var it = client.query(RowType.ARRAY, query)) {
            int n;
            for (var b = it.nextBatch(); (n = b.size) > 0; b = it.nextBatch(b)) {
                Term[][] a = b.array;
                for (int i = 0; i < n; i++) {
                    Term term = a[i][0];
                    if (term != null && term.isIri())
                        iris.add(term);
                }
            }
        }
        return new IriImmutableSet(iris);
    }

    @Override public void close() { }

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
