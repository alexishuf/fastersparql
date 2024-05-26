package com.github.alexishuf.fastersparql.fed;

import com.github.alexishuf.fastersparql.FS;
import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.client.model.SparqlConfiguration;
import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import com.github.alexishuf.fastersparql.client.model.SparqlMethod;
import com.github.alexishuf.fastersparql.exceptions.BadSerializationException;
import com.github.alexishuf.fastersparql.model.MediaType;
import com.github.alexishuf.fastersparql.model.SparqlResultFormat;
import com.github.alexishuf.fastersparql.util.NamedService;
import com.github.alexishuf.fastersparql.util.NamedServiceLoader;
import com.github.alexishuf.fastersparql.util.SafeCloseable;
import org.tomlj.TomlTable;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static com.github.alexishuf.fastersparql.util.ExceptionCondenser.closeAll;

@SuppressWarnings("ClassCanBeRecord")
public final class Source implements SafeCloseable {
    /* --- --- --- keys dictionary --- --- --- */
    public static final String TYPE = "type";
    public static final String SPARQL_TYPE = "sparql";
    public static final String CLIENT_TAG = "client-tag";
    public static final String SELECTOR = "selector";
    public static final String ESTIMATOR = "estimator";
    public static final String NAME = "name";
    public static final String URL = "url";
    public static final String CONFIG = "config";
    public static final String METHODS = "methods";
    public static final String RESULTS_ACCEPTS = "results-accepts";
    public static final String RDF_ACCEPTS = "rdf-accepts";
    public static final String PARAMS = "params";
    public static final String HEADERS = "headers";
    public static final String APPEND_HEADERS = "append-headers";

    /* --- --- --- fields --- --- --- */

    public  final SparqlClient         client;
    public  final Selector             selector;
    public  final CardinalityEstimator estimator;
    private final Spec                 spec;

    /* --- --- --- lifecycle --- --- --- */

    public Source(SparqlClient client, Selector selector,
                  CardinalityEstimator estimator, Spec spec) {
        this.client = client;
        this.selector = selector;
        this.estimator = estimator;
        this.spec = spec;
    }

    @Override public void close() {
        closeAll(List.of(client, selector));
    }

    /* --- --- --- accessors --- --- --- */

    public SparqlClient         client()    { return client; }
    public Selector             selector()  { return selector; }
    public CardinalityEstimator estimator() { return estimator; }
    public Spec                 spec()      { return spec; }

    /* --- --- --- static factory methods --- --- --- */

    public static  Source load(TomlTable tomlSpec, Path refDir) throws IOException {
        Spec spec = new Spec(tomlSpec);
        spec.set(Spec.PATHS_RELATIVE_TO, refDir);
        return load(spec);
    }
    public static  Source load(Spec spec) throws IOException {
        return NSL.get(spec.getString("type")).load(spec);
    }

    /* --- --- --- loaders --- --- --- */
    public interface Loader extends NamedService<String> {
         Source load(Spec spec) throws IOException;
    }

    private static final NamedServiceLoader<Loader, String> NSL
            = new NamedServiceLoader<>(Loader.class) {
        @Override protected Loader fallback(String name) {
            throw new BadSerializationException.UnknownSource(name);
        }
    };

    public static class SparqlSourceLoader implements Loader {
        @Override public  Source load(Spec sourceSpec) throws IOException {
            var ep = new SparqlEndpoint(readUrl(sourceSpec), readSparqlConfiguration(sourceSpec));
            String tag = sourceSpec.getString(CLIENT_TAG);
            var client = FS.clientFor(ep, tag);
            var selectorSpec = sourceSpec.getOr(SELECTOR, Spec.EMPTY);
            var selector = Selector.load(client, selectorSpec);
            var estimatorSpec = sourceSpec.getOr(ESTIMATOR, Spec.EMPTY);
            var estimator = CardinalityEstimator.load(client, estimatorSpec);
            return new Source(client, selector, estimator, sourceSpec);
        }
        @Override public String name() { return SPARQL_TYPE; }
    }

    /* --- --- --- loader helpers --- --- --- */

    public static String readUrl(Spec sourceSpec) {
        String url = sourceSpec.getString(URL);
        String uri = sourceSpec.getString("uri");
        String iri = sourceSpec.getString("iri");
        if (url == null) {
            if ((url = uri) == null) {
                url = iri;
            } else if (iri != null && !iri.equalsIgnoreCase(url)) {
                throw new BadSerializationException("Conflicting uri/iri keys");
            }
        } else if (uri != null && !uri.equalsIgnoreCase(url)) {
            throw new BadSerializationException("Conflicting url/uri keys");
        } else if (iri != null && !iri.equalsIgnoreCase(url)) {
            throw new BadSerializationException("Conflicting url/iri keys");
        }
        return url;
    }

    public static SparqlConfiguration readSparqlConfiguration(Spec sourceSpec) {
        Spec config = sourceSpec.get(CONFIG, Spec.class);
        if (config == null) return SparqlConfiguration.EMPTY;
        var b = SparqlConfiguration.builder();

        var methods = config.getListOf(METHODS, SparqlMethod.class, "method");
        if (!methods.isEmpty())
            b.methods(methods);
        var resultAccepts = config.getListOf(RESULTS_ACCEPTS, SparqlResultFormat.class, "results-accept");
        if (!resultAccepts.isEmpty())
            b.resultsAccepts(resultAccepts);
        var rdfAccepts = config.getListOf(RDF_ACCEPTS, MediaType.class, "rdf-accept");
        if (!rdfAccepts.isEmpty())
            b.rdfAccepts(rdfAccepts);

        Spec params = config.getOr(PARAMS, Spec.EMPTY);
        for (String name : params.keys())
            b.param(name, params.getListOf(name, String.class));
        Spec headers = config.getOr(HEADERS, Spec.EMPTY);
        for (String name : headers.keys())
            b.header(name, headers.getString(name));
        Spec appendHeaders = config.getOr(APPEND_HEADERS, Spec.EMPTY);
        for (String name : appendHeaders.keys())
            b.appendHeader(name, appendHeaders.getListOf(name, String.class));

        return b.build();
    }
}
