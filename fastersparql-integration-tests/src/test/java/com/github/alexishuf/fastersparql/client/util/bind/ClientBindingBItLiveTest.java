package com.github.alexishuf.fastersparql.client.util.bind;

import com.github.alexishuf.fastersparql.FS;
import com.github.alexishuf.fastersparql.HdtssContainer;
import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.client.SparqlClientTest;
import com.github.alexishuf.fastersparql.client.model.SparqlConfiguration;
import com.github.alexishuf.fastersparql.client.model.SparqlMethod;
import com.github.alexishuf.fastersparql.model.BindType;
import com.github.alexishuf.fastersparql.model.SparqlResultFormat;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.operators.plan.ResultsIntegrationTest;
import com.github.alexishuf.fastersparql.util.AutoCloseableSet;
import com.github.alexishuf.fastersparql.util.Results;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.List;

import static com.github.alexishuf.fastersparql.util.Results.results;

@Testcontainers
class ClientBindingBItLiveTest extends ResultsIntegrationTest {
    private static final Logger log = LoggerFactory.getLogger(ClientBindingBItLiveTest.class);

    @Container private static final HdtssContainer hdtss
            = new HdtssContainer(SparqlClientTest.class, "data.hdt", log);


    @Override protected AutoCloseableSet<SparqlClient> createClients() {
        var cfg = SparqlConfiguration.builder()
                .clearMethods().method(SparqlMethod.POST)
                .clearResultsAccepts().resultsAccept(SparqlResultFormat.TSV).build();
        return AutoCloseableSet.of(FS.clientFor(hdtss.asEndpoint(cfg)));
    }

    @Override protected List<Results> data() {
        return List.of(
                /* no binding produces no results */
                results("?x").bindings(Vars.EMPTY).query("SELECT * WHERE {?x foaf:age 23}"),

                /* single empty binding produces 2 results */
                results("?x", ":Alice", ":Eric")
                        .bindings(Vars.EMPTY)
                        .query("SELECT * WHERE {?x foaf:age 23}"),

                /* single binding produces 1~2 rows + join/left join */
                results("?y", "?x", 23, ":Alice", 23, ":Eric")
                        .bindings("?y", 23)
                        .query("SELECT * WHERE { ?x foaf:age ?y}"),
                results("?y", "?x", 25, ":Bob")
                        .bindings("?y", 25).bindType(BindType.LEFT_JOIN)
                        .query("SELECT * WHERE { ?x foaf:age ?y }"),

                /* single binding produces no rows + left join */
                results("?y", "?x", 51, null)
                        .bindings("?y", 51).bindType(BindType.LEFT_JOIN)
                        .query("SELECT * WHERE { ?x foaf:age ?y }"),

                /* single binding produces no rows + (not) exists */
                results("?y")
                        .bindings("?y", 51).bindType(BindType.EXISTS)
                        .query("SELECT * WHERE { ?x foaf:age ?y }"),
                results("?y", 51)
                        .bindings("?y", 51).bindType(BindType.NOT_EXISTS)
                        .query("SELECT * WHERE { ?x foaf:age ?y }"),

                /* single binding produces two rows + (not) exists */
                results("?y", 23)
                        .bindings("?y", 23).bindType(BindType.EXISTS)
                        .query("SELECT * WHERE { ?x foaf:age ?y }"),
                results("?y")
                        .bindings("?y", 23).bindType(BindType.NOT_EXISTS)
                        .query("SELECT * WHERE { ?x foaf:age ?y }"),

                /* multiple bindings produce multiple results */
                results("?y", "?x", 23, ":Alice", 23, ":Eric", 25, ":Bob")
                        .bindings("?y", 23, 25)
                        .query("SELECT * WHERE { ?x foaf:age ?y }"),
                results("?x", "?y",
                        ":Alice", 23,
                        ":Bob", 25,
                        ":Charlie", null,
                        ":Dave", null,
                        ":Eric", 23)
                        .bindings("?x", ":Alice", ":Bob", ":Charlie", ":Dave", ":Eric")
                        .bindType(BindType.LEFT_JOIN)
                        .query("SELECT * WHERE { ?x foaf:age ?y }")
        );
    }
}