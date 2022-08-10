package com.github.alexishuf.fastersparql;

import com.github.alexishuf.fastersparql.client.FasterSparql;
import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.util.reactive.IterableAdapter;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.HashSet;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Testcontainers
class HdtssContainerTest {
    private static final Logger log = LoggerFactory.getLogger(HdtssContainerTest.class);

    @Container
    private static final HdtssContainer container
            = new HdtssContainer(HdtssContainerTest.class, "client/data.hdt", log);

    @Test
    void test() {
        try (SparqlClient<String[], ?> client = FasterSparql.clientFor(container.asEndpoint())) {
            Results<String[]> results = client.query("SELECT * WHERE {?x <http://xmlns.com/foaf/0.1/age> 23.}");
            try (IterableAdapter<String[]> adapter = new IterableAdapter<>(results.publisher())) {
                HashSet<String> ex = new HashSet<>(asList("<http://example.org/Alice>",
                                                          "<http://example.org/Eric>"));
                assertEquals(ex, adapter.stream().map(r -> r[0]).collect(Collectors.toSet()));
            }
        }
    }



}