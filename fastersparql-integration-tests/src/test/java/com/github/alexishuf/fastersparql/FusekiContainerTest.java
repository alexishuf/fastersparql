package com.github.alexishuf.fastersparql;

import com.github.alexishuf.fastersparql.client.FasterSparql;
import com.github.alexishuf.fastersparql.client.util.reactive.IterableAdapter;
import lombok.val;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.HashSet;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toSet;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Testcontainers
class FusekiContainerTest {
    private static final Logger log = LoggerFactory.getLogger(FusekiContainerTest.class);

    @Container
    private static final FusekiContainer FUSEKI
            = new FusekiContainer(FusekiContainerTest.class, "client/data.ttl", log);

    @Test
    void test() {
        try (val client = FasterSparql.clientFor(FUSEKI.asEndpoint())) {
            val results = client.query("SELECT * WHERE { ?x <http://xmlns.com/foaf/0.1/age> 23 }");
            try (val adapter = new IterableAdapter<>(results.publisher())) {
                val ex = new HashSet<>(asList("<http://example.org/Alice>",
                                              "<http://example.org/Eric>"));
                assertEquals(ex, adapter.stream().map(r -> r[0]).collect(toSet()));
            }
        }
    }



}