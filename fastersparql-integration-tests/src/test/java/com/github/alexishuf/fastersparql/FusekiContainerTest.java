package com.github.alexishuf.fastersparql;

import com.github.alexishuf.fastersparql.client.FS;
import com.github.alexishuf.fastersparql.client.model.SparqlConfiguration;
import com.github.alexishuf.fastersparql.client.model.row.types.ListRow;
import com.github.alexishuf.fastersparql.sparql.OpaqueSparqlQuery;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Testcontainers
class FusekiContainerTest {
    private static final Logger log = LoggerFactory.getLogger(FusekiContainerTest.class);

    @Container
    private static final FusekiContainer FUSEKI
            = new FusekiContainer(FusekiContainerTest.class, "client/data.ttl", log);

    @Test
    void test() {
        try (var client = FS.clientFor(FUSEKI.asEndpoint(SparqlConfiguration.EMPTY), ListRow.STRING)) {
            var sparql = new OpaqueSparqlQuery("SELECT * WHERE { ?x <http://xmlns.com/foaf/0.1/age> 23 }");
            var actual = client.query(sparql).toSet();
            var expected = Set.of(List.of("<http://example.org/Alice>"),
                                  List.of("<http://example.org/Eric>"));
            assertEquals(expected, actual);
        }
    }



}