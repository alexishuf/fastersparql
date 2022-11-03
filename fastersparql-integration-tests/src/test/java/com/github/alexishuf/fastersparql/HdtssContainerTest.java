package com.github.alexishuf.fastersparql;

import com.github.alexishuf.fastersparql.client.FS;
import com.github.alexishuf.fastersparql.client.model.row.types.ListRow;
import com.github.alexishuf.fastersparql.sparql.SparqlQuery;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Testcontainers
class HdtssContainerTest {
    private static final Logger log = LoggerFactory.getLogger(HdtssContainerTest.class);

    @Container
    private static final HdtssContainer container
            = new HdtssContainer(HdtssContainerTest.class, "client/data.hdt", log);

    @Test
    void test() {
        var sparql = new SparqlQuery("SELECT * WHERE {?x <http://xmlns.com/foaf/0.1/age> 23.}");
        try (var client = FS.clientFor(container.asEndpoint(), ListRow.STRING)) {
            var actual = client.query(sparql).toSet();
            var expected = Set.of(List.of("<http://example.org/Alice>"),
                                  List.of("<http://example.org/Eric>"));
            assertEquals(expected, actual);
        }
    }



}