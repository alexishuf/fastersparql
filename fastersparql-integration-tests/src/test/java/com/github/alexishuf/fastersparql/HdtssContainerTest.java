package com.github.alexishuf.fastersparql;

import com.github.alexishuf.fastersparql.model.rope.ByteRope;
import com.github.alexishuf.fastersparql.model.row.RowType;
import com.github.alexishuf.fastersparql.sparql.OpaqueSparqlQuery;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
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
        var sparql = new OpaqueSparqlQuery(new ByteRope("SELECT * WHERE {?x <http://xmlns.com/foaf/0.1/age> 23.}"));
        try (var client = FS.clientFor(container.asEndpoint())) {
            var actual = client.query(RowType.LIST, sparql).toSet();
            var expected = Set.of(List.of(Term.valueOf("<http://example.org/Alice>")),
                                  List.of(Term.valueOf("<http://example.org/Eric>")));
            assertEquals(expected, actual);
        }
    }



}