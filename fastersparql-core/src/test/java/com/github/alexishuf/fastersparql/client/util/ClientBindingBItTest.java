package com.github.alexishuf.fastersparql.client.util;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.adapters.IteratorBIt;
import com.github.alexishuf.fastersparql.client.BindType;
import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.client.model.Graph;
import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import com.github.alexishuf.fastersparql.client.model.Vars;
import com.github.alexishuf.fastersparql.client.model.row.RowType;
import com.github.alexishuf.fastersparql.client.model.row.types.ListRow;
import com.github.alexishuf.fastersparql.client.util.bind.ClientBindingBIt;
import com.github.alexishuf.fastersparql.sparql.OpaqueSparqlQuery;
import com.github.alexishuf.fastersparql.sparql.SparqlQuery;
import com.github.alexishuf.fastersparql.sparql.binding.RowBinding;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.github.alexishuf.fastersparql.client.BindType.*;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertEquals;

class ClientBindingBItTest {

    private final OpaqueSparqlQuery SPARQL = new OpaqueSparqlQuery("SELECT * WHERE { ?x a ?y } ");

    private static class MockClient implements SparqlClient<List<String>, String, Object> {
        final OpaqueSparqlQuery sparql;
        SparqlQuery expectedSparql;
        final Vars boundVars;
        List<List<String>> results;

        public MockClient(OpaqueSparqlQuery sparql, Vars boundVars, List<List<String>> results) {
            this.expectedSparql = this.sparql = sparql;
            this.boundVars      = boundVars;
            this.results        = results;
        }

        public ClientBindingBIt<List<String>, String> bind(List<String> left, BindType type) {
            var binding = new RowBinding<>(ListRow.STRING, boundVars).row(left);
            this.expectedSparql = sparql.bind(binding);
            var leftIt = new IteratorBIt<>(List.of(left), List.class, boundVars);
            return new ClientBindingBIt<>(leftIt, type, ListRow.STRING,
                                          this, sparql);
        }

        @Override public RowType<List<String>, String> rowType() { return ListRow.STRING; }
        @Override public Class<Object> fragmentClass() { return Object.class; }
        @Override public SparqlEndpoint endpoint() {
            return SparqlEndpoint.parse("http://example.org/sparql");
        }
        @Override
        public BIt<List<String>> query(SparqlQuery sparql,
                                       @Nullable BIt<List<String>> bindings,
                                       @Nullable BindType bindType) {
            assert bindings == null;
            assert bindType == null;
            assertEquals(expectedSparql, sparql);
            return new IteratorBIt<>(results, List.class, sparql.publicVars());
        }
        @Override
        public Graph<Object> queryGraph(SparqlQuery ignored) {
            throw new UnsupportedOperationException();
        }
        @Override public void close() { }
    }

    @Test
    void testJoin() {
        try (var client = new MockClient(SPARQL, Vars.of("x"), List.of(List.of("<yValue>")))) {
            var actual = client.bind(List.of("<xValue>"), BindType.JOIN).toList();
            assertEquals(List.of(List.of("<xValue>", "<yValue>")), actual);
        }
    }

    @Test
    void testLeftJoinMatching() {
        try (var client = new MockClient(SPARQL, Vars.of("x"), List.of(List.of("<yValue>")))) {
            var it = client.bind(List.of("<xValue>"), LEFT_JOIN);
            assertEquals(Vars.of("x", "y"), it.vars());
            var actual = it.toList();
            assertEquals(List.of(List.of("<xValue>", "<yValue>")), actual);
        }
    }

    @Test
    void testLeftJoinNotMatching() {
        try (var client = new MockClient(SPARQL, Vars.of("x"), emptyList())) {
            var it = client.bind(List.of("<xValue>"), LEFT_JOIN);
            assertEquals(Vars.of("x", "y"), it.vars());
            var actual = it.toList();
            assertEquals(List.of(asList("<xValue>", null)), actual);
        }
    }

    @Test
    void testExistsMatches() {
        try (var client = new MockClient(SPARQL, Vars.of("x"), List.of(List.of("<yValue>")))) {
            var it = client.bind(List.of("<xValue>"), EXISTS);
            assertEquals(Vars.of("x"), it.vars());
            assertEquals(List.of(List.of("<xValue>")), it.toList());

            client.results = List.of();
            assertEquals(List.of(), client.bind(List.of("<xValue>"), EXISTS).toList());
        }
    }

    @Test
    void testNotExists() {
        try (var client = new MockClient(SPARQL, Vars.of("x"), List.of(List.of("<yValue>")))) {
            var it = client.bind(List.of("<xValue>"), NOT_EXISTS);
            assertEquals(Vars.of("x"), it.vars());
            assertEquals(List.of(), it.toList());

            client.results = List.of();
            var actual = client.bind(List.of("<xValue>"), NOT_EXISTS).toList();
            assertEquals(List.of(List.of("<xValue>")), actual);
        }
    }

}