package com.github.alexishuf.fastersparql.client.util;

import com.github.alexishuf.fastersparql.client.BindType;
import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.client.model.*;
import com.github.alexishuf.fastersparql.client.model.row.impl.ListOperations;
import com.github.alexishuf.fastersparql.client.util.bind.SparqlClientBinder;
import com.github.alexishuf.fastersparql.client.util.reactive.FSPublisher;
import com.github.alexishuf.fastersparql.client.util.sparql.ListBinding;
import com.github.alexishuf.fastersparql.client.util.sparql.SparqlUtils;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.value.qual.MinLen;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

class SparqlClientBinderFactoryTest {

    private final String SPARQL = "SELECT * WHERE { ?x a ?y } ";
    private final ListBinding LIST_BINDING = ListBinding.wrap(singletonList("x"), singletonList("<xValue>"));
    private final String BOUND_SPARQL = SparqlUtils.bind(SPARQL, LIST_BINDING).toString();

    @SuppressWarnings("unchecked")
    private static class MockClient implements SparqlClient<List<String>, Object> {
        SparqlConfiguration expectConfiguration = SparqlConfiguration.builder()
                .clearMethods().method(SparqlMethod.POST).build();
        String expectSparql;
        List<List<String>> results;

        public MockClient(String expectSparql, List<List<String>> results) {
            this.expectSparql = expectSparql;
            this.results = results;
        }

        @Override public Class<List<String>> rowClass() {
            Class<?> cls = List.class;
            return (Class<List<String>>) cls;
        }
        @Override public Class<Object> fragmentClass() { return Object.class; }
        @Override public SparqlEndpoint endpoint() {
            return SparqlEndpoint.parse("http://example.org/sparql");
        }
        @Override
        public Results<List<String>> query(CharSequence sparql,
                                           @Nullable SparqlConfiguration configuration,
                                           @Nullable Results<List<String>> bindings,
                                           @Nullable BindType bindType) {
            assert bindings == null;
            assert bindType == null;
            assertEquals(expectSparql, sparql.toString());
            assertSame(expectConfiguration, configuration);
            List<@MinLen(1) String> vars = SparqlUtils.publicVars(sparql);
            FSPublisher<List<String>> publisher = FSPublisher.bindToAny(Flux.fromIterable(this.results));
            return new Results<>(vars, rowClass(), publisher);
        }
        @Override
        public Graph<Object> queryGraph(CharSequence sparql, @Nullable SparqlConfiguration configuration) {
            throw new UnsupportedOperationException();
        }
        @Override public void close() { }

    }

    private SparqlClientBinder<List<String>>
    createBinder(List<String> bindingVars, MockClient client, BindType join) {
        return new SparqlClientBinder<>(ListOperations.get(), bindingVars, client, SPARQL,
                client.expectConfiguration, join);
    }

    @Test
    void testJoin() {
        MockClient client = new MockClient(BOUND_SPARQL, singletonList(singletonList("<yValue>")));
        SparqlClientBinder<List<String>> binder = createBinder(singletonList("x"), client, BindType.JOIN);
        FSPublisher<List<String>> boundPublisher = binder.bind(singletonList("<xValue>"));
        List<List<String>> actual = Flux.from(boundPublisher).collectList().block();
        assertEquals(singletonList(asList("<xValue>", "<yValue>")), actual);
    }

    @Test
    void testLeftJoinMatching() {
        MockClient client = new MockClient(BOUND_SPARQL, singletonList(singletonList("<yValue>")));
        SparqlClientBinder<List<String>> binder = createBinder(singletonList("x"), client, BindType.LEFT_JOIN);
        FSPublisher<List<String>> boundPublisher = binder.bind(singletonList("<xValue>"));
        List<List<String>> actual = Flux.from(boundPublisher).collectList().block();
        assertEquals(singletonList(asList("<xValue>", "<yValue>")), actual);
    }

    @Test
    void testLeftJoinNotMatching() {
        MockClient client = new MockClient(BOUND_SPARQL, emptyList());
        SparqlClientBinder<List<String>> binder = createBinder(singletonList("x"), client, BindType.LEFT_JOIN);
        FSPublisher<List<String>> boundPublisher = binder.bind(singletonList("<xValue>"));
        List<List<String>> actual = Flux.from(boundPublisher).collectList().block();
        assertEquals(singletonList(asList("<xValue>", null)), actual);
    }

    @Test
    void testExistsMatches() {
        MockClient client = new MockClient(BOUND_SPARQL, singletonList(singletonList("<yValue>")));
        SparqlClientBinder<List<String>> binder = createBinder(singletonList("x"), client, BindType.EXISTS);
        assertEquals(singletonList("x"), binder.resultVars());

        FSPublisher<List<String>> bp1 = binder.bind(singletonList("<xValue>"));
        assertEquals(singletonList(singletonList("<xValue>")), Flux.from(bp1).collectList().block());

        client.results = emptyList();
        FSPublisher<List<String>> bp2 = binder.bind(singletonList("<xValue>"));
        assertEquals(emptyList(), Flux.from(bp2).collectList().block());
    }

    @Test
    void testNotExists() {
        MockClient client = new MockClient(BOUND_SPARQL, singletonList(singletonList("<yValue>")));
        SparqlClientBinder<List<String>> binder = createBinder(singletonList("x"), client, BindType.NOT_EXISTS);
        assertEquals(singletonList("x"), binder.resultVars());

        FSPublisher<List<String>> bp1 = binder.bind(singletonList("<xValue>"));
        assertEquals(emptyList(), Flux.from(bp1).collectList().block());

        client.results = emptyList();
        FSPublisher<List<String>> bp2 = binder.bind(singletonList("<xValue>"));
        assertEquals(singletonList(singletonList("<xValue>")), Flux.from(bp2).collectList().block());
    }

}