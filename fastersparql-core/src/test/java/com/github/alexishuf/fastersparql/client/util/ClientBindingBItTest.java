package com.github.alexishuf.fastersparql.client.util;

import com.github.alexishuf.fastersparql.client.ResultsSparqlClient;
import com.github.alexishuf.fastersparql.model.BindType;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.sparql.OpaqueSparqlQuery;
import com.github.alexishuf.fastersparql.util.Results;
import org.junit.jupiter.api.RepeatedTest;

import java.util.List;

import static com.github.alexishuf.fastersparql.model.BindType.*;
import static com.github.alexishuf.fastersparql.util.Results.results;

class ClientBindingBItTest {
    private final OpaqueSparqlQuery SPARQL = new OpaqueSparqlQuery("SELECT * WHERE { ?x a ?y } ");

    private ResultsSparqlClient.BoundAnswersStage1 client(Results expected) {
        //noinspection resource
        return new ResultsSparqlClient(false)
                .answerWith(SPARQL, expected)
                .forBindings(SPARQL, Vars.of("x"), Vars.of("y"));
    }

    @RepeatedTest(10)
    void testJoin() {
        Results expected = results("?x", "?y",
                "1", "11",
                "3", "31",
                "3", "32"
        ).query(SPARQL).bindings("?x", "1", "2", "3");
        try (var client = client(expected)
                .answer("1").with("11")
                .answer("2").with()
                .answer("3").with("31", "32").end()) {
            expected.check(client);
        }
    }

    @RepeatedTest(10)
    void testLeftJoin() {
        var expected = results("?x", "?y",
                "1", null,
                "2", "21",
                "2", "22",
                "3", "31"
        ).query(SPARQL).bindType(LEFT_JOIN);
        try (var client = client(expected)
                .answer("1").with()
                .answer("2").with("21", "22")
                .answer("3").with("31").end()) {
            expected.check(client);
        }
    }

    @RepeatedTest(10)
    void testExists() {
        var expected = results("?x", "1", "3").query(SPARQL).bindType(EXISTS);
        try (var client = client(expected)
                .answer("1").with("11")
                .answer("2").with()
                .answer("3").with("31", "32").end()) {
            expected.check(client);
        }
    }

    @RepeatedTest(10)
    void testNotExistsAndMinus() {
        for (BindType type : List.of(NOT_EXISTS, MINUS)) {
            var expected = results("?x", "2").query(SPARQL).bindType(type);
            try (var client = client(expected)
                    .answer("1").with("11")
                    .answer("2").with()
                    .answer("3").with("31", "32").end()) {
                expected.check(client);
            }
        }
    }
}