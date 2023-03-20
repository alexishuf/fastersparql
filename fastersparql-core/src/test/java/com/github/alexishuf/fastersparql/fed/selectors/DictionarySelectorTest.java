package com.github.alexishuf.fastersparql.fed.selectors;

import com.github.alexishuf.fastersparql.fed.Selector;
import com.github.alexishuf.fastersparql.fed.Spec;
import com.github.alexishuf.fastersparql.sparql.OpaqueSparqlQuery;
import com.github.alexishuf.fastersparql.util.concurrent.Async;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static com.github.alexishuf.fastersparql.fed.Selector.InitOrigin.QUERY;
import static com.github.alexishuf.fastersparql.sparql.expr.Term.termList;
import static com.github.alexishuf.fastersparql.util.Results.results;
import static org.junit.jupiter.api.Assertions.assertEquals;

class DictionarySelectorTest extends SelectorTestBase {

    @Override protected Spec createSpec() {
        return Spec.of(Selector.TYPE, DictionarySelector.NAME, "fetch-classes", true);
    }

    @Test void test() throws IOException {
        // build the selector
        var predicates = new IriImmutableSet(termList("exns:p", "ex:p", "foaf:p"));
        var classes = new IriImmutableSet(termList("exns:A", "exns:AB", "ex:C", "foaf:Person"));
        Selector sel = saveAndLoad(new DictionarySelector(ENDPOINT, spec, predicates, classes));

        testTPs(sel, List.of(
                // query indexed predicates
                "?x123 exns:p ?y1",
                "?z ex:p ?y2",
                "?_w foaf:p ?x",

                // query indexed predicates, bound subject and object
                "exns:S exns:p ex:o",
                "ex:S ex:p ex:o",
                "ex:S foaf:p foaf:Person",

                // match by class
                "?s a exns:A",
                "?s a exns:AB",
                "?s a ex:C",
                "?s a foaf:Person",

                // match by class with ground subject
                "ex:S a exns:A",
                "exns:S a exns:AB",
                "ex:S a ex:C",
                "ex:S a foaf:Person"
        ), List.of(
                // negative predicate
                "?x exns:q ?y",
                "?x ex:q ?y",
                "?x foaf:q ?y",
                "?x exns:pp ?y",
                "?x ex:pp ?y",
                "?x foaf:pp ?y",
                "?x exns:p1 ?y",
                "?x ex:p1 ?y",
                "?x foaf:p1 ?y",
                "?x exns: ?y",
                "?x ex: ?y",
                "?x foaf: ?y",

                // negative predicate with ground subject and object
                "exns:p exns:q foaf:Person",
                "ex:p ex:q foaf:Person",
                "foaf:p foaf:q foaf:Person",

                // negative class
                "?x a exns:AA",
                "?x a exns:B",
                "?x a ex:A",
                "?x a ex:AB",
                "?x a foaf:Document",
                "?x a foaf:A",

                //negative class with ground subject
                "exns:A a exns:AA",
                "exns:AB a exns:BA",
                "ex:C a ex:A",
                "foaf:Person a foaf:Perso",
                "foaf:Person a foaf:Personx",
                "foaf:Person a foaf:Document"
        ));
    }

    @Test void testInit() throws IOException {
        spec.set("fetch-classes", true);
        client.answerWith(new OpaqueSparqlQuery("SELECT DISTINCT ?p WHERE {?s ?p ?o}"),
                          results("?p", "exns:p", "exns:p1", "ex:q", "rdf:type"));
        client.answerWith(new OpaqueSparqlQuery("SELECT DISTINCT ?c WHERE {?s a ?c}"),
                          results("?c", "ex:C", "foaf:Person"));
        var initSel = new DictionarySelector(client, spec);
        assertEquals(QUERY, Async.waitStage(initSel.initialization()));
        Selector sel = saveAndLoad(initSel);

        testTPs(sel,
                List.of(
                        "?s exns:p ?o",
                        "?s exns:p1 ?o",
                        "?s ex:q ?o",
                        "?s rdf:type ?o",

                        "?s a ex:C",
                        "<a> a ex:C",
                        "exns:Subject a ex:C",
                        "?s a foaf:Person"
                ), List.of(
                        "?s exns:q ?o",
                        "exns:pq exns:q exns:p",
                        "ex:q ex:p ex:p1",
                        "rdf:type rdf:value foaf:Person",

                        "?s a ex:C1",
                        "?s a exns:C",
                        "foaf:Person a foaf:Personx"
        ));
    }

}