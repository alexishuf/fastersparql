package com.github.alexishuf.fastersparql.fed.selectors;

import com.github.alexishuf.fastersparql.client.DummySparqlClient;
import com.github.alexishuf.fastersparql.fed.Selector;
import com.github.alexishuf.fastersparql.fed.Spec;
import com.github.alexishuf.fastersparql.sparql.OpaqueSparqlQuery;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import com.github.alexishuf.fastersparql.store.index.dict.DictSorter;
import com.github.alexishuf.fastersparql.store.index.dict.LocalityStandaloneDict;
import com.github.alexishuf.fastersparql.util.concurrent.Async;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.github.alexishuf.fastersparql.fed.Selector.InitOrigin.QUERY;
import static com.github.alexishuf.fastersparql.sparql.expr.Term.termList;
import static com.github.alexishuf.fastersparql.util.Results.results;
import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class DictionarySelectorTest extends SelectorTestBase {

    @Override protected Spec createSpec(File refDir) {
        assertFalse(absStateFileOrDir.exists());
        return Spec.of(Spec.PATHS_RELATIVE_TO, refDir,
                Selector.TYPE, DictionarySelector.NAME,
                DictionarySelector.FETCH_CLASSES, true,
                DictionarySelector.STATE, Spec.of(
                        DictionarySelector.STATE_DIR, absStateFileOrDir,
                        Selector.STATE_INIT_SAVE, true));
    }

    private static final List<Path> tempDirs = Collections.synchronizedList(new ArrayList<>());

    @AfterAll static void afterAll() throws IOException {
        for (Path dir : tempDirs) {
            try (var files = Files.newDirectoryStream(dir)) {
                for (Path file : files)
                    Files.deleteIfExists(file);
            }
        }
        tempDirs.clear();
    }

    private static LocalityStandaloneDict createDict(String... ttlIris) throws IOException {
        Path tempDir = Files.createTempDirectory("fastersparql");
        Path dest = Files.createTempFile(tempDir, "iris", ".dict");
        tempDirs.add(tempDir);
        try (DictSorter sorter = new DictSorter(tempDir, false, true)) {
            for (Term term : termList(ttlIris))
                sorter.copy(requireNonNull(term).first(), term.second());
            sorter.writeDict(dest);
        }
        var dict = new LocalityStandaloneDict(dest);
        dict.validate();
        return dict;
    }

    @Test void test() throws IOException {
        // build the selector
        var predicates = createDict("exns:p", "ex:p", "foaf:p");
        var classes = createDict("exns:A", "exns:AB", "ex:C", "foaf:Person");
        DummySparqlClient client = new DummySparqlClient(ENDPOINT);
        Selector sel = new DictionarySelector(client, spec, predicates, classes);

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
        Selector sel = checkSavedOnInit(initSel);

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