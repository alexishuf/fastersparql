package com.github.alexishuf.fastersparql.fed.selectors;

import com.github.alexishuf.fastersparql.fed.Selector;
import com.github.alexishuf.fastersparql.fed.Spec;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;

class TrivialSelectorTest extends SelectorTestBase {
    @Override protected Spec createSpec(File refDir) {
        return Spec.of(Spec.PATHS_RELATIVE_TO, refDir, Selector.TYPE, TrivialSelector.NAME);
    }

    @Test void test() {
        testTPs(new TrivialSelector(ENDPOINT, spec),
                List.of(
                        "?s ?p ?o",
                        "exns:s ex:p foaf:Person"
                ), List.of());
    }
}