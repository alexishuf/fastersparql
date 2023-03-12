package com.github.alexishuf.fastersparql.fed.selectors;

import com.github.alexishuf.fastersparql.fed.Selector;
import com.github.alexishuf.fastersparql.fed.Spec;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static com.github.alexishuf.fastersparql.util.Results.*;

class AskSelectorTest extends SelectorTestBase {

    @Override protected Spec createSpec() {
        return Spec.of(Selector.TYPE, AskSelector.NAME);
    }

    @Test void test() throws IOException {
        client.answerWith(parseTP("ex:s   a       exns:A1").toAsk(), positiveResult());
        client.answerWith(parseTP("exns:s a       exns:A1").toAsk(), positiveResult());
        client.answerWith(parseTP("ex:s   ex:p1   ex:o ").toAsk(),   positiveResult());
        client.answerWith(parseTP("ex:s   exns:p1 ex:Bob").toAsk(),  positiveResult());

        client.answerWith(parseTP("?s     a       exns:A2").toAsk(), negativeResult());
        client.answerWith(parseTP("ex:s   exns:p1 ex:o").toAsk(),    negativeResult());
        client.answerWith(parseTP("ex:t   exns:p1 ?x").toAsk(),      negativeResult());
        client.answerWith(parseTP("?s     ex:p2   ?o").toAsk(),      negativeResult());

        testTPs(saveAndLoad(new AskSelector(client, spec)),
                List.of(
                        "ex:s   a       exns:A1", // queries
                        "?s     a       exns:A1", // answer from cache
                        "exns:s a       exns:A1", // queries
                        "ex:s   ex:p1   ex:o ",   // queries
                        "?s     ex:p1   ex:o ",   // answer from cache
                        "ex:s   ex:p1   ?o ",     // answer from cache
                        "?s     ex:p1   ?o ",     // answer from cache
                        "ex:s   exns:p1 ex:Bob"   // queries
                ),
                List.of(
                        "?s     a       exns:A2", // queries
                        "exns:s a       exns:A2", // answer from cache
                        "ex:s   exns:p1 ex:o",    //queries
                        "ex:t   exns:p1 ?x",      //queries
                        "?s     ex:p2   ?o",      //queries
                        "ex:s   ex:p2   ex:t",    //answer from cache
                        "ex:s   ex:p2   ?o",      //answer from cache
                        "?x     ex:p2   ex:o"     //answer from cache
                )
        );
    }
}