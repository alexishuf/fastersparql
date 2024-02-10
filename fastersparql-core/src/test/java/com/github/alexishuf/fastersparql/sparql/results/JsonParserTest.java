package com.github.alexishuf.fastersparql.sparql.results;

import com.github.alexishuf.fastersparql.batch.BatchQueue;
import com.github.alexishuf.fastersparql.batch.base.SPSCBIt;
import com.github.alexishuf.fastersparql.batch.type.CompressedBatch;
import com.github.alexishuf.fastersparql.client.util.TestTaskSet;
import com.github.alexishuf.fastersparql.exceptions.FSException;
import com.github.alexishuf.fastersparql.model.SparqlResultFormat;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.ByteRope;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import com.github.alexishuf.fastersparql.util.Results;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.batch.type.CompressedBatchType.COMPRESSED;
import static com.github.alexishuf.fastersparql.util.Results.*;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public class JsonParserTest extends ResultsParserTest {

    public static Stream<Arguments> test() {
        return Stream.of(
                /*  1 */arguments("[]", results().error(InvalidSparqlResultsException.class)),
                /*  2 */arguments("{}", results().error(InvalidSparqlResultsException.class)),
                // non-empty response with unexpected properties
                /*  3 */arguments("{\"x\": false}", results().error(InvalidSparqlResultsException.class)),
                // empty vars, no results
                /*  4 */arguments("{\"head\": {\"vars\": []}}", negativeResult()),
                //empty vars, null results
                /*  5 */arguments("{\"head\": {\"vars\": []}, \"results\": null}", negativeResult()),
                //empty vars, null bindings
                /*  6 */arguments("{\"head\": {\"vars\": []}, \"results\": {\"bindings\": null}}", negativeResult()),
                //empty vars, empty bindings
                /*  7 */arguments("{\"head\": {\"vars\": []}, \"results\": {\"bindings\": []}}", negativeResult()),

                // single var, no bindings
                /*  8 */arguments("{\"head\": {\"vars\": [\"x\"]}, \"results\": {\"bindings\": []}}",
                                  results(Vars.of("x"))),
                // two vars, no bindings
                /*  9 */arguments("{\"head\": {\"vars\": [\"x\", \"y\"]}, \"results\": {\"bindings\": []}}",
                                  results("?x", "?y")),

                // single var, single typed result
                /* 10 */arguments("""
                                  {
                                    "head": {
                                      "vars": [ "x" ]
                                    }
                                    "results" : {
                                      "bindings" : [
                                        {
                                          "x" : {
                                            "value": 23,
                                            "type": "literal",
                                            "datatype": "http://www.w3.org/2001/XMLSchema#integer"
                                          }
                                        }
                                      ]
                                    }
                                  }
                                  """,
                                 results("?x", "23")),
                // single var, single lang-tagged result
                /* 11 */arguments("{\"head\": {\"vars\": [\"x\"]}, \"results\": {\"bindings\": [" +
                                  "{\"x\": {\"value\": \"bob\", \"type\":\"literal\", \"xml:lang\": \"en\"}}" +
                                  "]}}",
                                  results("?x", "\"bob\"@en")),
                // single var, plain literal
                /* 12 */arguments("{\"head\": {\"vars\": [\"x\"]}, \"results\": {\"bindings\": [" +
                                  "{\"x\": {\"value\": \"alice\", \"type\":\"literal\"}}" +
                                  "]}}",
                                  results("?x", "\"alice\"")),
                // single var, three rows
                /* 13 */arguments("{\"head\": {\"vars\": [\"x\"]}, \"results\": {\"bindings\": [" +
                                  "{\"x\": {\"value\": \"23\", \"type\":\"literal\", \"datatype\": \"http://www.w3.org/2001/XMLSchema#int\"}}," +
                                  "{\"x\": {\"value\": \"alice\", \"type\":\"literal\", \"xml:lang\": \"en_US\"}}," +
                                  "{\"x\": {\"value\": \"bob\", \"type\":\"literal\"}}," +
                                  "]}}",
                                 results("?x",
                                         "\"23\"^^<http://www.w3.org/2001/XMLSchema#int>",
                                         "\"alice\"@en-US",
                                         "\"bob\"")),
                //single var extra var on binding
                /* 14 */arguments("{\"head\": {\"vars\": [\"x\"]}, \"results\": {\"bindings\": [" +
                                  "{\"x\": {\"value\": \"bob\", \"type\":\"literal\"}, " +
                                  "\"y\": {\"value\": \"wrong\", \"type\":\"literal\"}}," +
                                  "]}}",
                                  results("?x", "\"bob\"")),
                //single var extra var on 2nd binding
                /* 15 */arguments("{\"head\": {\"vars\": [\"x\"]}, \"results\": {\"bindings\": [" +
                                  "{\"x\": {\"value\": \"alice\", \"type\":\"literal\"}}," +
                                  "{\"x\": {\"value\": \"bob\", \"type\":\"literal\"}, " +
                                  " \"y\": {\"value\": \"wrong\", \"type\":\"literal\"}}," +
                                  "]}}",
                                  results("?x", "\"alice\"", "\"bob\"")),
                //single var empty value object on 2nd
                /* 16 */arguments("{\"head\": {\"vars\": [\"x\"]}, \"results\": {\"bindings\": [" +
                                  "{\"x\": {\"value\": \"alice\", \"type\":\"literal\"}}," +
                                  "{\"x\": {}}," +
                                  "]}}",
                                  results("?x", "\"alice\"", null)),
                //single var empty binding on 2nd
                /* 17 */arguments("{\"head\": {\"vars\": [\"x\"]}, \"results\": {\"bindings\": [" +
                                  "{\"x\": {\"value\": \"bob\", \"type\":\"literal\"}}," +
                                  "{}" +
                                  "]}}",
                                  results("?x", "\"bob\"", null)),
                //single var empty binding
                /* 18 */arguments("{\"head\": {\"vars\": [\"x\"]}, \"results\": {\"bindings\": [{}]}}",
                                  results("?x", null)),

                //two vars two rows with unbound on first
                /* 19 */arguments("{\"head\": {\"vars\": [\"x\", \"y\"]}, \"results\": {\"bindings\": [" +
                                  "{\"x\": {\"value\": \"bob\", \"type\":\"literal\"}, \"y\": null}," +
                                  "{\"x\": {\"value\": \"charlie\", \"type\":\"literal\"}}," +
                                  "{\"x\": {\"type\":\"literal\", \"value\": \"alice\", \"xml:lang\": \"en\"},"+
                                  " \"y\": {\"type\":\"literal\", \"value\": \"charlie\"}}" +
                                  "]}}",
                                  results("?x", "?y",
                                          "\"bob\"", null,
                                          "\"charlie\"", null,
                                          "\"alice\"@en", "\"charlie\"")),

                //negative ask
                /* 20 */arguments("{\"head\": {}, \"boolean\": false}", negativeResult()),
                //positive ask
                /* 21 */arguments("{\"head\": {}, \"boolean\": true}", positiveResult()),
                //no-head negative ask
                /* 22 */arguments("{\"boolean\": false}", negativeResult()),
                //no-head positive ask
                /* 23 */arguments("{\"boolean\": true}", positiveResult()),
                //negative ask with links
                /* 24 */arguments("{\"head\": {\"link\": [\"http://example.com\"]}, \"boolean\": false}",
                        negativeResult()),
                //positive ask with links
                /* 25 */arguments("{\"head\": {\"link\": [\"http://example.com\"]}, \"boolean\": true}",
                        positiveResult()),

                //negative ask typed as string
                /* 26 */arguments("{\"head\": {\"link\": [\"http://example.com\"]}, \"boolean\": \"false\"}",
                        negativeResult()),
                //positive ask typed as string
                /* 27 */arguments("{\"head\": {\"link\": [\"http://example.com\"]}, \"boolean\": \"true\"}",
                        positiveResult()),
                //negative ask typed as camel string
                /* 28 */arguments("{\"head\": {\"link\": [\"http://example.com\"]}, \"boolean\": \"False\"}",
                        negativeResult()),
                //positive ask typed as camel string
                /* 29 */arguments("{\"head\": {\"link\": [\"http://example.com\"]}, \"boolean\": \"True\"}",
                        positiveResult()),
                //negative ask typed as number
                /* 30 */arguments("{\"head\": {\"link\": [\"http://example.com\"]}, \"boolean\": 0}",
                        negativeResult()),
                //positive ask typed as number
                /* 31 */arguments("{\"head\": {\"link\": [\"http://example.com\"]}, \"boolean\": 1}",
                        positiveResult()),
                //ask typed as unguessable string
                /* 32 */arguments("{\"head\": {\"link\": [\"http://example.com\"]}, \"boolean\": \"x\"}",
                        results().error(InvalidSparqlResultsException.class)),
                //negative ask typed as null
                /* 33 */arguments("{\"head\": {\"link\": [\"http://example.com\"]}, \"boolean\": null}",
                        negativeResult()),
                //positive ask with empty vars list
                /* 34 */arguments("{\"head\": {\"vars\": []}, \"boolean\": true}", positiveResult()),
                //negative ask with empty vars list
                /* 35 */arguments("{\"head\": {\"vars\": []}, \"boolean\": false}", negativeResult())
        );
    }

    @ParameterizedTest @MethodSource
    void test(String in, Results expected) throws Exception {
        doTest(new JsonParser.JsonFactory(), expected, SegmentRope.of(in));
    }

    private void feedS8(ResultsParser<?> parser) {
        try {
            var is = getClass().getResourceAsStream("S8.json.chunked");
            assertNotNull(is, "resource file missing");
            try (var reader = new BufferedReader(new InputStreamReader(is, UTF_8))) {
                var chunk = new ByteRope();
                int nSegments = 0;
                for (String line; (line = reader.readLine()) != null; ) {
                    if (line.equals("BEGIN")) {
                        assertTrue(chunk.isEmpty());
                    } else if (line.equals("END")) {
                        try {
                            parser.feedShared(chunk);
                        } catch (BatchQueue.QueueStateException e) {
                            fail("queue terminated after nSegments=" + nSegments);
                        }
                        chunk.clear();
                        ++nSegments;
                    } else {
                        chunk.append(line, 4, line.length());
                    }
                }
                parser.feedEnd();
            }
        } catch (Throwable t) {
            parser.feedError(FSException.wrap(null, t));
        }
    }

    private static final Term S8_DRUG_0 = Term.valueOf("<http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugs/DB00008>");
    private static final Term S8_MELT_0 = Term.valueOf("\"61 oC (Beldarrain, A. et al., Biochemistry 38:7865-7873 (1999))Davis JM, Narachi MA, Levine HL, Alton NK, Arakawa T. Conformation and stability of two recombinant human interferon-alpha analogs.\"");

    private static final Term S8_DRUG_1 = Term.valueOf("<http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugs/DB00009>");
    private static final Term S8_MELT_1 = Term.valueOf("\"60 oC (Novokhatny, V.V. et al., J. Biol. Chem. 266:12994-123002 (1991))\"");

    private static final Term S8_DRUG_2 = Term.valueOf("<http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugs/DB00011>");
    private static final Term S8_MELT_2 = Term.valueOf("\"Melting point: 61 oC (Beldarrain, A. et al., Biochemistry 38:7865-7873 (1999))\"");

    private static final Term S8_DRUG__3 = Term.valueOf("<http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugs/DB05245>");
    private static final Term S8_MELT__3 = Term.valueOf("\"285 oC\"");

    private static final Term S8_DRUG__2 = Term.valueOf("<http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugs/DB06151>");
    private static final Term S8_MELT__2 = Term.valueOf("\"109.5 oC\"");

    private static final Term S8_DRUG__1 = Term.valueOf("<http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugs/DB06288>");
    private static final Term S8_MELT__1 = Term.valueOf("\"126-127 oC [PhysProp]\"");

    @SuppressWarnings("SequencedCollectionMethodCanBeUsed")
    private static void checkS8(SPSCBIt<CompressedBatch> queue) {
        List<Term> results = new ArrayList<>();
        for (CompressedBatch batch = null; (batch= queue.nextBatch(batch)) != null; ) {
            for (CompressedBatch n = batch; n != null; n = n.next) {
                for (int r = 0, rows = n.rows; r < rows; r++) {
                    results.add(n.get(r, 0));
                    results.add(n.get(r, 1));
                }
            }
        }
        assertFalse(results.isEmpty());
        assertTrue(results.size() > 6);
        assertEquals(S8_DRUG_0, results.get(0));
        assertEquals(S8_MELT_0, results.get(1));
        assertEquals(S8_DRUG_1, results.get(2));
        assertEquals(S8_MELT_1, results.get(3));
        assertEquals(S8_DRUG_2, results.get(4));
        assertEquals(S8_MELT_2, results.get(5));

        assertEquals(S8_DRUG__1, results.get(results.size()-2));
        assertEquals(S8_MELT__1, results.get(results.size()-1));
        assertEquals(S8_DRUG__2, results.get(results.size()-4));
        assertEquals(S8_MELT__2, results.get(results.size()-3));
        assertEquals(S8_DRUG__3, results.get(results.size()-6));
        assertEquals(S8_MELT__3, results.get(results.size()-5));
        assertEquals(1159*2, results.size());
    }

    private static final ExecutorService feederService
            = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

    @AfterAll
    static void afterAll() throws InterruptedException {
        feederService.shutdown();
        assertTrue(feederService.awaitTermination(1, TimeUnit.SECONDS));
    }

    private Void doRegressionEarlyTerminationOnS8(boolean lateConsumer) throws Exception {
        var queue = new SPSCBIt<>(COMPRESSED, Vars.of("drug", "melt"), Integer.MAX_VALUE);
        var parser = JsonParser.createFor(SparqlResultFormat.JSON, queue);
        var asyncFed = lateConsumer ? null : feederService.submit(() -> feedS8(parser));
        if (asyncFed == null)
            feedS8(parser);
        try {
            checkS8(queue);
        } finally {
            if (asyncFed != null)
                asyncFed.get(1, TimeUnit.SECONDS);
        }
        return null;
    }

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void regressionEarlyTerminationOnS8(boolean lateConsumer) throws Exception{
        for (int i = 0; i < 20; i++)
            doRegressionEarlyTerminationOnS8(lateConsumer);

    }

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void concurrentRegressionEarlyTerminationOnS8(boolean lateConsumer) throws Exception {
        int threads = Runtime.getRuntime().availableProcessors()
                    * (lateConsumer ? 2 : 1);
        try (var tasks = TestTaskSet.platformTaskSet(getClass().getSimpleName())) {
            for (int i = 0; i < 10; i++) {
                tasks.repeat(threads, () -> doRegressionEarlyTerminationOnS8(false));
                tasks.awaitAndReset();
            }
        }
    }
}
