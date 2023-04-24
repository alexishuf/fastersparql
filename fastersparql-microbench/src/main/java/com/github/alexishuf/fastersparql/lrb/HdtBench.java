package com.github.alexishuf.fastersparql.lrb;

import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import com.github.alexishuf.fastersparql.hdt.FSHdtProperties;
import com.github.alexishuf.fastersparql.hdt.HdtSparqlClient;
import com.github.alexishuf.fastersparql.hdt.batch.HdtBatch;
import com.github.alexishuf.fastersparql.model.rope.Rope;
import com.github.alexishuf.fastersparql.sparql.SparqlQuery;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import com.github.alexishuf.fastersparql.sparql.parser.SparqlParser;
import com.github.alexishuf.fastersparql.util.concurrent.Async;
import org.openjdk.jmh.annotations.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@Threads(1)
@Fork(value = 1, warmups = 0, jvmArgsPrepend = {"--enable-preview", "--add-modules", "jdk.incubator.vector"})
@Measurement(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Warmup(iterations = 4, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@BenchmarkMode({Mode.AverageTime})
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class HdtBench {
    private static final Logger log = LoggerFactory.getLogger(HdtBench.class);

    private static final String PROLOGUE = """
            PREFIX     : <http://example.org/>
            PREFIX exns: <http://www.example.org/ns#>
            PREFIX  xsd: <http://www.w3.org/2001/XMLSchema##>
            PREFIX  rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            PREFIX  owl: <http://www.w3.org/2002/07/owl#>
            PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            """;
    private File nytFile;
    private HdtSparqlClient nyt;
    private SparqlQuery dump;
    private SparqlQuery distinctPredicates;

    @Setup(Level.Trial) public void setup() throws IOException {
        if (nytFile == null || !nytFile.isFile()) {
            nytFile = Files.createTempFile("fastersparql-NYT", ".hdt").toFile();
            try (var is = HdtBench.class.getResourceAsStream("NYT.hdt");
                 var out = new FileOutputStream(nytFile)) {
                if (is == null) throw new RuntimeException("Resource NYT.hdt not found");
                if (is.transferTo(out) == 0) throw new RuntimeException("Empty NYT.hdt");
            }
            log.info("Loading {} (might take a few seconds to build sidecar index)...", nytFile);
        }
        System.setProperty(FSHdtProperties.ESTIMATOR_PEEK, "METADATA");
        FSHdtProperties.refresh();
        String url = "file://" + nytFile.getAbsolutePath().replace(" ", "%20");
        nyt = new HdtSparqlClient(SparqlEndpoint.parse(url));
        nyt = Async.waitStage(nyt.estimatorReady());
        var p = new SparqlParser();
        dump = p.parse(Rope.of(PROLOGUE, "SELECT * WHERE { ?s ?p ?o }"));
        distinctPredicates = p.parse(Rope.of(PROLOGUE,"SELECT DISTINCT ?p WHERE { ?s ?p ?o }"));
    }

    @Setup(Level.Iteration) public void iterationSetup() {
        Workloads.cooldown(250);
    }

    @TearDown(Level.Trial) public void tearDown() throws IOException {
        nyt.close();
    }

    private double query(SparqlClient client, SparqlQuery query) {
        int total = 0, lits = 0;
        try (var it = client.query(HdtBatch.TYPE, query)) {
            for (HdtBatch b = null; (b = it.nextBatch(b)) != null; ) {
                for (int r = 0, rows = b.rows, cols = b.cols; r < rows; r++) {
                    for (int c = 0; c < cols; c++) {
                        if (b.termType(r, c) == Term.Type.LIT) ++lits;
                    }
                }
                total += b.rows*b.cols;
            }
        }
        return lits/(double)total;
    }

    @Benchmark public double dump()               { return query(nyt, dump); }
    @Benchmark public double distinctPredicates() { return query(nyt, distinctPredicates); }
}
