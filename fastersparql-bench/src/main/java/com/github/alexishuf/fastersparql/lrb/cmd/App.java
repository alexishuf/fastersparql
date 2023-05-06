package com.github.alexishuf.fastersparql.lrb.cmd;

import com.github.alexishuf.fastersparql.batch.Timestamp;
import org.rdfhdt.hdt.dictionary.DictionarySection;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;

import java.io.IOException;

@Command(name = "experiment",
        description = "Run a benchmark command",
        subcommands = {Measure.class})
public class App {
    private static final Logger log = LoggerFactory.getLogger(App.class);
    public static void main(String[] args) throws IOException {
        String hdtPath = "/home/alexis/linked-data/freqel-benchmark-data/downloads/LinkedTCGA-M.hdt";
        long n = 0;
        try (HDT hdt = HDTManager.mapIndexedHDT(hdtPath)) {
            n += visit(hdt.getDictionary().getShared(), "shared");
            n += visit(hdt.getDictionary().getSubjects(), "subjects");
            n += visit(hdt.getDictionary().getPredicates(), "predicates");
            n += visit(hdt.getDictionary().getObjects(), "objects");
        }
        log.info("Loaded strings: {}", n);
//        System.exit(run(args));
    }

    private static long visit(DictionarySection d, String sectionName) {
        long last = Timestamp.nanoTime();
        long sum = 0;
        for (long i = 1, n = d.getNumberOfElements(); i <= n; i++) {
            sum += d.extract(i).length();
            if ((i&256) == 0 && (Timestamp.nanoTime()-last) > 1_000_000_000L) {
                last = Timestamp.nanoTime();
                log.info("{}/{} for {}", i, n, sectionName);
            }
        }
        return sum;
    }

    public static int run(String[] args) {
        //noinspection InstantiationOfUtilityClass
        return new CommandLine(new App()).execute(args);
    }
}
