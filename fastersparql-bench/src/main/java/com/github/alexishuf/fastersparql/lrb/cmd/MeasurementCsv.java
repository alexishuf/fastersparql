package com.github.alexishuf.fastersparql.lrb.cmd;

import com.github.alexishuf.fastersparql.lrb.query.QueryName;
import com.github.alexishuf.fastersparql.lrb.sources.SelectorKind;
import com.github.alexishuf.fastersparql.lrb.sources.SourceKind;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("SpellCheckingInspection")
public class MeasurementCsv {
    public static final String QUERY_HDR = "query";
    public static final String SOURCE_HDR = "source";
    public static final String SELECTOR_HDR = "selector";
    public static final String REP_HDR = "rep";
    public static final String DISPATCHNS_HDR = "dispatchNs";
    public static final String SELECTIONANDAGGLUTINATIONNS_HDR = "selectionAndAgglutinationNs";
    public static final String OPTIMIZATIONNS_HDR = "optimizationNs";
    public static final String FIRSTROWNS_HDR = "firstRowNs";
    public static final String ALLROWSNS_HDR = "allRowsNs";
    public static final String ROWS_HDR = "rows";
    public static final String MINNSBETWEENBATCHES_HDR = "minNsBetweenBatches";
    public static final String MAXNSBETWEENBATCHES_HDR = "maxNsBetweenBatches";
    public static final String TERMINALNS_HDR = "terminalNs";
    public static final String CANCELLED_HDR = "cancelled";
    public static final String ERROR_HDR = "error";
    private static final List<String> HEADERS = List.of(
            QUERY_HDR, SOURCE_HDR, SELECTOR_HDR, REP_HDR,
            DISPATCHNS_HDR, SELECTIONANDAGGLUTINATIONNS_HDR, OPTIMIZATIONNS_HDR,
            FIRSTROWNS_HDR, ALLROWSNS_HDR, ROWS_HDR,
            MINNSBETWEENBATCHES_HDR, MAXNSBETWEENBATCHES_HDR, TERMINALNS_HDR,
            CANCELLED_HDR, ERROR_HDR);
    private static final CSVFormat CSV_FMT = CSVFormat.DEFAULT.builder()
            .setHeader().setSkipHeaderRecord(true).build();

    public static void appendRow(File file, Measurement m) throws IOException {
        if (file.length() < 4) //noinspection ResultOfMethodCallIgnored
            file.delete();
        boolean needsHeader = file.length() == 0;
        try (var fileWriter = new FileWriter(file, true);
             var printer = new CSVPrinter(fileWriter, CSV_FMT)) {
            if (needsHeader)
                printer.printRecord(HEADERS);
            var t = m.task();
            printer.printRecord(
                    t.query().name(), t.source().name(), t.selector().name(), m.rep(),
                    m.dispatchNs(), m.selectionAndAgglutinationNs(), m.optimizationNs(),
                    m.firstRowNs(), m.allRowsNs(), m.rows(),
                    m.minNsBetweenBatches(), m.maxNsBetweenBatches(), m.terminalNs(),
                    m.cancelled(), m.error());
        }
    }

    public static List<Measurement> load(File file) throws IOException {
        List<Measurement> list = new ArrayList<>();
        try (var parser = new CSVParser(new FileReader(file, StandardCharsets.UTF_8), CSV_FMT)) {
            for (CSVRecord record : parser) {
                list.add(new Measurement(
                        new MeasureTask(
                                QueryName.valueOf(record.get(QUERY_HDR)),
                                SelectorKind.valueOf(record.get(SELECTOR_HDR)),
                                SourceKind.valueOf(record.get(SOURCE_HDR))
                        ),
                        Integer.parseInt(record.get(REP_HDR)),
                        Long.parseLong(record.get(DISPATCHNS_HDR)),
                        Long.parseLong(record.get(SELECTIONANDAGGLUTINATIONNS_HDR)),
                        Long.parseLong(record.get(OPTIMIZATIONNS_HDR)),
                        Long.parseLong(record.get(FIRSTROWNS_HDR)),
                        Long.parseLong(record.get(ALLROWSNS_HDR)),
                        Integer.parseInt(record.get(ROWS_HDR)),
                        Long.parseLong(record.get(MINNSBETWEENBATCHES_HDR)),
                        Long.parseLong(record.get(MAXNSBETWEENBATCHES_HDR)),
                        Long.parseLong(record.get(TERMINALNS_HDR)),
                        Boolean.parseBoolean(record.get(CANCELLED_HDR)),
                        nullIfEmpty(record.get(ERROR_HDR))
                ));
            }
        }
        return list;
    }

    private static String nullIfEmpty(@Nullable String s) {
        return s != null && s.isEmpty() ? null : s;
    }

}
