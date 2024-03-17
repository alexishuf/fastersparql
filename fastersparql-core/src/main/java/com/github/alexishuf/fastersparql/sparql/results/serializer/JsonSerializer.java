package com.github.alexishuf.fastersparql.sparql.results.serializer;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.model.SparqlResultFormat;
import com.github.alexishuf.fastersparql.model.rope.ByteSink;
import com.github.alexishuf.fastersparql.sparql.expr.Term;

import java.util.Map;

import static com.github.alexishuf.fastersparql.model.SparqlResultFormat.JSON;
import static java.nio.charset.StandardCharsets.UTF_8;

public class JsonSerializer extends ResultsSerializer {
    private boolean firstRow = true;

    public static class JsonFactory implements Factory {
        @Override public ResultsSerializer create(Map<String, String> params) {
            return new JsonSerializer();
        }
        @Override public SparqlResultFormat name() { return JSON; }
    }

    public JsonSerializer() {
        super(JSON.asMediaType());
    }

    private static final byte[] HDR_BFR = "{\"head\":{\"vars\":[".getBytes(UTF_8);
    private static final byte[] HDR_ASK = "]},\n \"boolean\":".getBytes(UTF_8);
    private static final byte[] HDR_SEL = "]},\n\"results\":{\"bindings\":[".getBytes(UTF_8);

    @Override protected void onInit() {
        firstRow = true;
    }

    @Override public void serializeHeader(ByteSink<?, ?> dest) {
        dest.append(HDR_BFR);
        for (int i = 0, n = subset.size(); i < n; i++) {
            if (i != 0) dest.append(',');
            dest.append('"').append(subset.get(i)).append('"');
        }
        dest.append(ask ? HDR_ASK : HDR_SEL);
    }

    private static final byte[] COL_LANG     = "\", \"xml:lang\":\"".getBytes(UTF_8);
    private static final byte[] COL_DATATYPE = "\", \"datatype\":\"".getBytes(UTF_8);
    private static final byte[] COL_LIT   = "\":{\"type\":\"literal\", \"value\":".getBytes(UTF_8);
    private static final byte[] COL_IRI   = "\":{\"type\":\"iri\", \"value\":\"".getBytes(UTF_8);
    private static final byte[] COL_BLANK = "\":{\"type\":\"bnode\", \"value\":\"".getBytes(UTF_8);
    private static final byte[] COL_END = "\"}".getBytes(UTF_8);
    private static final byte[] COL_SEP = ",\n ".getBytes(UTF_8);
    private static final byte[] ROW_OPEN  = "\n{".getBytes(UTF_8);
    private static final byte[] ROW_SEP   = ",\n{".getBytes(UTF_8);
    @Override public void serialize(Batch<?> batch, int begin, int nRows, ByteSink<?, ?> dest) {
        if (ask) {
            if (nRows > 0) empty = false;
            return;
        }
        for (int end = begin+nRows; begin < end; ++begin) {
            dest.append(firstRow ? ROW_OPEN : ROW_SEP);
            this.firstRow = false;
            boolean firstCol = true;
            for (int col : columns) {
                var type = col < 0 ? null : batch.termType(begin, col);
                if (type == null) continue;
                if (firstCol) firstCol = false;
                else          dest.append(COL_SEP);
                dest.append('"').append(vars.get(col)); // write: "$VAR
                int len = batch.len(begin, col);
                switch (type) {
                    case LIT -> {
                        dest.append(COL_LIT);
                        int lexEnd = batch.lexEnd(begin, col);
                        batch.write(dest, begin, col, 0, lexEnd);
                        Term dtTerm = batch.datatypeTerm(begin, col);
                        if (dtTerm == Term.RDF_LANGSTRING) { // @lang
                            dest.append(COL_LANG);
                            batch.write(dest, begin, col, lexEnd + 2, len);
                        } else if (dtTerm != null && dtTerm != Term.XSD_STRING) { // datatype
                            dest.append(COL_DATATYPE);
                            dest.append(dtTerm, 1, dtTerm.len - 1);
                        }
                    }
                    case IRI -> {
                        dest.append(COL_IRI);
                        batch.write(dest, begin, col, 1, len - 1);
                    }
                    case BLANK -> {
                        dest.append(COL_BLANK);
                        batch.write(dest, begin, col, 2, len);
                    }
                }
                dest.append(COL_END);
            }
            dest.append('}');
        }
    }

    private static final byte[] TRAILER_SEL = "\n]}}".getBytes(UTF_8);
    private static final byte[] TRAILER_ASK_TRUE  =  "true\n}".getBytes(UTF_8);
    private static final byte[] TRAILER_ASK_FALSE = "false\n}".getBytes(UTF_8);
    @Override public void serializeTrailer(ByteSink<?, ?> dest) {
        if (ask) {
            dest.append(empty ? TRAILER_ASK_FALSE : TRAILER_ASK_TRUE);
        } else {
            dest.append(TRAILER_SEL);
        }
    }
}
