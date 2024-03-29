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

    @Override protected void onInit() { firstRow = true; }

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

    @Override
    public <B extends Batch<B>, S extends ByteSink<S, T>, T>
    void serialize(Batch<B> batch0, ByteSink<S, T> sink, int hardMax,
                   NodeConsumer<B> nodeConsumer, ChunkConsumer<T> chunkConsumer) {
        if (batch0 == null) return;
        if (batch0.rows > 0) empty = false;

        @SuppressWarnings("unchecked") B batch = (B)batch0;
        boolean chunk = !chunkConsumer.isNoOp();
        int softMax = sink.freeCapacity();
        int chunkRows = 0, lastLen = 0, r = 0;
        try {
            if (ask) {
                serializeAsk(batch, nodeConsumer);
                return;
            }
            for (; batch != null; batch = detachAndDeliverNode(batch, nodeConsumer)) {
                r = 0;
                for (int rows = batch.rows; r < rows; ++r) {
                    serialize(batch, sink, r);
                    ++chunkRows;
                    if (chunk) {
                        int len = sink.len();
                        if (len >= softMax-((len-lastLen)<<1)) {
                            deliver(sink, chunkConsumer, chunkRows, lastLen, hardMax);
                            chunkRows = 0;
                            sink.touch();
                        }
                    }
                    lastLen = sink.len();
                }
            }
            if (chunk)
                deliver(sink, chunkConsumer, 1, sink.len(),  hardMax);
        } catch (Throwable t) {
            handleNotSerialized(batch, r, nodeConsumer, t);
            throw t;
        }
    }

    private <B extends Batch<B>> void serializeAsk(B batch, NodeConsumer<B> nodeConsumer) {
        while (batch != null)
            batch = detachAndDeliverNode(batch, nodeConsumer);
    }

    @Override
    public void serialize(Batch<?> batch, ByteSink<?, ?> sink, int row) {
        sink.append(firstRow ? ROW_OPEN : ROW_SEP);
        this.firstRow = false;
        boolean firstCol = true;
        for (int col : columns) {
            var type = col < 0 ? null : batch.termType(row, col);
            if (type == null) continue;
            if (firstCol) firstCol = false;
            else          sink.append(COL_SEP);
            sink.append('"').append(vars.get(col)); // write: "$VAR
            int len = batch.len(row, col);
            switch (type) {
                case LIT -> {
                    sink.append(COL_LIT);
                    int lexEnd = batch.lexEnd(row, col);
                    batch.write(sink, row, col, 0, lexEnd);
                    Term dtTerm = batch.datatypeTerm(row, col);
                    if (dtTerm == Term.RDF_LANGSTRING) { // @lang
                        sink.append(COL_LANG);
                        batch.write(sink, row, col, lexEnd + 2, len);
                    } else if (dtTerm != null && dtTerm != Term.XSD_STRING) { // datatype
                        sink.append(COL_DATATYPE);
                        sink.append(dtTerm, 1, dtTerm.len - 1);
                    }
                }
                case IRI -> {
                    sink.append(COL_IRI);
                    batch.write(sink, row, col, 1, len - 1);
                }
                case BLANK -> {
                    sink.append(COL_BLANK);
                    batch.write(sink, row, col, 2, len);
                }
            }
            sink.append(COL_END);
        }
        sink.append('}');
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
