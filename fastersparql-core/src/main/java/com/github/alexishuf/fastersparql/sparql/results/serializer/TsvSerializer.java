package com.github.alexishuf.fastersparql.sparql.results.serializer;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.model.SparqlResultFormat;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.ByteSink;

import java.util.Map;

import static com.github.alexishuf.fastersparql.model.SparqlResultFormat.TSV;

public class TsvSerializer extends ResultsSerializer {

    public static class TsvFactory implements Factory {
        @Override public ResultsSerializer create(Map<String, String>params) {
            if (!params.getOrDefault("charset", "utf-8").equalsIgnoreCase("utf-8"))
                throw new NoSerializerException("Cannot generate TSV in a format other than UTF-8");
            return new TsvSerializer();
        }
        @Override public SparqlResultFormat name() { return TSV;}
    }

    public TsvSerializer() {
        super(TSV, TSV.contentType());
    }

    @Override protected void init(Vars subset, ByteSink<?> dest) {
        for (int i = 0, n = subset.size(); i < n; ++i) {
            if (i > 0) dest.append('\t');
            dest.append('?').append(subset.get(i));
        }
        dest.append('\n');
    }

    @Override public void serialize(Batch<?> batch, int begin, int nRows, ByteSink<?> dest) {
        for (int end = begin+nRows; begin < end; begin++) {
            for (int i = 0; i < columns.length; i++) {
                int col = columns[i];
                if (i != 0) dest.append('\t');
                if (col >= 0)
                    batch.writeNT(dest, begin, col);
            }
            dest.append('\n');
        }
    }

    @Override public void serializeTrailer(ByteSink<?> dest) { }
}
