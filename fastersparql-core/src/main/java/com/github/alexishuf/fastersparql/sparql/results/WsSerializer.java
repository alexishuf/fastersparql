package com.github.alexishuf.fastersparql.sparql.results;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.model.RopeArrayMap;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.ByteRope;
import com.github.alexishuf.fastersparql.model.rope.Rope;
import com.github.alexishuf.fastersparql.sparql.PrefixAssigner;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import com.github.alexishuf.fastersparql.sparql.parser.PrefixMap;

public class WsSerializer<B extends Batch<B>> {
    private static final ByteRope PREFIX_CMD = new ByteRope("!prefix ");

    private final int[] columns;
    private final ByteRope buffer = new ByteRope(128);
    private final ByteRope rowsBuffer = new ByteRope(128);
    private final WsPrefixAssigner prefixAssigner;
    private boolean headersDone;

    public WsSerializer(Vars vars) {
        this(vars, vars);
    }
    public WsSerializer(Vars vars, Vars subset) {
        this.columns = new int[subset.size()];
        if (subset == vars) {
            for (int i = 0; i < columns.length; i++)
                columns[i] = i;
        } else {
            for (int i = 0; i < columns.length; i++) {
                int idx = vars.indexOf(subset.get(i));
                if (idx < 0) throw new IllegalArgumentException("subset is not a subset of vars");
                columns[i] = idx;
            }
        }
        RopeArrayMap prefix2name = new RopeArrayMap();
        prefix2name.put(Term.XSD.sub(0, Term.XSD.len()-1), PrefixMap.XSD_NAME);
        prefix2name.put(Term.RDF.sub(0, Term.RDF.len()-1), PrefixMap.RDF_NAME);
        prefixAssigner = new WsPrefixAssigner(prefix2name);
        for (Rope name : subset)
            buffer.append('?').append(name).append('\t');
        if (columns.length > 0)
            buffer.unAppend(1);
        buffer.append('\n');
    }

    public ByteRope serialize(B batch) {
        return serialize(batch, 0, batch == null ? 0 : batch.rows);
    }

    public ByteRope serialize(B batch, int begin, int nRows) {
        if (headersDone) buffer.clear();
        else             headersDone = true;

        if (this.columns.length == 0)
            return buffer.repeat((byte) '\n', nRows);
        for (int end = begin+nRows; begin < end; ++begin) {
            // write terms to rowsBuffer, concurrently !prefix commands may be written to buffer
            for (int col : columns) {
                batch.writeSparql(rowsBuffer, begin, col, prefixAssigner);
                rowsBuffer.append('\t');
            }
            rowsBuffer.utf8[rowsBuffer.len-1] = '\n'; // replace last '\t' with line separator
            if ((begin&0xf) == 0xf) { // flush rowsBuffer to buffer once every 16 lines
                buffer.append(rowsBuffer);
                rowsBuffer.clear();
            }
        }
        // always flush rowsBuffer on end
        buffer.append(rowsBuffer);
        rowsBuffer.clear();
        return buffer;
    }

    private final class WsPrefixAssigner extends PrefixAssigner {
        public WsPrefixAssigner(RopeArrayMap prefix2name) {
            super(prefix2name);
        }

        @Override public Rope nameFor(Rope prefix) {
            Rope name = prefix2name.get(prefix);
            if (name == null) {
                name = new ByteRope().append('p').append(prefix2name.size());
                prefix2name.put(prefix, name);
                buffer.ensureFreeCapacity(PREFIX_CMD.len+name.len()+ prefix.len()+3)
                      .append(PREFIX_CMD).append(name).append(':')
                      .append(prefix).append('>').append('\n');
            }
            return name;
        }
    }
}
