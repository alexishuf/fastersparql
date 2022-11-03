package com.github.alexishuf.fastersparql.client;

import com.github.alexishuf.fastersparql.client.model.Vars;
import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.adapters.IteratorBIt;
import com.github.alexishuf.fastersparql.client.model.row.types.ArrayRow;
import com.github.alexishuf.fastersparql.sparql.SparqlQuery;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.util.Collections.singletonList;

@SuppressWarnings("unused")
public class BindData extends ResultsData {
    private Vars bindingVars;
    private List<String[]> bindingRows;
    private BindType bindType;

    public BindData(ResultsData other) {
        super(other);
        this.bindingVars = Vars.EMPTY;
        this.bindingRows = singletonList(new String[0]);
        this.bindType = BindType.JOIN;
    }

    public BindData(BindData other) {
        super(other);
        this.bindingVars = other.bindingVars;
        this.bindingRows = other.bindingRows;
        this.bindType = other.bindType;
    }

    public BindData(String sparql, Vars bindingVars, List<String[]> bindingRows,
                    BindType bindType, boolean value) {
        super(sparql, value);
        this.bindingVars = bindingVars;
        this.bindingRows = bindingRows;
        this.bindType = bindType;
    }

    public BindData(String sparql, Vars bindingVars, List<String[]> bindingRows,
                    BindType bindType, String... values) {
        super(sparql, bindType.resultVars(bindingVars, new SparqlQuery(sparql).publicVars), values);
        this.bindingVars = bindingVars;
        this.bindingRows = new ArrayList<>();
        for (String[] row : bindingRows) {
            String[] expanded = new String[row.length];
            for (int i = 0; i < row.length; i++)
                expanded[i] = expandVars(row[i]);
            this.bindingRows.add(expanded);
        }
        this.bindType = bindType;
    }

    public List<String>   bindingVars() { return bindingVars; }
    public List<String[]> bindingRows() { return bindingRows; }
    public BindType          bindType() { return bindType; }

    public BindData bindingVars(Vars value)           { bindingVars = value; return this; }
    public BindData bindingRows(List<String[]> value) { bindingRows = value; return this; }
    public BindData    bindType(BindType value)       { bindType    = value; return this; }

    public BIt<String[]> bindings() {
        return new IteratorBIt<>(bindingRows, String[].class, bindingVars);
    }

    @Override public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("BindData").append('(').append(bindType).append(')');
        sb.append(bindingVars).append("<-[");
        for (String[] row : bindingRows) {
            String string = Arrays.toString(row)
                    .replaceAll("\"([^\"]*)\"\\^\\^<http://www\\.w3\\.org/2001/XMLSchema#([^>]*)>",
                               "\"$1\"^^xsd:$2")
                    .replaceAll("<http://example\\.org/([^>]+)>", ":$1");
            sb.append(string).append(", ");
        }
        if (!bindingRows.isEmpty())
            sb.setLength(sb.length()-2);
        sb.append("]{");
        return sb.append("sparql=").append(sparql().sparql.replace(PREFIX, "")).append('}').toString();
    }

    public static BuilderStage1 join(String sparql, String... vars) {
        return new BuilderStage1(BindType.JOIN, sparql, Vars.of(vars));
    }

    public static BuilderStage1 leftJoin(String sparql, String... vars) {
        return new BuilderStage1(BindType.LEFT_JOIN, sparql, Vars.of(vars));
    }

    public static BuilderStage1 exists(String sparql, String... vars) {
        return new BuilderStage1(BindType.EXISTS, sparql, Vars.of(vars));
    }

    public static BuilderStage1 notExists(String sparql, String... vars) {
        return new BuilderStage1(BindType.NOT_EXISTS, sparql, Vars.of(vars));
    }

    public static BuilderStage1 minus(String sparql, String... vars) {
        return new BuilderStage1(BindType.MINUS, sparql, Vars.of(vars));
    }

    public static final class BuilderStage1 {
        private final BindType bindType;
        private final String sparql;
        private final Vars vars;

        public BuilderStage1(BindType bindType, String sparql, Vars vars) {
            this.bindType = bindType;
            this.sparql = sparql;
            this.vars = vars;
        }

        public BuilderStage2 to(String... values) {
            List<String[]> rows = new ArrayList<>();
            for (int i = 0; i < values.length; i += vars.size()) {
                int rowEnd = i + vars.size();
                assert rowEnd <= values.length;
                rows.add(Arrays.copyOfRange(values, i, rowEnd));
            }
            return new BuilderStage2(this, rows);
        }
    }

    public static class BuilderStage2 {
        private final BuilderStage1 stage1;
        private final List<String[]> bindingRows;

        public BuilderStage2(BuilderStage1 stage1, List<String[]> bindingRows) {
            this.stage1 = stage1;
            this.bindingRows = bindingRows;
        }

        public BindData expecting(String... values) {
            return new BindData(stage1.sparql, stage1.vars, bindingRows, stage1.bindType, values);
        }
        public BindData expecting(boolean value) {
            return new BindData(stage1.sparql, stage1.vars, bindingRows, stage1.bindType, value);
        }
    }

    public <R> BIt<R> query(SparqlClient<R, ?, ?> client) {
        var bindings = client.rowType().convert(ArrayRow.STRING, bindings());
        return client.query(sparql(), bindings, bindType());
    }
}
