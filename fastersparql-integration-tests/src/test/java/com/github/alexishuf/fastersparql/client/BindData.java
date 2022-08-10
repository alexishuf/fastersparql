package com.github.alexishuf.fastersparql.client;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.model.SparqlConfiguration;
import com.github.alexishuf.fastersparql.client.parser.row.RowParser;
import com.github.alexishuf.fastersparql.client.util.reactive.FSPublisher;
import com.github.alexishuf.fastersparql.client.util.sparql.SparqlUtils;
import reactor.core.publisher.Flux;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

@SuppressWarnings("unused")
public class BindData extends ResultsData {
    private List<String> bindingVars;
    private List<String[]> bindingRows;
    private BindType bindType;

    public BindData(ResultsData other) {
        super(other);
        this.bindingVars = emptyList();
        this.bindingRows = singletonList(new String[0]);
        this.bindType = BindType.JOIN;
    }

    public BindData(BindData other) {
        super(other);
        this.bindingVars = other.bindingVars;
        this.bindingRows = other.bindingRows;
        this.bindType = other.bindType;
    }

    public BindData(String sparql, List<String> bindingVars, List<String[]> bindingRows,
                    BindType bindType, boolean value) {
        super(sparql, value);
        this.bindingVars = bindingVars;
        this.bindingRows = bindingRows;
        this.bindType = bindType;
    }

    public BindData(String sparql, List<String> bindingVars, List<String[]> bindingRows,
                    BindType bindType, String... values) {
        super(sparql, bindType.resultVars(bindingVars, SparqlUtils.publicVars(sparql)), values);
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

    public BindData bindingVars(List<String> value)   { bindingVars = value; return this; }
    public BindData bindingRows(List<String[]> value) { bindingRows = value; return this; }
    public BindData    bindType(BindType value)       { bindType    = value; return this; }

    public Results<String[]> bindings() {
        return new Results<>(bindingVars, String[].class,
                             FSPublisher.bindToAny(Flux.fromIterable(bindingRows)));
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
        if (config().methods().size() == 1)
            sb.append("method=").append(config().methods().get(0)).append(", ");
        if (config().resultsAccepts().size() == 1)
            sb.append("fmt=").append(config().resultsAccepts().get(0)).append(", ");
        return sb.append("sparql=").append(sparql().replace(PREFIX, "")).append('}').toString();
    }

    public static BuilderStage1 join(String sparql, String... vars) {
        return new BuilderStage1(BindType.JOIN, sparql, vars == null ? emptyList() : asList(vars));
    }

    public static BuilderStage1 leftJoin(String sparql, String... vars) {
        return new BuilderStage1(BindType.LEFT_JOIN, sparql, vars == null ? emptyList() : asList(vars));
    }

    public static BuilderStage1 exists(String sparql, String... vars) {
        return new BuilderStage1(BindType.EXISTS, sparql, vars == null ? emptyList() : asList(vars));
    }

    public static BuilderStage1 notExists(String sparql, String... vars) {
        return new BuilderStage1(BindType.NOT_EXISTS, sparql, vars == null ? emptyList() : asList(vars));
    }

    public static BuilderStage1 minus(String sparql, String... vars) {
        return new BuilderStage1(BindType.MINUS, sparql, vars == null ? emptyList() : asList(vars));
    }

    public static final class BuilderStage1 {
        private final BindType bindType;
        private final String sparql;
        private final List<String> vars;

        public BuilderStage1(BindType bindType, String sparql, List<String> vars) {
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

    @Override public BindData config(SparqlConfiguration config) {
        return (BindData) super.config(config);
    }

    @Override public BindData sparql(String sparql) {
        return (BindData) super.sparql(sparql);
    }

    @Override
    public BindData with(Consumer<SparqlConfiguration.Builder> configurator) {
        SparqlConfiguration.Builder b = config().toBuilder();
        configurator.accept(b);
        return new BindData(this).config(b.build());
    }

    public Results<String[]> query(SparqlClient<String[], ?> client) {
        return client.query(sparql(), config(), bindings(), bindType());
    }

    @SuppressWarnings("unchecked")
    public <R> Results<?> query(SparqlClient<?, ?> client, RowParser<R> rowParser) {
        SparqlClient<Object, ?> objClient = (SparqlClient<Object, ?>) client;
        Results<String[]> bindings = bindings();
        FSPublisher<R> pub = rowParser.parseStringsArray(bindings);
        Results<R> parsedBindings = new Results<>(bindings.vars(), rowParser.rowClass(), pub);
        return objClient.query(sparql(), config(), (Results<Object>) parsedBindings, bindType());
    }
}
