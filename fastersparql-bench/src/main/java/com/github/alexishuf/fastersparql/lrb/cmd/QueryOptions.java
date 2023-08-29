package com.github.alexishuf.fastersparql.lrb.cmd;

import com.github.alexishuf.fastersparql.lrb.query.QueryName;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

@Command
public class QueryOptions {
    @SuppressWarnings("unused") public QueryOptions() { this(new ArrayList<>()); }
    public QueryOptions(List<String> selectors) { this.selectors = selectors; }

    @Option(names = {"-q", "--queries"}, arity = "0..*",
            description = "A regular expression matching query names in LargeRDFBench (e.g., " +
                    "S1, S. or C.*). If no expression is given, all queries will be " +
                    "selected.")
    private List<String> selectors;

    public List<QueryName> queries() {
        if (selectors.isEmpty())
            return List.of(QueryName.values());
        List<QueryName> queries = new ArrayList<>();
        List<Pattern> patterns = selectors.stream().map(Pattern::compile).toList();
        for (QueryName query : QueryName.values()) {
            for (Pattern p : patterns) {
                if (p.matcher(query.name()).matches()) {
                    queries.add(query);
                    break;
                }
            }
        }
        return queries;
    }
}
