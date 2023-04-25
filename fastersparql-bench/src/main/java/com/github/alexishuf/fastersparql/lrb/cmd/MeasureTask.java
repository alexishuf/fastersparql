package com.github.alexishuf.fastersparql.lrb.cmd;

import com.github.alexishuf.fastersparql.lrb.query.QueryName;
import com.github.alexishuf.fastersparql.lrb.sources.SelectorKind;
import com.github.alexishuf.fastersparql.lrb.sources.SourceKind;
import com.github.alexishuf.fastersparql.operators.plan.Plan;

public record MeasureTask(QueryName query, Plan parsed, SelectorKind selector, SourceKind source) {
    public MeasureTask(QueryName query, SelectorKind selector, SourceKind source) {
        this(query, query.parsed(), selector, source);
    }
}
