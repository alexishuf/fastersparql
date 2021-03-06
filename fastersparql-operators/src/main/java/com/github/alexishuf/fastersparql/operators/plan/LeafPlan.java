package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.model.SparqlConfiguration;
import com.github.alexishuf.fastersparql.client.util.CSUtils;
import com.github.alexishuf.fastersparql.client.util.sparql.Binding;
import com.github.alexishuf.fastersparql.client.util.sparql.SparqlUtils;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.experimental.Accessors;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Collections.emptyList;

@Accessors(fluent = true) @EqualsAndHashCode(callSuper = true)
public class LeafPlan<R> extends AbstractPlan<R, LeafPlan<R>> {
    private static final AtomicInteger nextId = new AtomicInteger(1);
    @Getter private final CharSequence query;
    @Getter private final SparqlClient<R, ?> client;
    @Getter private final SparqlConfiguration configuration;
    private @MonotonicNonNull List<String> publicVars;
    private @MonotonicNonNull List<String> allVars;

    @Builder
    public LeafPlan(@lombok.NonNull CharSequence query,
                    @lombok.NonNull SparqlClient<R, ?> client,
                    @Nullable SparqlConfiguration configuration,
                    @Nullable LeafPlan<R> parent, @Nullable String name) {
        super(client.rowClass(), emptyList(),
              name == null ? "Query-"+nextId.getAndIncrement() : name, parent);
        this.query = query.toString();
        this.client = client;
        this.configuration = configuration == null ? SparqlConfiguration.EMPTY : configuration;
    }

    private LeafPlan(@lombok.NonNull LeafPlan<R> parent, @lombok.NonNull CharSequence query,
                     @lombok.NonNull SparqlClient<R, ?> client,
                     @lombok.NonNull SparqlConfiguration configuration) {
        super(client.rowClass(), emptyList(), parent.name, parent);
        this.query = query;
        this.client = client;
        this.configuration = configuration;
    }

    public static <T> LeafPlanBuilder<T> builder(SparqlClient<T, ?> client, CharSequence query) {
        return new LeafPlanBuilder<T>().client(client).query(query);
    }

    @Override public List<String> publicVars() {
        if (publicVars == null)
            publicVars = SparqlUtils.publicVars(query);
        return publicVars;
    }

    @Override public List<String> allVars() {
        if (allVars == null)
            allVars = SparqlUtils.allVars(query);
        return allVars;
    }


    @Override protected String algebraName() {
        StringBuilder sb = new StringBuilder();
        sb.append("Query[").append(client.endpoint().uri()).append("](");
        if (query.length() < 80) {
            return sb.append(query.toString().replace("\n", "\\n")).append(')').toString();
        } else {
            sb.append('\n');
            for (int start = 0, i, len = query.length(); start < len; start = i+1) {
                i = CSUtils.skipUntil(query, start, '\n');
                sb.append("  ").append(query, start, i).append('\n');
            }
            return sb.append(')').toString();
        }
    }

    @Override public Results<R> execute() {
        return client.query(query, configuration);
    }

    @Override public Plan<R> bind(Binding binding) {
        CharSequence bound = SparqlUtils.bind(query, binding);
        return new LeafPlan<>(this, bound, client, configuration);
    }
}
