package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.operators.Slice;
import lombok.Builder;
import lombok.Value;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Map;

import static java.lang.String.format;

@Slf4j
@Value @Accessors(fluent = true)
public class SlicePlan<R> implements Plan<R> {
    Class<? super R> rowClass;
    Slice op;
    Plan<R> input;
    long offset, limit;
    String name;

    @SuppressWarnings({"FieldMayBeFinal", "unused"}) //default values
    public static class SlicePlanBuilder<R> {
        private long offset = 0;
        private long limit = Long.MAX_VALUE;
    }

    @Builder
    public SlicePlan(@lombok.NonNull Class<? super R> rowClass, @lombok.NonNull Slice op,
                     @lombok.NonNull Plan<R> input, long offset, long limit,
                     @Nullable String name) {
        this.rowClass = rowClass;
        this.op = op;
        this.input = input;
        if (offset < 0)
            throw new IllegalArgumentException("Negative offset "+offset);
        this.offset = offset;
        this.limit = limit;
        if (limit < 0)
            throw new IllegalArgumentException("Negative limit "+limit);
        if (limit == 0) {
            log.warn("limit=0 on SlicePlan({}, {}, {} {}, {}, {})",
                     rowClass, op, input, offset, limit, name);
        }
        if (name == null) {
            name = format("Slice[%d:%s]-%s", offset,
                          limit > Integer.MAX_VALUE ? "" : String.valueOf(limit), input.name());
        }
        this.name = name;
    }

    @Override public Results<R> execute() {
        return op.run(this);
    }

    @Override public List<String> publicVars() {
        return input.publicVars();
    }

    @Override public List<String> allVars() {
        return input.allVars();
    }

    @Override public Plan<R> bind(Map<String, String> var2ntValue) {
        return new SlicePlan<>(rowClass, op, input.bind(var2ntValue), offset, limit, name);
    }

    @Override public Plan<R> bind(List<String> vars, List<String> ntValues) {
        return new SlicePlan<>(rowClass, op, input.bind(vars, ntValues), offset, limit, name);
    }

    @Override public Plan<R> bind(List<String> vars, String[] ntValues) {
        return new SlicePlan<>(rowClass, op, input.bind(vars, ntValues), offset, limit, name);
    }

}
