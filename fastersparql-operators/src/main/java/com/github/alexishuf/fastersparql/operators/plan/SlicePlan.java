package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.util.sparql.Binding;
import com.github.alexishuf.fastersparql.operators.Slice;
import lombok.Builder;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

import static java.lang.String.format;
import static java.util.Collections.singletonList;

public class SlicePlan<R> extends AbstractUnaryPlan<R, SlicePlan<R>> {
    private static final Logger log = LoggerFactory.getLogger(SlicePlan.class);
    private final Slice op;
    private final long offset, limit;

    @SuppressWarnings({"FieldMayBeFinal", "unused"}) //default values
    public static class SlicePlanBuilder<R> {
        private long offset = 0;
        private long limit = Long.MAX_VALUE;
    }

    public Slice op()     { return op; }
    public long  offset() { return offset; }
    public long  limit()  { return limit; }

    private static String algebraName(long offset, long limit) {
        String limitStr = limit > Integer.MAX_VALUE ? "" : String.valueOf(limit);
        return format("Slice[%d:%s]", offset, limitStr);
    }

    @Builder
    public SlicePlan(@lombok.NonNull Class<? super R> rowClass, @lombok.NonNull Slice op,
                     @lombok.NonNull Plan<R> input, long offset, long limit,
                     @Nullable SlicePlan<R> parent, @Nullable String name) {
        super(rowClass, singletonList(input),
              name != null ? name : algebraName(offset, limit) + "-" + input.name(), parent);
        this.op = op;
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
    }

    @Override protected String    algebraName() { return algebraName(offset, limit); }
    @Override public Results<R>   execute()     { return op.run(this); }

    @Override public Plan<R> bind(Binding binding) {
        return new SlicePlan<>(rowClass, op, operands.get(0).bind(binding),
                               offset, limit, this, name);
    }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SlicePlan)) return false;
        if (!super.equals(o)) return false;
        SlicePlan<?> slicePlan = (SlicePlan<?>) o;
        return offset == slicePlan.offset && limit == slicePlan.limit && op.equals(slicePlan.op);
    }

    @Override public int hashCode() {
        return Objects.hash(super.hashCode(), op, offset, limit);
    }
}
