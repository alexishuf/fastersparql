package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.client.util.CSUtils;
import lombok.EqualsAndHashCode;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

@EqualsAndHashCode
abstract class AbstractPlan<R, Implementation extends Plan<R>> implements Plan<R> {
    protected final Class<? super R> rowClass;
    protected final List<? extends Plan<R>> operands;
    protected final @Nullable Implementation parent;
    protected final String name;

    public AbstractPlan(@lombok.NonNull Class<? super R> rowClass,
                        @lombok.NonNull List<? extends Plan<R>> operands,
                        @lombok.NonNull String name, @Nullable Implementation parent) {
        this.parent   = parent;
        this.rowClass = rowClass;
        this.operands = operands;
        this.name     = name;
    }

    @Override public @Nullable Plan<R>       parent()     { return parent; }
    @Override public String                  name()       { return name; }
    @Override public Class<? super R>        rowClass()   { return rowClass; }
    @Override public List<? extends Plan<R>> operands()   { return operands; }

    protected String algebraName() {
        return getClass().getSimpleName().replace("Plan", "");
    }

    private StringBuilder indent(StringBuilder sb, String string) {
        for (int start = 0, i, len = string.length(); start < len; start = i+1) {
            i = CSUtils.skipUntil(string, start, '\n');
            sb.append("  ");
            sb.append(string, start, i);
            sb.append('\n');
        }
        sb.setLength(Math.max(0, sb.length()-1));
        return sb;
    }

    @Override public String toString() {
        if (operands.isEmpty())
            return algebraName();
        StringBuilder sb = new StringBuilder().append(algebraName()).append("(\n");
        for (Plan<R> op : operands)
            indent(sb, op.toString()).append(",\n");
        sb.setLength(sb.length()-2);
        return sb.append("\n)").toString();
    }
}
