package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.client.util.CSUtils;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Objects;

abstract class AbstractPlan<R, Implementation extends Plan<R>> implements Plan<R> {
    protected final Class<? super R> rowClass;
    protected final List<? extends Plan<R>> operands;
    protected final @Nullable Implementation parent;
    protected final String name;

    public AbstractPlan(Class<? super R> rowClass, List<? extends Plan<R>> operands,
                        String name, @Nullable Implementation parent) {
        if (rowClass == null || operands == null || name == null)
            throw new NullPointerException("rowClass, operands and name cannot be null");
        for (Plan<R> o : operands) {
            if (o == null) throw new IllegalArgumentException("operands list contains a null");
        }
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

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AbstractPlan)) return false;
        AbstractPlan<?, ?> that = (AbstractPlan<?, ?>) o;
        return rowClass.equals(that.rowClass) && operands.equals(that.operands) && Objects.equals(parent, that.parent) && name.equals(that.name);
    }

    @Override public int hashCode() {
        return Objects.hash(rowClass, operands, parent, name);
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
