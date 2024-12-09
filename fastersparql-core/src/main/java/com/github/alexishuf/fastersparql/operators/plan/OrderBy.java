package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.batch.dedup.SortProjection;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.FinalSegmentRope;
import com.github.alexishuf.fastersparql.model.rope.MutableRope;
import com.github.alexishuf.fastersparql.sparql.expr.InvalidExprTypeException;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import com.github.alexishuf.fastersparql.util.BS;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;
import java.util.Objects;

public class OrderBy {
    private int size;
    private @Nullable FinalSegmentRope var0;
    private long desc0;
    private FinalSegmentRope @Nullable[] vars;
    private long @Nullable[] desc;

    public OrderBy() {}

    public OrderBy(OrderBy o) {
        size  = o.size;
        var0  = o.var0;
        desc0 = o.desc0;
        vars  = o.vars == null ? null : Arrays.copyOf(o.vars, o.vars.length);
        desc  = o.desc == null ? null : Arrays.copyOf(o.desc, o.desc.length);
    }

    public int        size()      {return size;}
    public boolean isEmpty()      {return size == 0;}
    public boolean  isAsc (int i) {return !isDesc(i);}

    public static boolean  isEmpty(@Nullable OrderBy o) {return o == null ||  o.isEmpty();}
    public static boolean nonEmpty(@Nullable OrderBy o) {return o != null && !o.isEmpty();}

    public FinalSegmentRope get(int i) {
        if (i < 0 || i > size)
            throw new IndexOutOfBoundsException(i);
        if (vars != null)
            return vars[i];
        return var0;
    }

    public boolean isDesc(int i) {
        if (i < 0 || i > size)
            throw new IndexOutOfBoundsException(i);
        long word = desc == null ? desc0 : desc[i >> 6];
        return (word&(1L<<i)) != 0;
    }

    public boolean intersects(@Nullable Vars vars) {
        if (vars == null) return false;
        for (int i = 0; i < size; i++) {
            if (vars.contains(get(i))) return true;
        }
        return false;
    }

    public OrderBy minus(@Nullable Vars vars) {
        if (vars == null || vars.isEmpty())
            return this;
        OrderBy subset = new OrderBy();
        int removed = 0;
        for (int i = 0; i < size; i++) {
            var v = get(i);
            if (!vars.contains(v)) subset.add(v, isDesc(i));
            else                   ++removed;
        }
        if      (removed ==    0) return this;
        else if (removed == size) return null;
        else                      return subset;
    }

    public void add(Term var, boolean descending) {
        if (!var.isVar())
            throw new InvalidExprTypeException(var, var, "Var");
        add(var.name(), descending);
    }

    public void add(FinalSegmentRope name, boolean descending) {
        int size = this.size, idx = 0;
        if (size == 0) {
            var0 = name;
        } else if (size == 1) {
            if (!name.equals(var0)) {
                var a = new FinalSegmentRope[10];
                vars = a;
                a[    0] = var0;
                a[idx=1] = name;
            }
        } else {
            var arr = Objects.requireNonNull(this.vars);
            for (; idx < size; ++idx) {
                if (arr[idx].equals(name))
                    break; // found name, idx < size
            }
            if (idx == size) { // if name is new
                if (size >= arr.length) // if storage is full
                    arr = grow(arr);
                arr[size] = name;
            }
        }
        if (idx == size) // if name is new
            this.size = ++size;
        // set/clear descending bit
        long mask = 1L<<idx, set = descending ? mask : 0;
        if (desc != null) desc[idx>>>6] = (desc[idx>>>6]&~mask) | set;
        else              desc0         = (desc0        &~mask) | set;
    }

    private FinalSegmentRope[] grow(FinalSegmentRope[] varNames) {
        int capacity = varNames.length + (varNames.length >> 1);
        varNames = Arrays.copyOf(varNames, capacity);
        int required = BS.longsFor(capacity);
        if (desc != null) {
            if (required > desc.length)
                desc = Arrays.copyOf(desc, required);
        } else if (required > 64) {
            (desc = new long[required])[0] = desc0;
        }
        return varNames;
    }

    @Override public boolean equals(Object obj) {
        if (!(obj instanceof OrderBy r)) return false;
        if (r.size != size) return false;
        for (int i = 0; i < size; i++) {
            if (!get(i).equals(r.get(i))) return false;
            if (isDesc(i) != r.isDesc(i)) return false;
        }
        return true;
    }

    @Override public int hashCode() {
        int h = FinalSegmentRope.FNV_BASIS;
        for (int i = 0; i < size; i++) {
            if (isDesc(i)) h = DESC.hash(h);
            h = get(i).hash(h);
        }
        return h;
    }
    private static final FinalSegmentRope DESC = FinalSegmentRope.asFinal("DESC");

    public void append(MutableRope out) {
        if (size == 0)
            return;
        out.append("ORDER BY ");
        for (int i = 0; i < size; i++) {
            if (isDesc(i)) out.append("DESC ");
            out.append('?').append(get(i)).append(' ');
        }
        out.len--;
    }

    @Override public String toString() {
        if (size == 0)
            return "";
        var sb = new StringBuilder();
        sb.append("ORDER BY ");
        for (int i = 0; i < size; i++) {
            if (isDesc(i)) sb.append("DESC ");
            sb.append('?').append(get(i)).append(' ');
        }
        sb.setLength(sb.length()-1);
        return sb.toString();
    }

    public <P extends SortProjection<P>> SortProjection.Builder<P>
    fill(Vars allVars, SortProjection.Builder<P> builder) {
        for (int i = 0; i < size; i++) {
            FinalSegmentRope name = get(i);
            int col = allVars.indexOf(name);
            boolean desc = isDesc(i);
            if (col < 0)
                throw mkBogusVarException(desc, name);
            builder.add(col, desc);
        }
        return builder;
    }

    private static IllegalArgumentException
    mkBogusVarException(boolean des, FinalSegmentRope name) {
        String dir = des ? "DESC " : "ASC ";
        return new IllegalArgumentException("ORDER BY "+dir+name+": bogus variable");
    }

}
