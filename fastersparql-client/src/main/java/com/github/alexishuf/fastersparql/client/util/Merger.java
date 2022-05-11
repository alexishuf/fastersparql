package com.github.alexishuf.fastersparql.client.util;

import com.github.alexishuf.fastersparql.client.BindType;
import com.github.alexishuf.fastersparql.client.model.row.RowBinding;
import com.github.alexishuf.fastersparql.client.model.row.RowOperations;
import com.github.alexishuf.fastersparql.client.util.sparql.SparqlUtils;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.value.qual.MinLen;

import java.util.ArrayList;
import java.util.List;

import static com.github.alexishuf.fastersparql.client.util.sparql.SparqlUtils.allVars;
import static com.github.alexishuf.fastersparql.client.util.sparql.SparqlUtils.publicVars;

public class Merger<R> {
    protected final RowOperations rowOps;
    protected final List<String> outVars;
    protected final RowBinding<R> leftTempBinding;
    protected final String sparql;
    protected final int[] colSource;
    protected final boolean trivialLeft, product;

    public Merger(Merger<R> other) {
        this.rowOps          = other.rowOps;
        this.outVars         = other.outVars;
        this.leftTempBinding = new RowBinding<>(other.leftTempBinding.rowOps(),
                                                other.leftTempBinding.vars());
        this.sparql          = other.sparql;
        this.colSource       = other.colSource;
        this.trivialLeft     = other.trivialLeft;
        this.product         = other.product;
    }

    private static List<String> rightFreeVars(List<String> leftPublicVars,
                                              List<String> rightPublicVars) {
        ArrayList<String> copy = new ArrayList<>(rightPublicVars);
        boolean change = false;
        for (int i = copy.size()-1; i >= 0; i--) {
            if (leftPublicVars.contains(copy.get(i))) {
                copy.remove(i);
                change = true;
            }
        }
        return change ? copy : rightPublicVars;
    }

    private static boolean isProduct(List<String> leftPublicVars, List<String> rightAllVars) {
        for (String name : rightAllVars) {
            if (leftPublicVars.contains(name))
                return false;
        }
        return true;
    }

    public Merger(RowOperations rowOps, List<String> leftVars, CharSequence sparql,
                  List<String> rightPublicVars, List<String> rightAllVars, BindType bindType) {
        this.rowOps = rowOps;
        this.leftTempBinding = new RowBinding<>(rowOps, leftVars);
        this.sparql = sparql.toString();
        this.product = isProduct(leftVars, rightAllVars);
        List<@MinLen(1) String> rightFreeVars = rightFreeVars(leftVars, rightPublicVars);
        if (bindType.isJoin()) {
            this.outVars = new ArrayList<>(leftVars.size() + rightFreeVars.size());
            this.outVars.addAll(leftVars);
            this.outVars.addAll(rightFreeVars);
        } else {
            this.outVars = leftVars;
        }
        this.colSource = new int[outVars.size()];
        boolean allLeft = true;
        for (int i = 0; i < colSource.length; i++) {
            String name = outVars.get(i);
            int idx = leftVars.indexOf(name);
            if (idx >= 0) {
                colSource[i] = idx + 1;
            } else {
                allLeft = false;
                colSource[i] = -(rightFreeVars.indexOf(name) + 1);
            }
        }
        this.trivialLeft = allLeft;
    }

    public Merger(RowOperations rowOps, List<String> leftVars,
                  CharSequence sparql, BindType bindType) {
        this(rowOps, leftVars, sparql, publicVars(sparql), allVars(sparql), bindType);
    }

    public RowOperations rowOps() { return rowOps; }
    public List<String> outVars() { return outVars; }
    public boolean    isProduct() { return product; }

    public CharSequence bindSparql(@Nullable R leftRow) {
        return SparqlUtils.bind(sparql, leftTempBinding.row(leftRow));
    }

    @SuppressWarnings("unchecked")
    public R merge(@Nullable R left, @Nullable R right) {
        if (trivialLeft && left != null)
            return left;
        R merged = (R) rowOps.createEmpty(outVars);
        for (int i = 0; i < colSource.length; i++) {
            int idx = colSource[i];
            if (idx != 0) {
                String var = outVars.get(i);
                R source = idx > 0 ? left : right;
                int sourceIdx = Math.abs(idx) - 1;
                rowOps.set(merged, i, var, rowOps.getNT(source, sourceIdx, var));
            }
        }
        return merged;
    }
}
