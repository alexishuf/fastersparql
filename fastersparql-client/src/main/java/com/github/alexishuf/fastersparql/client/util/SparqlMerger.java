package com.github.alexishuf.fastersparql.client.util;

import com.github.alexishuf.fastersparql.client.BindType;
import com.github.alexishuf.fastersparql.client.model.row.RowBinding;
import com.github.alexishuf.fastersparql.client.model.row.RowOperations;
import com.github.alexishuf.fastersparql.client.util.sparql.SparqlUtils;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

import static com.github.alexishuf.fastersparql.client.util.sparql.SparqlUtils.allVars;
import static com.github.alexishuf.fastersparql.client.util.sparql.SparqlUtils.publicVars;

public final class SparqlMerger<R> {
    private final Merger<R> merger;
    private final RowBinding<R> leftTempBinding;
    private final String sparql;
    private final boolean product;

    public SparqlMerger(SparqlMerger<R> other) {
        this.merger = other.merger;
        this.leftTempBinding = new RowBinding<>(other.rowOps(), other.leftTempBinding.vars());
        this.sparql = other.sparql;
        this.product = other.product;
    }

    public SparqlMerger(RowOperations rowOps, List<String> leftPublicVars,
                        CharSequence sparql, BindType bindType) {
        this.sparql = sparql.toString();
        List<String> rPub = publicVars(this.sparql), rAll = allVars(this.sparql);
        List<String> rFree = Merger.rightFreeVars(leftPublicVars, rPub);
        this.merger = Merger.forMerge(rowOps, leftPublicVars, rFree, bindType);
        this.leftTempBinding = new RowBinding<>(rowOps(), leftPublicVars);
        this.product = Merger.isProduct(leftPublicVars, rAll);
    }

    public RowOperations rowOps() { return merger.rowOps(); }
    public List<String> outVars()  { return merger.outVars(); }
    public boolean isTrivialLeft() { return merger.isTrivialLeft(); }
    public String        sparql()  { return sparql; }
    public boolean    isProduct()  { return product; }

    public CharSequence bindSparql(@Nullable R leftRow) {
        return SparqlUtils.bind(sparql, leftTempBinding.row(leftRow));
    }

    public R merge(@Nullable R left, @Nullable R right) { return merger.merge(left, right); }
}
