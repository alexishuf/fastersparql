package com.github.alexishuf.fastersparql.fed;

import com.github.alexishuf.fastersparql.batch.Timestamp;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import com.github.alexishuf.fastersparql.sparql.SparqlQuery;

import java.util.Objects;

import static java.lang.String.format;

@SuppressWarnings("unused")
public final class FedMetrics {
    public final Federation federation;
    public final SparqlQuery input;
    public Plan plan;
    public long dispatchNs;
    public long selectionAndAgglutinationNs;
    public long optimizationNs;

    public FedMetrics(Federation federation, SparqlQuery input) {
        this.federation = federation;
        this.input = input;
    }

    public Federation  federation() { return federation; }
    public SparqlQuery      input() { return input; }
    public Plan              plan() { return plan; }
    public long        dispatchNs() { return dispatchNs; }
    public long   agglutinationNs() { return selectionAndAgglutinationNs; }
    public long    optimizationNs() { return optimizationNs; }

    long addDispatch(long startNs) {
        long now = Timestamp.nanoTime();
        dispatchNs += now-startNs;
        return now;
    }

    long addSelectionAndAgglutination(long startNs) {
        long now = Timestamp.nanoTime();
        selectionAndAgglutinationNs += now-startNs;
        return now;
    }

    @SuppressWarnings("UnusedReturnValue")
    long addOptimization(long startNs) {
        long now = Timestamp.nanoTime();
        optimizationNs += now-startNs;
        return now;
    }

    public boolean equals(Object obj) {
        if (obj == this) return true;
        return obj instanceof FedMetrics r
                && r.federation == federation
                && r.input.equals(input)
                && r.plan.equals(plan)
                && r.dispatchNs == dispatchNs
                && r.selectionAndAgglutinationNs == selectionAndAgglutinationNs
                && r.optimizationNs == optimizationNs;
    }

    @Override
    public int hashCode() {
        return Objects.hash(federation, input, plan, dispatchNs, selectionAndAgglutinationNs, optimizationNs);
    }

    @Override
    public String toString() {
        return format("FedMetrics{dispatchMs=%.3f, selectionAndAgglutinationMs=%.3f, " +
                      "optimizationMs=%.3f, input=%s, federation=%s, plan=%s}",
                      dispatchNs/1_000_000.0,
                      selectionAndAgglutinationNs /1_000_000.0, optimizationNs/1_000_000.0,
                      input.sparql().toString().replace("\n", "\\n").replace("\r", "\\r"),
                      federation, plan);
    }

}
