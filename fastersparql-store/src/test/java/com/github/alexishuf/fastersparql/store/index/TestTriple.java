package com.github.alexishuf.fastersparql.store.index;

import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.IOException;

record TestTriple(long s, long p, long o) implements Comparable<TestTriple> {
    @Override public String toString() {
        return "(" + s + " " + p + " " + o + ")";
    }

    public void addTo(TriplesBlock  block )                    { block .add      (s, p, o); }
    public void addTo(TriplesSorter sorter) throws IOException { sorter.addTriple(s, p, o); }

    @Override public int compareTo(@NonNull TestTriple rhs) {
        int d = Long.compare(s, rhs.s);
        if (d != 0) return d;
        d = Long.compare(p, rhs.p);
        if (d != 0) return d;
        return Long.compare(o, rhs.o);
    }

    public TestTriple spo2pso() {
        return new TestTriple(p, s, o);
    }

    public TestTriple pso2ops() {
        return new TestTriple(o, s, p);
    }
}
