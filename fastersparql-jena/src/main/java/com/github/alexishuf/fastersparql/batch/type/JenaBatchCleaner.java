package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.org.apache.jena.graph.Node;

import java.util.function.Supplier;

public class JenaBatchCleaner extends ObjBatchCleaner<JenaBatch> {
    public static final JenaBatchCleaner INSTANCE = new JenaBatchCleaner();

    static final class NewJenaBatchFactory implements Supplier<JenaBatch> {
        @Override public JenaBatch get() {
            var terms = new Node[BatchType.PREFERRED_BATCH_TERMS];
            return new JenaBatch.Concrete(terms, 0, 1).takeOwnership(RECYCLED);
        }
        @Override public String toString() {return "NewJenaBatchFactory";}
    }
    private JenaBatchCleaner() {super("JenaBatchCleaner", new NewJenaBatchFactory());}
}
