package com.github.alexishuf.fastersparql.batch.type;


import com.github.alexishuf.fastersparql.org.apache.jena.graph.Node;

public class JenaBatchType extends ObjBatchType<Node, JenaBatch> {
    public static final JenaBatchType JENA = new JenaBatchType();

    public static JenaBatchType get() { return JENA; }

    public JenaBatchType() {
        super(JenaBatch.class, Node.class, JenaBatchCleaner.INSTANCE.newInstance,
              JenaBatchCleaner.INSTANCE.clearElseMake, JenaBatch.BYTES);
        JenaBatchCleaner.INSTANCE.pool = this.pool;
    }
}
