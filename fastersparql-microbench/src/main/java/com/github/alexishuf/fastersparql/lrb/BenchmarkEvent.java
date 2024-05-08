package com.github.alexishuf.fastersparql.lrb;

import jdk.jfr.*;

@Registered
@Enabled
@StackTrace(value = false)
@Label("Benchmark Iteration")
@Category({"FasterSparql", "JMH"})
public class BenchmarkEvent extends Event {
    @Label("Benchmark class")
    @Description("Fully qualified class name of the JMH benchmark class")
    public String className;

    @Label("Iteration")
    @Description("The iteration sequence number. This starts at zero and grows without " +
                 "resetting once warmup iterations are complete")
    public int iterationNumber;

    @Label("Warmup")
    @Description("Whether this benchmark iteration was a warmup iteration")
    public boolean warmup;
}
