package com.github.alexishuf.fastersparql.util.concurrent;

import jdk.jfr.*;

@Enabled
@Name("com.github.alexishuf.fastersparql.util.concurrent")
@Label("Pool stats")
@Description("Stats about pool usage")
@Registered
@StackTrace(value = false)
@Category({"FasterSparql"})
public class PoolStatsEvent extends Event {
    @Label("Pool")
    @Description("The name of the pool to which this stats correspond")
    public String poolName;

    @Label("Shared objects")
    @Description("Current number of pooled objects that have no thread affinity")
    public int sharedObjects;


    @Label("Local objects")
    @Description("Current number of pooled objects that have a thread affinity")
    public int localObjects;

    @Label("Local objects bytes")
    @Description("Current memory usage, in bytes, of all pooled objects that do have a thread affinity")
    public int localBytes;

    @Label("Shared objects bytes")
    @Description("Current memory usage, in bytes, of all pooled objects that have no thread affinity")
    public int sharedBytes;


    /** Fill all properties using {@code pool} and {@link #commit()} {@code this} */
    public void fillAndCommit(StatsPool pool) {
        poolName      = pool.name();
        sharedObjects = pool.sharedObjects();
        sharedBytes   = pool.sharedBytes();
        localObjects  = pool.localObjects();
        localBytes    = pool.localBytes();
        commit();
    }
}
