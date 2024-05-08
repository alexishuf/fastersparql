package com.github.alexishuf.fastersparql.util.owned;

import jdk.jfr.*;

@Enabled
@Name("com.github.alexishuf.fastersparql.owned.Leak")
@Label("Owned leak")
@Description("A Owned instance became eligible for collection by the GC before it got recycle()d " +
             "or transferred to a LeakyOwner ")
@Registered
@StackTrace(false) // trace from LeakDetector.run() is not helpful
@Category({"FasterSparql", "Owned"})
public class OwnedLeak extends Event {
    @Label("leakedClassName")
    @Description("The getClass().getName() of the leaked object")
    public String leakedClassName;
}
