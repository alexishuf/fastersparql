package com.github.alexishuf.fastersparql.operators;

public class BidCosts {

    /**
     * Bids at this value will exclude the bidder.
     *
     * If all bidders bid this, no bidder will be left and operator creation will fail.
     */
    public static final int UNSUPPORTED = Integer.MAX_VALUE;

    /**
     * Every implementation in fastersparql-operators will always have this cost imbued.
     *
     * This is done so that low-cost implementations, expected to be near-optimal in
     * all scenarios can still be overridden by a user-defined implementation if that
     * implementation decides to not add this cost (or to add something smaller than this).
     */
    public static final int BUILTIN_COST = 1024;

    /**
     * If the operator implementation does not support some minor flag, such as
     * {@link OperatorFlags#ASYNC}, it should add this to its bid, once per unsupported feature.
     *
     * Not honoring flags that allow unsound/incomplete results should also increase the bid,
     * since the user wants to make that tradeoff. However not that the magnitude of this value
     * is small compared to {@link BidCosts#SLOW_COST} so that an implementation can ignore those
     * flags and still bid less if it is expected to be faster.
     */
    public static final int MINOR_COST = 128;

    /**
     * Given informational flags about the operands, a provider should estimate how slow it is
     * in a scale from 0 to 32 (with 0 being the fastest possible) and multiply that number by
     * this cost.
     *
     * A bidder may however be slower than {@code 32*SLOW_COST}. That is valid but will make it
     * even worse than a bidder that thinks it will run out of memory.
     */
    public static final int SLOW_COST = 1024;

    /**
     * This cost is added to the bid if the operator is likely to run out of memory.
     *
     * Implementations should multiply this cost by a number between 0 and 32 with 0 being
     * "plausible" risk and 32 "almost certain".
     */
    public static final int OOM_COST = 32*1024;
}
