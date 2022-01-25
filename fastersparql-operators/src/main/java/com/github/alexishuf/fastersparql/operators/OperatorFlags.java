package com.github.alexishuf.fastersparql.operators;

@SuppressWarnings("PointlessBitwiseExpression")
public class OperatorFlags {
    /**
     * The first operand result set is small enough to fit in main memory.
     *
     * This may not be the case for the other operators.
     */
    public static final long SMALL_FIRST       = 0x1 << 0;

    /**
     * The second operand result set is small enough to fit in main memory.
     *
     * This flag should be used with {@link LeftJoin}, which is not comutative.
     * For comutative operators, the operands should be swapped and
     * {@link OperatorFlags#SMALL_FIRST} used instead.
     */
    public static final long SMALL_SECOND      = 0x1 << 1;

    /**
     * The first operand result set is certainly not small. No attempt should be made
     * to hold the entire set in main memory nor in disk
     */
    public static final long LARGE_FIRST       = 0x1 << 2;

    /**
     * The second operand result set is certainly not small. No attempt should be made
     * to hold the entire set in main memory nor in disk
     */
    public static final long LARGE_SECOND      = 0x1 << 3;

    /**
     * All operands result sets are small enough to fit in main memory
     */
    public static final long ALL_SMALL         = (0x1 << 4) | SMALL_FIRST | SMALL_SECOND;

    /**
     * All the operands result sets are large. No attempt should be made to hold any of them
     * in main memory or disk.
     */
    public static final long ALL_LARGE         = (0x1 << 5) | LARGE_FIRST | LARGE_SECOND;

    /**
     * The operator implementation should be able to use disk storage if there is not enough
     * main memory.
     */
    public static final long SPILLOVER         = 0x1 << 6;

    /**
     * The operator implementation should offload its own processing over rows to one or
     * more threads.
     *
     * Note that this will not offload execution of operands. For example an async {@link Filter}
     * will run filter expressions on a thread, while a non-async one will run filters within
     * {@link org.reactivestreams.Subscriber#onNext(Object)}.
     */
    public static final long ASYNC             = 0x1 << 7;

    /**
     * The operator is allowed to drop duplicate rows if that makes evaluation faster.
     */
    public static final long ALLOW_DEDUPLICATE = 0x1 << 8;

    /**
     * The operator is allowed to drop any row under any conditions if that makes evaluation faster.
     */
    public static final long ALLOW_INCOMPLETE  = (0x1 << 9) | ALLOW_DEDUPLICATE;

    /**
     * The operator is allowed to produce more duplicates of a row than the SPARQL semantics
     * would require if doing so makes evaluation faster.
     */
    public static final long ALLOW_DUPLICATES  = 0x1 << 10;

    /**
     * The operator is allowed to include rows not present according to the SPARQL semantics.
     *
     * Unlike {@link OperatorFlags#ALLOW_DUPLICATES}, this allows "adding" new rows completely,
     * for example, by not executing some filters.
     */
    public static final long ALLOW_UNSOUND     = (0x1 << 11) | ALLOW_DUPLICATES;


    /**
     * These are bits that can be used by user-defined flags.
     */
    public static final long USER_BITS = 0xffffffff00000000L << 5;

}
