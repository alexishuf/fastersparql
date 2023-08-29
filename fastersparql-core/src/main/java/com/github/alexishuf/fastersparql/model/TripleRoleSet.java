package com.github.alexishuf.fastersparql.model;

@SuppressWarnings("unused")
public enum TripleRoleSet {
    EMPTY,       //0b000 -> 0x0
    OBJ,         //0b001 -> 0x1
    PRE,         //0b010 -> 0x2
    PRE_OBJ,     //0b011 -> 0x3
    SUB,         //0b100 -> 0x4
    SUB_OBJ,     //0b101 -> 0x5
    SUB_PRE,     //0b110 -> 0x6
    SUB_PRE_OBJ; //0b111 -> 0x7

    public static final byte       EMPTY_BITS = 0x0; // 0b0000
    public static final byte         OBJ_BITS = 0x1; // 0b0001
    public static final byte         PRE_BITS = 0x2; // 0b0010
    public static final byte     PRE_OBJ_BITS = 0x3; // 0b0011
    public static final byte         SUB_BITS = 0x4; // 0b0100
    public static final byte     SUB_OBJ_BITS = 0x5; // 0b0101
    public static final byte     SUB_PRE_BITS = 0x6; // 0b0110
    public static final byte SUB_PRE_OBJ_BITS = 0x7; // 0b0111

    private static final TripleRoleSet[] VALUES = values();

    public static TripleRoleSet fromBitset(int bits) { return VALUES[bits]; }

    public TripleRoleSet withSub() { return VALUES[ordinal() | 0x4]; }
    public TripleRoleSet withPre() { return VALUES[ordinal() | 0x2]; }
    public TripleRoleSet withObj() { return VALUES[ordinal() | 0x1]; }

    public TripleRoleSet union(TripleRoleSet other) { return VALUES[ordinal() | other.ordinal()]; }
    public TripleRoleSet union(int bitset) { return VALUES[ordinal() | bitset]; }

    public int asBitset() { return ordinal(); }
    public boolean hasObj() { return (ordinal()&0x1) != 0; }
    public boolean hasPre() { return (ordinal()&0x2) != 0; }
    public boolean hasSub() { return (ordinal()&0x4) != 0; }

    public TripleRoleSet invert() { return VALUES[~ordinal() & 0x7]; }

}
