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

    public static TripleRoleSet fromBitset(int bits) {
        return switch (bits) {
            case 0 -> EMPTY;
            case 1 -> OBJ;
            case 2 -> PRE;
            case 3 -> PRE_OBJ;
            case 4 -> SUB;
            case 5 -> SUB_OBJ;
            case 6 -> SUB_PRE;
            case 7 -> SUB_PRE_OBJ;
            default -> throw new IllegalArgumentException();
        };
    }

    public int asBitset() { return ordinal(); }
    public boolean hasObj() { return (ordinal()&0x1) != 0; }
    public boolean hasPre() { return (ordinal()&0x2) != 0; }
    public boolean hasSub() { return (ordinal()&0x4) != 0; }

    public TripleRoleSet invert() {
        return switch (this) {
            case EMPTY       -> SUB_PRE_OBJ;
            case OBJ         -> SUB_PRE;
            case PRE         -> SUB_OBJ;
            case PRE_OBJ     -> SUB;
            case SUB         -> PRE_OBJ;
            case SUB_OBJ     -> PRE;
            case SUB_PRE     -> OBJ;
            case SUB_PRE_OBJ -> EMPTY;
        };
    }

}
