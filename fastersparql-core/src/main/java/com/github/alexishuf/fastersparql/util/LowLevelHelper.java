package com.github.alexishuf.fastersparql.util;

import com.github.alexishuf.fastersparql.FSProperties;
import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.VectorSpecies;
import sun.misc.Unsafe;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;

public class LowLevelHelper {
    public static final boolean ENABLE_VEC = FSProperties.useVectorization();
    public static final boolean ENABLE_UNSAFE = FSProperties.useUnsafe();
    public static final int U8_BASE;
    public static final boolean HAS_UNSAFE;
    public static final Unsafe U;

    public static final VectorSpecies<Byte>    B_SP = ByteVector.SPECIES_PREFERRED;
    public static final VectorSpecies<Integer> I_SP = IntVector.SPECIES_PREFERRED;
    public static final VectorSpecies<Long>    L_SP = LongVector.SPECIES_PREFERRED;
    public static final int B_LEN = B_SP.length();
    public static final int I_LEN = I_SP.length();

    static {
        Unsafe u = null;
        if (ENABLE_UNSAFE) {
            try {
                Field field = Unsafe.class.getDeclaredField("theUnsafe");
                field.setAccessible(true);
                u = (Unsafe) field.get(null);
            } catch (Throwable ignored) {
                try {
                    Constructor<Unsafe> c = Unsafe.class.getDeclaredConstructor();
                    u = c.newInstance();
                } catch (Throwable ignored1) {}
            }
        }
        U = u;
        HAS_UNSAFE = u != null;
        U8_BASE = u == null ? 0 : u.arrayBaseOffset(byte[].class);
    }
}
