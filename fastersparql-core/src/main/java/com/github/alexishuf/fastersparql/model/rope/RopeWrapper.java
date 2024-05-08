package com.github.alexishuf.fastersparql.model.rope;

import static com.github.alexishuf.fastersparql.model.rope.Rope.get;
import static com.github.alexishuf.fastersparql.model.rope.Rope.isEscaped;
import static java.lang.System.arraycopy;
import static java.nio.charset.StandardCharsets.UTF_8;

public enum RopeWrapper {
    NONE,
    LIT,
    OPEN_LIT,
    CLOSE_LIT,
    IRI,
    OPEN_IRI,
    CLOSE_IRI;

    private static int naturalLen(Object o) {
        return switch (o) {
            case byte[] a -> a.length;
            case Rope r -> r.len;
            case CharSequence cs -> cs.length();
            default -> throw new IllegalArgumentException("Expected byte[], Rope, or CharSequence");
        };
    }

    public static RopeWrapper forOpenLit(Object o, int begin, int end) {
        return end > begin && get(o, begin) == '"' ? NONE : OPEN_LIT;
    }
    public static RopeWrapper forOpenLit(Object o) { return forOpenLit(o, 0, naturalLen(o)); }

    public static RopeWrapper forLit(Object o, int begin, int end) {
        int lst = end - 1;
        boolean close = begin >= lst || get(o, lst) != '"' || isEscaped(o, begin, lst);
        if (end > begin && get(o, begin) == '"') return close ? CLOSE_LIT : NONE;
        else                                     return close ? LIT       : OPEN_LIT;
    }
    public static RopeWrapper forLit(Object o) { return forLit(o, 0, naturalLen(o)); }

    public static RopeWrapper forIri(Object o, int begin, int end) {
        boolean close = begin >= end || get(o, end-1) != '>';
        if (begin < end && get(o, begin) == '<') return close ? CLOSE_IRI : NONE;
        else                                     return close ? IRI       : OPEN_IRI;
    }
    public static RopeWrapper forIri(Object o) { return forIri(o, 0, naturalLen(o)); }

    public static RopeWrapper forCloseIri(Object o, int begin, int end) {
        return begin >= end || get(o, end-1) != '>' ? CLOSE_IRI : NONE;
    }
    @SuppressWarnings("unused")
    public static RopeWrapper forCloseIri(Object o) { return forCloseIri(o, 0, naturalLen(o));}

    public static byte[] asOpenLitU8(Object o, int begin, int end) {
        var w = end > begin && get(o, begin) == '"' ? NONE : OPEN_LIT;
        byte[] u8 = w.toBodyArray(o, begin, end);
        if (w == OPEN_LIT) u8[0] = '"';
        return u8;
    }

    public static byte[] asLitU8(Object o, int begin, int end) {
        int lst = end - 1;
        boolean close = begin >= lst || get(o, lst) != '"' || isEscaped(o, begin, lst);
        var w = end > begin && get(o, begin) == '"' ? close ? CLOSE_LIT : NONE
                                                    : close ? LIT       : OPEN_LIT;
        byte[] u8 = w.toBodyArray(o, begin, end);
        switch (w) {
            case CLOSE_LIT -> u8[u8.length-1] = '"';
            case OPEN_LIT  -> u8[0] = '"';
            case LIT       -> u8[0] = u8[u8.length-1] = '"';
        }
        return u8;
    }

    public static byte[] asIriU8(Object o, int begin, int end) {
        boolean close = begin >= end || get(o, end-1) != '>';
        var w = begin < end && get(o, begin) == '<' ? close ? CLOSE_IRI : NONE
                                                    : close ? IRI       : OPEN_IRI;
        byte[] u8 = w.toBodyArray(o, begin, end);
        switch (w) {
            case CLOSE_IRI -> u8[u8.length-1] = '>';
            case OPEN_IRI -> u8[0] = '<';
            case IRI -> {
                u8[0] = '<';
                u8[u8.length-1] = '>';
            }
        }
        return u8;
    }

    public static byte[] asCloseIriU8(Object o, int begin, int end) {
        var w = begin >= end || get(o, end - 1) != '>' ? CLOSE_IRI : NONE;
        byte[] u8 = w.toBodyArray(o, begin, end);
        if (w == CLOSE_IRI) u8[u8.length-1] = '>';
        return u8;
    }

    public int extraBytes() {
        return switch (this) {
            case NONE -> 0;
            case OPEN_LIT, CLOSE_LIT, OPEN_IRI, CLOSE_IRI -> 1;
            case LIT, IRI -> 2;
        };
    }

    private byte[] toBodyArray(Object o, int begin, int end) {
        if (!(o instanceof byte[]) && !(o instanceof Rope))
            return toBodyArrayObject(o, begin, end);
        int bodyLen = end-begin;
        byte[] a = o instanceof byte[] arr ? arr : null;
        if (this == NONE && a != null && bodyLen == a.length)
            return a;
        int body = switch (this) {
            case NONE, CLOSE_LIT, CLOSE_IRI -> 0;
            case LIT, OPEN_LIT, IRI, OPEN_IRI -> 1;
        };
        var u8 = new byte[bodyLen + extraBytes()];
        if (a == null) ((Rope)o).copy(begin, end, u8, body);
        else           arraycopy(a, begin, u8, body, bodyLen);
        return u8;
    }

    private byte[] toBodyArrayObject(Object o, int begin, int end) {
        byte[] u8 = o.toString().substring(begin, end).getBytes(UTF_8);
        int extra = switch (this) {
            case NONE -> 0;
            case IRI, LIT -> 2;
            case OPEN_IRI, CLOSE_IRI, OPEN_LIT, CLOSE_LIT -> 1;
        };
        if (extra == 0)
            return u8;
        int off = switch (this) { //noinspection ConstantConditions
            case NONE, CLOSE_IRI, CLOSE_LIT -> 0;
            case IRI, OPEN_IRI, LIT, OPEN_LIT -> 1;
        };
        byte[] padded = new byte[u8.length + extra];
        arraycopy(u8, 0, padded, off, u8.length);
        return padded;
    }

    public byte[] toArray(Object o) {
        return toArray(o, 0, naturalLen(o));
    }
    public byte[] toArray(Object o, int begin, int end) {
        var u8 = toBodyArray(o, begin, end);
        switch (this) {
            case LIT ->  u8[0] = u8[u8.length-1] = '"';
            case OPEN_LIT -> u8[0] = '"';
            case CLOSE_LIT -> u8[u8.length-1] = '"';
            case OPEN_IRI ->  u8[0] = '<';
            case CLOSE_IRI -> u8[u8.length-1] = '>' ;
            case IRI -> {
                u8[0] = '<';
                u8[u8.length-1] = '>';
            }
        }
        return u8;
    }

    public MutableRope append(MutableRope dest, Object o, int begin, int end) {
        dest.ensureFreeCapacity(extraBytes()+end-begin);
        switch (this) {
            case IRI, OPEN_IRI -> dest.append('<');
            case LIT, OPEN_LIT -> dest.append('"');
        }
        if (o instanceof byte[] u8) dest.append(u8, begin, end-begin);
        else                        dest.append((CharSequence)o, begin, end);
        switch (this) {
            case IRI, CLOSE_IRI -> dest.append('>');
            case LIT, CLOSE_LIT -> dest.append('"');
        }
        return dest;
    }

    public MutableRope append(MutableRope dest, Object o) {
        dest.ensureFreeCapacity(extraBytes()+naturalLen(o));
        switch (this) {
            case IRI, OPEN_IRI -> dest.append('<');
            case LIT, OPEN_LIT -> dest.append('"');
        }
        if (o instanceof byte[] u8) dest.append(u8);
        else                        dest.append((CharSequence)o);
        switch (this) {
            case IRI, CLOSE_IRI -> dest.append('>');
            case LIT, CLOSE_LIT -> dest.append('"');
        }
        return dest;
    }

    public RopeFactory append(RopeFactory dest, Rope o, int begin, int end) {
        switch (this) {
            case IRI, OPEN_IRI -> dest.add('<');
            case LIT, OPEN_LIT -> dest.add('"');
        }
        dest.add(o, begin, end);
        switch (this) {
            case IRI, CLOSE_IRI -> dest.add('>');
            case LIT, CLOSE_LIT -> dest.add('"');
        }
        return dest;
    }

    public RopeFactory append(RopeFactory dest, Object o, int begin, int end) {
        switch (this) {
            case IRI, OPEN_IRI -> dest.add('<');
            case LIT, OPEN_LIT -> dest.add('"');
        }
        if (o instanceof byte[] u8) dest.add(u8, begin, end);
        else                        dest.add((CharSequence)o, begin, end);
        switch (this) {
            case IRI, CLOSE_IRI -> dest.add('>');
            case LIT, CLOSE_LIT -> dest.add('"');
        }
        return dest;
    }

    public RopeFactory append(RopeFactory dest, CharSequence o) {
        switch (this) {
            case IRI, OPEN_IRI -> dest.add('<');
            case LIT, OPEN_LIT -> dest.add('"');
        }
        dest.add(o);
        switch (this) {
            case IRI, CLOSE_IRI -> dest.add('>');
            case LIT, CLOSE_LIT -> dest.add('"');
        }
        return dest;
    }

    public RopeFactory append(RopeFactory dest, Object o) {
        switch (this) {
            case IRI, OPEN_IRI -> dest.add('<');
            case LIT, OPEN_LIT -> dest.add('"');
        }
        if (o instanceof byte[] u8) dest.add(u8);
        else                        dest.add((CharSequence)o);
        switch (this) {
            case IRI, CLOSE_IRI -> dest.add('>');
            case LIT, CLOSE_LIT -> dest.add('"');
        }
        return dest;
    }
}
