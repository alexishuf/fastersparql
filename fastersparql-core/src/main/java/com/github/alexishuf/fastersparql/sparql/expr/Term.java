package com.github.alexishuf.fastersparql.sparql.expr;

import com.github.alexishuf.fastersparql.sparql.RDFTypes;
import com.github.alexishuf.fastersparql.sparql.binding.Binding;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.Objects;

import static com.github.alexishuf.fastersparql.sparql.RDFTypes.*;

public sealed interface Term extends Expr {
    String nt();

    /** Implements SPARQL {@code str()} function */
    Lit str();

    /** Interpret this as a boolean or raise an {@link InvalidExprTypeException} */
    boolean asBool();

    @Override default int  argCount()            { return 0; }
    @Override default Expr arg(int i)            { throw new IndexOutOfBoundsException(i); }
    @Override default Term eval(Binding ignored) { return this; }

    @Override default Expr bind(Binding ignored) { return this; }

    final class Var implements Term {
        private final String nt;
        private @Nullable String name;

        public Var(String nt) { this.nt = nt; }

        public String name() {
            return name == null ? name = nt.substring(1) : name;
        }

        @Override public Term eval(Binding binding) {
            Term value = binding.parse(name());
            if (value == null) throw new UnboundVarException(this);
            return value;
        }

        @Override public Expr bind(Binding binding) {
            Term value = binding.parse(name());
            return value == null ? this : value;
        }

        @Override public String       nt() { return nt; }
        @Override public Lit         str() { throw new UnboundVarException(this); }
        @Override public boolean  asBool() { throw new UnboundVarException(this); }
        @Override public int    hashCode() { return nt.hashCode(); }
        @Override public String toString() { return nt; }
        @Override public boolean equals(Object o) {
            return this == o || (o instanceof Var v && nt.equals(v.nt));
        }
    }

    final class IRI implements Term {
        private final String nt;
        private @Nullable Lit str;

        public IRI(String nt) {
            this.nt = nt;
        }

        @Override public Lit str() {
            if (str == null)
                str = new Lit(nt.substring(1, nt.length() - 1), string, null);
            return str;
        }

        @Override public boolean asBool() {
            throw new IllegalArgumentException("BNodes do not have a boolean value");
        }

        @Override public String nt() { return nt; }
        @Override public int hashCode() { return Objects.hash(nt); }
        @Override public String toString() { return nt; }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) return true;
            if (obj == null || obj.getClass() != this.getClass()) return false;
            var that = (IRI) obj;
            return Objects.equals(this.nt, that.nt);
        }
    }

    final class BNode implements Term {
        private final String nt;
        private @Nullable Lit str;

        public BNode(String nt) { this.nt = nt; }

        @Override public Lit str() {
            if (str == null)
                str = new Lit(nt.substring(2), string, null);
            return str;
        }

        @Override public boolean asBool() {
            throw new IllegalArgumentException("BNodes do not have a boolean value");
        }


        @Override public String nt() { return nt; }
        @Override public String toString() { return nt; }
        @Override public int hashCode() { return Objects.hash(nt); }

        @Override public boolean equals(Object obj) {
            if (obj == this) return true;
            if (obj == null || obj.getClass() != this.getClass()) return false;
            var that = (BNode) obj;
            return Objects.equals(this.nt, that.nt);
        }

    }


    final class Lit implements Term {
        private final String lexical;
        private final String datatype;
        private final @Nullable String lang;
        private @Nullable Number number;
        private boolean numberParsed = false;
        private @Nullable Boolean bool;
        private @Nullable String nt;

        public static final Lit TRUE = new Lit("true", RDFTypes.BOOLEAN, null);
        public static final Lit FALSE = new Lit("false", RDFTypes.BOOLEAN, null);
        public static final Lit EMPTY = new Lit("", RDFTypes.string, null);

        public Lit(String lexical, String datatype, @Nullable String lang) {
            this.lexical = lexical;
            this.datatype = datatype;
            this.lang = lang;
        }

        /** The lexical form of the literal with escape sequences used in {@link Lit#nt()}
         *  but without the surrounding quotes */
        public           String  lexical() { return lexical; }
        /** The full datatype IRI, but without the surrounding {@code <>}'s. */
        public           String datatype() { return datatype; }
        /** The language tag or {@code null} if {@link Lit#datatype()} is not
         *  {@link RDFTypes#langString}. */
        public @Nullable String     lang() { return lang; }

        public static Lit valueOf(Object o) {
            return switch (o) {
                case null -> null;
                case Boolean b -> b ? TRUE : FALSE;
                case BigDecimal d -> new Lit(d.toString(), RDFTypes.decimal, null);
                case BigInteger d -> new Lit(d.toString(), RDFTypes.integer, null);
                case Double d -> new Lit(d.toString(), RDFTypes.DOUBLE, null);
                case Float d -> new Lit(d.toString(), RDFTypes.FLOAT, null);
                case Long d -> new Lit(d.toString(), RDFTypes.LONG, null);
                case Integer d -> new Lit(d.toString(), RDFTypes.INT, null);
                case Short d -> new Lit(d.toString(), RDFTypes.SHORT, null);
                case Byte d -> new Lit(d.toString(), RDFTypes.BYTE, null);
                case String s -> new Lit(s, string, null);
                default -> throw new IllegalArgumentException("Cannot convert "+o+" into a literal");
            };
        }

        @Override public String nt() {
            if (this.nt == null) {
                if (isXsd(datatype, string))
                    nt = "\"" + lexical + "\"";
                else if (lang != null)
                    nt = "\"" + lexical + "\"@" + lang;
                else
                    nt = "\"" + lexical + "\"^^<" + datatype + ">";
            }
            return nt;
        }

        @Override public Lit str() {
            if (RDFTypes.isXsd(datatype, string))
                return this;
            return new Lit(lexical, string, null);
        }

        @Override public boolean asBool() {
            if (bool != null)
                return bool;
            if (RDFTypes.isXsd(datatype, RDFTypes.BOOLEAN)) {
                char c = lexical.charAt(0);
                return bool = c == 't' || c == 'T';
            } else if (RDFTypes.isXsd(datatype, string) || lang != null) {
                return bool = !lexical.isEmpty();
            } else {
                Number n = asNumber();
                return bool = switch (n) {
                    case null -> throw new InvalidExprTypeException("No boolean value for "+this);
                    case BigInteger i -> !i.equals(BigInteger.ZERO);
                    case BigDecimal i -> !i.equals(BigDecimal.ZERO);
                    case Double d     -> !d.isNaN() && d != 0;
                    case Float d      -> !d.isNaN() && d != 0;
                    default           -> n.longValue() != 0;
                };
            }
        }

        public @Nullable Number asNumber() {
            if (numberParsed)
                return number;
            int hash = RDFTypes.XSD.length() - 1;
            try {
                if (datatype.charAt(hash) == '#' && datatype.startsWith(RDFTypes.XSD)) {
                    char c1 = datatype.charAt(hash + 1),
                            c2 = datatype.charAt(hash + 2), c3 = datatype.charAt(hash + 3);
                    if (c1 == 'i' && datatype.length() == RDFTypes.INT.length()) {
                        number = Integer.valueOf(lexical);
                    } else if (c1 == 'd' && c2 == 'o') {
                        number = Double.valueOf(lexical);
                    } else if (c1 == 'd' && c2 == 'e') {
                        number = new BigDecimal(lexical);
                    } else if (c1 == 'u' || c1 == 'i' || c1 == 'p'
                            || (c1 == 'n' && (c3 == 'n' || c3 == 'g'))) {
                        number = new BigInteger(lexical);
                    } else if (c1 == 's' && c2 == 'h') {
                        number = Short.valueOf(lexical);
                    } else if (c1 == 'b' && c2 == 'y') {
                        number = Byte.valueOf(lexical);
                    } else if (c1 == 'l' && c2 == 'o') {
                        number = Long.valueOf(lexical);
                    } else if (c1 == 'f') {
                        number = Float.valueOf(lexical);
                    }
                }

            } catch (NumberFormatException e) {
                throw new ExprEvalException("Lexical form " + lexical + " is not valid for " + datatype + ": " + e.getMessage());
            } finally {
                numberParsed = true;
            }
            return number;
        }

        public int asInt() {
            Number n = asNumber();
            if (n == null) throw new ExprEvalException(this+" is not a numeric literal");
            long l = n.longValue();
            if (l < Integer.MIN_VALUE || l > Integer.MAX_VALUE)
                throw new ExprEvalException(this+" overflows as int");
            return (int)l;
        }

        private Number requireNumeric(String caller) {
            Number n = asNumber();
            if (n == null)
                throw new ExprEvalException(caller + " not defined for " + this);
            return n;
        }
        private static BigDecimal asBigDecimal(Number n, Number other) {
            int scale = Math.max(n     instanceof BigDecimal d ? d.scale() : 0,
                                 other instanceof BigDecimal d ? d.scale() : 0);
            if (scale == 0)
                scale = 8;
            if (n instanceof BigDecimal d)
                return d.setScale(scale, RoundingMode.HALF_DOWN);
            BigDecimal d = new BigDecimal(n.toString());
            if (d.scale() < scale)
                d = d.setScale(scale, RoundingMode.HALF_DOWN);
            return d;
        }

        private static BigInteger asBigInteger(Number n) {
            return switch (n) {
                case BigInteger b -> b;
                case BigDecimal d -> d.toBigInteger();
                default -> BigInteger.valueOf(n.longValue());
            };
        }

        public int compareTo(Lit rhs) {
            Number l = requireNumeric("compareTo"), r = rhs.requireNumeric("compareTo");
            if (l instanceof BigDecimal || r instanceof BigDecimal)
                return asBigDecimal(l, r).compareTo(asBigDecimal(r, l));
            else if (l instanceof BigInteger || r instanceof BigInteger)
                return asBigDecimal(l, r).compareTo(asBigDecimal(r, l));
            else if (l instanceof Double || r instanceof Double || l instanceof Float || r instanceof Float)
                return Double.compare(l.doubleValue(), r.doubleValue());
            else
                return Long.compare(l.longValue(), r.longValue());
        }

        public Lit add(Lit rhs) {
            Number l = requireNumeric("add"), r = rhs.requireNumeric("add");
            Number result;
            String datatype;
            if (l instanceof BigDecimal || r instanceof BigDecimal) {
                result = asBigDecimal(l, r).add(asBigDecimal(r, l));
                datatype = decimal;
            } else if (l instanceof BigInteger || r instanceof BigInteger) {
                result = asBigInteger(l).add(asBigInteger(r));
                datatype = integer;
            } else if (l instanceof Double || r instanceof Double) {
                result = l.doubleValue() + r.doubleValue();
                datatype = DOUBLE;
            } else if (l instanceof Float || r instanceof Float) {
                result = l.floatValue() + r.floatValue();
                datatype = FLOAT;
            } else if (l instanceof Long || r instanceof Long) {
                result = l.longValue() + r.longValue();
                datatype = LONG;
            } else if (l instanceof Integer || r instanceof Integer) {
                result = l.intValue() + r.intValue();
                datatype = INT;
            } else if (l instanceof Short || r instanceof Short) {
                result = l.shortValue() + r.shortValue();
                datatype = SHORT;
            } else if (l instanceof Byte || r instanceof Byte) {
                result = l.byteValue() + r.byteValue();
                datatype = BYTE;
            } else {
                result = l.doubleValue() + r.doubleValue();
                datatype = DOUBLE;
            }
            return new Lit(result.toString(), datatype, lang);
        }

        public Lit subtract(Lit rhs) {
            Number l = requireNumeric("subtract"), r = rhs.requireNumeric("subtract");
            Number result;
            String datatype;
            if (l instanceof BigDecimal || r instanceof BigDecimal) {
                result = asBigDecimal(l, r).subtract(asBigDecimal(r, l));
                datatype = decimal;
            } else if (l instanceof BigInteger || r instanceof BigInteger) {
                result = asBigInteger(l).subtract(asBigInteger(r));
                datatype = integer;
            } else if (l instanceof Double || r instanceof Double) {
                result = l.doubleValue() - r.doubleValue();
                datatype = DOUBLE;
            } else if (l instanceof Float || r instanceof Float) {
                result = l.floatValue() - r.floatValue();
                datatype = FLOAT;
            } else if (l instanceof Long || r instanceof Long) {
                result = l.longValue() - r.longValue();
                datatype = LONG;
            } else if (l instanceof Integer || r instanceof Integer) {
                result = l.intValue() - r.intValue();
                datatype = INT;
            } else if (l instanceof Short || r instanceof Short) {
                result = l.shortValue() - r.shortValue();
                datatype = SHORT;
            } else if (l instanceof Byte || r instanceof Byte) {
                result = l.byteValue() - r.byteValue();
                datatype = BYTE;
            } else {
                result = l.doubleValue() - r.doubleValue();
                datatype = DOUBLE;
            }
            return new Lit(result.toString(), datatype, lang);
        }

        public Lit negate() {
            Number n = asNumber();
            if (n != null) {
                if (lexical.charAt(0) == '-')
                    return new Lit(lexical.substring(1), datatype, lang);
                else
                    return new Lit("-"+lexical, datatype, lang);
            }
            return asBool() ? FALSE : TRUE;
        }

        public Lit multiply(Lit rhs) {
            Number l = requireNumeric("multiply"), r = rhs.requireNumeric("multiply");
            Number result;
            String datatype;
            if (l instanceof BigDecimal || r instanceof BigDecimal) {
                result = asBigDecimal(l, r).multiply(asBigDecimal(r, l));
                datatype = decimal;
            } else if (l instanceof BigInteger || r instanceof BigInteger) {
                result = asBigInteger(l).multiply(asBigInteger(r));
                datatype = integer;
            } else if (l instanceof Double || r instanceof Double) {
                result = l.doubleValue() * r.doubleValue();
                datatype = DOUBLE;
            } else if (l instanceof Float || r instanceof Float) {
                result = l.floatValue() * r.floatValue();
                datatype = FLOAT;
            } else if (l instanceof Long || r instanceof Long) {
                result = l.longValue() * r.longValue();
                datatype = LONG;
            } else if (l instanceof Integer || r instanceof Integer) {
                result = l.intValue() * r.intValue();
                datatype = INT;
            } else if (l instanceof Short || r instanceof Short) {
                result = l.shortValue() * r.shortValue();
                datatype = SHORT;
            } else if (l instanceof Byte || r instanceof Byte) {
                result = l.byteValue() * r.byteValue();
                datatype = BYTE;
            } else {
                result = l.doubleValue() * r.doubleValue();
                datatype = DOUBLE;
            }
            return new Lit(result.toString(), datatype, lang);
        }

        public Lit divide(Lit rhs) {
            Number l = requireNumeric("divide"), r = rhs.requireNumeric("divide");
            Number result;
            String datatype;
            if (l instanceof BigDecimal || r instanceof BigDecimal) {
                result = asBigDecimal(l, r).divide(asBigDecimal(r, l), RoundingMode.HALF_DOWN);
                datatype = decimal;
            } else if (l instanceof BigInteger || r instanceof BigInteger) {
                result = asBigInteger(l).divide(asBigInteger(r));
                datatype = integer;
            } else if (l instanceof Double || r instanceof Double) {
                result = l.doubleValue() / r.doubleValue();
                datatype = DOUBLE;
            } else if (l instanceof Float || r instanceof Float) {
                result = l.floatValue() / r.floatValue();
                datatype = FLOAT;
            } else if (l instanceof Long || r instanceof Long) {
                result = l.longValue() / r.longValue();
                datatype = LONG;
            } else if (l instanceof Integer || r instanceof Integer) {
                result = l.intValue() / r.intValue();
                datatype = INT;
            } else if (l instanceof Short || r instanceof Short) {
                result = l.shortValue() / r.shortValue();
                datatype = SHORT;
            } else if (l instanceof Byte || r instanceof Byte) {
                result = l.byteValue() / r.byteValue();
                datatype = BYTE;
            } else {
                result = l.doubleValue() / r.doubleValue();
                datatype = DOUBLE;
            }
            return new Lit(result.toString(), datatype, lang);

        }

        public Lit abs() {
            requireNumeric("abs");
            if (lexical.charAt(0) == '-')
                return new Lit(lexical.substring(1), datatype, lang);
            return this;
        }

        public Lit ceil() {
            Number n = requireNumeric("ceil");
            String result = (switch (n) {
                case BigDecimal b -> b.setScale(0, RoundingMode.UP);
                case Double d     -> Math.ceil(d);
                case Float d      -> Math.ceil(d);
                default           -> n;
            }).toString();
            return new Lit(result, datatype, lang);
        }

        public Lit floor() {
            Number n = requireNumeric("floor");
            String result = (switch (n) {
                case BigDecimal b -> b.setScale(0, RoundingMode.DOWN);
                case Double d     -> Math.floor(d);
                case Float d      -> Math.floor(d);
                default           -> n;
            }).toString();
            return new Lit(result, datatype, lang);
        }

        public Lit round() {
            Number n = requireNumeric("floor");
            String result = (switch (n) {
                case BigDecimal b -> b.setScale(0, RoundingMode.HALF_DOWN);
                case Double d     -> Math.floor(d);
                case Float d      -> Math.floor(d);
                default           -> n;
            }).toString();
            return new Lit(result, datatype, lang);
        }


        @Override public boolean equals(Object obj) {
            if (obj == this) return true;
            if (!(obj instanceof Lit lit)) return false;
            Number number = asNumber();
            if (number != null)
                return compareTo(lit) == 0;
            return lexical.equals(lit.lexical) && datatype.equals(lit.datatype)
                    && Objects.equals(lang, lit.lang);
        }

        @Override public int hashCode() {
            Number number = asNumber();
            return number == null ? Objects.hash(lexical, datatype, lang) : number.hashCode();
        }

        @Override public String toString() { return nt(); }
    }
}
