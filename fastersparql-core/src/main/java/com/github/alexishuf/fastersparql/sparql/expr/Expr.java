package com.github.alexishuf.fastersparql.sparql.expr;

import com.github.alexishuf.fastersparql.client.model.Vars;
import com.github.alexishuf.fastersparql.client.util.UriUtils;
import com.github.alexishuf.fastersparql.sparql.binding.ArrayBinding;
import com.github.alexishuf.fastersparql.sparql.binding.Binding;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Objects;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import static com.github.alexishuf.fastersparql.sparql.RDFTypes.langString;
import static com.github.alexishuf.fastersparql.sparql.RDFTypes.string;
import static com.github.alexishuf.fastersparql.sparql.expr.Term.Lit.*;

public sealed interface Expr permits Term, Expr.Function {
    int argCount();
    Expr arg(int i);

    /** Evaluate this expression assigning the given values to variables.
     *  If some variable is not bound at evaluation time, an {@link UnboundVarException}
     *  will be thrown. Unbound vars do not raise an error on the discarded branches of
     *  {@code IF(cond, true, false)} and {@code COALESCE(...)} functions. */
    Term eval(Binding binding);

    /** Get an {@link Expr} with all vars in {@code binding} replaced with their
     *  mapped non-var {@link Term}s. If no var in {@code binding} appears in this {@link Expr},
     *  returns {@code this}*/
    Expr bind(Binding binding);

    default <T extends Expr> T evalAs(Binding binding, Class<T> cls) {
        Term value = eval(binding);
        if (!cls.isInstance(value))
            throw new InvalidExprTypeException(this, value, cls.getSimpleName());
        //noinspection unchecked
        return (T)value;
    }

    /** Add all vars used in {@code e} to {@code out} and return the number of vars
     *  effectively added (i.e., not already present in {@code out}). */
    static int addVars(Vars out, Expr e) {
        if (e instanceof Var v) return out.add(v.name()) ? 1 : 0;
        int added = 0;
        for (int i = 0, n = e.argCount(); i < n; i++)
            added += addVars(out, e.arg(i));
        return added;
    }

    sealed abstract class Function implements Expr permits BinaryFunction, NAryFunction, Supplier, UnaryFunction {
        private int hash;
        public String sparqlName() { return getClass().getSimpleName().toLowerCase(); }

        @Override public boolean equals(Object obj) {
            if (obj == this) return true;
            if (!(obj instanceof Function f) || !f.sparqlName().equals(sparqlName())
                                             || f.argCount() != argCount()) {
                return false;
            }
            for (int i = 0, n = argCount(); i < n; i++) {
                if (!f.arg(i).equals(arg(i))) return false;
            }
            return true;
        }

        @Override public int hashCode() {
            if (hash == 0) {
                int h = sparqlName().hashCode();
                for (int i = 0, n = argCount(); i < n; i++)
                    h = 31*h + Objects.hashCode(arg(i).hashCode());
                hash = h;
            }
            return hash;
        }
    }

    non-sealed abstract class Supplier extends Function {
        @Override public int argCount() { return 0; }
        @Override public Expr arg(int i) { throw new IndexOutOfBoundsException(i); }
        @Override public Expr bind(Binding binding) { return this; }
    }
    non-sealed abstract class UnaryFunction extends Function {
        protected final Expr in;
        public UnaryFunction(Expr in) { this.in = in; }
        @Override public int argCount() { return 1; }
        @Override public Expr arg(int i) { return in; }
    }

    abstract class UnaryOperator extends UnaryFunction {
        public UnaryOperator(Expr in) { super(in); }
        @Override public String toString() { return sparqlName()+arg(0); }
    }

    non-sealed abstract class BinaryFunction extends Function {
        protected final Expr l;
        protected final Expr r;
        public BinaryFunction(Expr l, Expr r) { this.l = l; this.r = r; }
        @Override public int argCount() { return 2; }
        @Override public Expr arg(int i) {
            return switch (i) {
                case 0 -> l; case 1 -> r; default -> throw new IndexOutOfBoundsException(i);
            };
        }

        @Override public String toString() {
            return sparqlName() + '(' + l + ", " + r + ')';
        }
    }

    abstract class BinaryOperator extends BinaryFunction {
        public BinaryOperator(Expr l, Expr r) { super(l, r); }
        @Override public String toString() { return l + " " + sparqlName() + " " + r; }
    }

    abstract non-sealed class NAryFunction extends Function {
        protected final Expr[] args;

        public NAryFunction(Expr[] args) { this.args = args; }
        @Override public int argCount() { return args.length; }
        @Override public Expr arg(int i) { return args[i]; }

        protected Expr[] boundArgs(Binding binding) {
            Expr[] bound = new Expr[args.length];
            boolean change = false;
            for (int i = 0; i < args.length; i++)
                change |= (bound[i] = args[i].bind(binding)) != args[i];
            return change ? bound : args;
        }

        @Override public String toString() {
            var sb = new StringBuilder();
            sb.append(sparqlName()).append('(');
            for (int i = 0, n = argCount(); i < n; i++)
                sb.append(arg(i)).append(", ");
            if (sb.charAt(sb.length()-1) == ' ')
                sb.setLength(sb.length()-2);
            return sb.append(')').toString();
        }
    }

    class Or extends BinaryOperator {
        public Or(Expr l, Expr r) { super(l, r); }
        @Override public String sparqlName() { return "||"; }
        @Override public Term eval(Binding b) {
            return l.eval(b).asBool() || r.eval(b).asBool() ? TRUE : FALSE;
        }
        @Override public Expr bind(Binding binding) {
            Expr bl = l.bind(binding), br = r.bind(binding);
            return bl == l && br == r ? this : new Or(bl, br);
        }
    }

    class And extends BinaryOperator {
        public And(Expr l, Expr r) { super(l, r); }
        @Override public String sparqlName() { return "&&"; }
        @Override public Term eval(Binding b) {
            return l.eval(b).asBool() && r.eval(b).asBool() ? TRUE : FALSE;
        }
        @Override public Expr bind(Binding binding) {
            Expr bl = l.bind(binding), br = r.bind(binding);
            return bl == l && br == r ? this : new And(bl, br);
        }
    }

    class Eq extends BinaryOperator {
        public Eq(Expr l, Expr r) { super(l, r); }
        @Override public String sparqlName() { return "="; }
        @Override public Term eval(Binding b) { return l.eval(b).equals(r.eval(b)) ? TRUE : FALSE; }
        @Override public Expr bind(Binding binding) {
            Expr bl = l.bind(binding), br = r.bind(binding);
            return bl == l && br == r ? this : new Eq(bl, br);
        }
    }

    class Neq extends BinaryOperator {
        public Neq(Expr l, Expr r) { super(l, r); }
        @Override public String sparqlName() { return "!="; }
        @Override public Term eval(Binding b) { return l.eval(b).equals(r.eval(b)) ? FALSE : TRUE; }
        @Override public Expr bind(Binding binding) {
            Expr bl = l.bind(binding), br = r.bind(binding);
            return bl == l && br == r ? this : new Neq(bl, br);
        }
    }

    class Lt extends BinaryOperator {
        public Lt(Expr l, Expr r) { super(l, r); }
        @Override public String sparqlName() { return "<"; }
        @Override public Term eval(Binding b) {
            return l.evalAs(b, Lit.class).compareTo(r.evalAs(b, Lit.class)) < 0 ? TRUE : FALSE;
        }
        @Override public Expr bind(Binding binding) {
            Expr bl = l.bind(binding), br = r.bind(binding);
            return bl == l && br == r ? this : new Lt(bl, br);
        }
    }

    class Gt extends BinaryOperator {
        public Gt(Expr l, Expr r) { super(l, r); }
        @Override public String sparqlName() { return ">"; }
        @Override public Term eval(Binding b) {
            return l.evalAs(b, Lit.class).compareTo(r.evalAs(b, Lit.class)) > 0 ? TRUE : FALSE;
        }
        @Override public Expr bind(Binding binding) {
            Expr bl = l.bind(binding), br = r.bind(binding);
            return bl == l && br == r ? this : new Gt(bl, br);
        }
    }

    class Lte extends BinaryOperator {
        public Lte(Expr l, Expr r) { super(l, r); }
        @Override public String sparqlName() { return "<="; }
        @Override public Term eval(Binding b) {
            return l.evalAs(b, Lit.class).compareTo(r.evalAs(b, Lit.class)) <= 0 ? TRUE : FALSE;
        }
        @Override public Expr bind(Binding binding) {
            Expr bl = l.bind(binding), br = r.bind(binding);
            return bl == l && br == r ? this : new Lte(bl, br);
        }
    }

    class Gte extends BinaryOperator {
        public Gte(Expr l, Expr r) { super(l, r); }
        @Override public String sparqlName() { return ">="; }
        @Override public Term eval(Binding b) {
            return l.evalAs(b, Lit.class).compareTo(r.evalAs(b, Lit.class)) >= 0 ? TRUE : FALSE;
        }
        @Override public Expr bind(Binding binding) {
            Expr bl = l.bind(binding), br = r.bind(binding);
            return bl == l && br == r ? this : new Gte(bl, br);
        }
    }

    class Neg extends UnaryOperator {
        public Neg(Expr in) { super(in); }
        @Override public String sparqlName() { return "!"; }
        @Override public Term eval(Binding b) { return in.evalAs(b, Lit.class).negate(); }
        @Override public Expr bind(Binding binding) {
            Expr b = in.bind(binding);
            return b == in ? this : new Neg(b);
        }
    }

    class Minus extends UnaryOperator {
        public Minus(Expr in) { super(in); }
        @Override public String sparqlName() { return "-"; }
        @Override public Term eval(Binding b) { return in.evalAs(b, Lit.class).negate(); }
        @Override public Expr bind(Binding binding) {
            Expr b = in.bind(binding);
            return b == in ? this : new Minus(b);
        }
    }

    class Add extends BinaryOperator {
        public Add(Expr l, Expr r) { super(l, r); }
        @Override public String sparqlName() { return "+"; }
        @Override public Term eval(Binding b) {
            return l.evalAs(b, Lit.class).add(r.evalAs(b, Lit.class));
        }
        @Override public Expr bind(Binding binding) {
            Expr bl = l.bind(binding), br = r.bind(binding);
            return bl == l && br == r ? this : new Add(bl, br);
        }
    }

    class Subtract extends BinaryOperator {
        public Subtract(Expr l, Expr r) { super(l, r); }
        @Override public String sparqlName() { return "-"; }
        @Override public Term eval(Binding b) {
            return l.evalAs(b, Lit.class).subtract(r.evalAs(b, Lit.class));
        }
        @Override public Expr bind(Binding binding) {
            Expr bl = l.bind(binding), br = r.bind(binding);
            return bl == l && br == r ? this : new Subtract(bl, br);
        }
    }

    class Multiply extends BinaryOperator {
        public Multiply(Expr l, Expr r) { super(l, r); }
        @Override public String sparqlName() { return "*"; }
        @Override public Term eval(Binding b) {
            return l.evalAs(b, Lit.class).multiply(r.evalAs(b, Lit.class));
        }
        @Override public Expr bind(Binding binding) {
            Expr bl = l.bind(binding), br = r.bind(binding);
            return bl == l && br == r ? this : new Multiply(bl, br);
        }
    }

    class Divide extends BinaryOperator {
        public Divide(Expr l, Expr r) { super(l, r); }
        @Override public String sparqlName() { return "/"; }
        @Override public Term eval(Binding b) {
            return l.evalAs(b, Lit.class).divide(r.evalAs(b, Lit.class));
        }
        @Override public Expr bind(Binding binding) {
            Expr bl = l.bind(binding), br = r.bind(binding);
            return bl == l && br == r ? this : new Divide(bl, br);
        }
    }

    class In extends NAryFunction {
        public In(Expr[] args) { super(args); }
        @Override public Term eval(Binding b) {
            Term key = args[0].eval(b);
            for (int i = 1; i < args.length; i++)
                if (key.equals(args[1].eval(b))) return TRUE;
            return FALSE;
        }
        @Override public Expr bind(Binding binding) {
            Expr[] b = boundArgs(binding);
            return b == args ? this : new In(b);
        }
    }

    class NotIn extends NAryFunction {
        public NotIn(Expr[] args) { super(args); }
        @Override public String sparqlName() { return "not in"; }
        @Override public Term eval(Binding b) {
            Term key = args[0].eval(b);
            for (int i = 1; i < args.length; i++)
                if (key.equals(args[1].eval(b))) return FALSE;
            return TRUE;
        }
        @Override public Expr bind(Binding binding) {
            Expr[] b = boundArgs(binding);
            return b == args ? this : new NotIn(b);
        }
    }

    class Str extends UnaryFunction {
        public Str(Expr in) { super(in); }
        @Override public Term eval(Binding b) { return in.eval(b).str(); }
        @Override public Expr bind(Binding binding) {
            Expr b = in.bind(binding);
            return b == in ? this : new Str(b);
        }
    }

    class Lang extends UnaryFunction {
        public Lang(Expr in) { super(in); }
        @Override public Term eval(Binding binding) {
            String lang = in.evalAs(binding, Lit.class).lang();
            return lang == null ? EMPTY : Lit.valueOf(lang);
        }
        @Override public Expr bind(Binding binding) {
            Expr b = in.bind(binding);
            return b == in ? this : new Lang(b);
        }
    }

    class Datatype extends UnaryFunction {
        public Datatype(Expr in) { super(in); }
        @Override public Term eval(Binding b) {
            return Lit.valueOf(in.evalAs(b, Lit.class).datatype());
        }
        @Override public Expr bind(Binding binding) {
            Expr b = in.bind(binding);
            return b == in ? this : new Datatype(b);
        }
    }

    class MakeBNode extends NAryFunction {
        public MakeBNode(Expr... args) {
            super(args);
            if (args.length > 1)
                throw new IllegalArgumentException("BNode can take at most one argument");
        }
        @Override public Term eval(Binding binding) {
            return new Term.BNode(args[0].evalAs(binding, Lit.class).lexical());
        }
        @Override public Expr bind(Binding binding) {
            Expr[] b = boundArgs(binding);
            return b == args ? this : new MakeBNode(b);
        }
    }

    class Bound extends UnaryFunction {
        public Bound(Expr in) { super(in); }
        @Override public Term eval(Binding b) {
            return in instanceof Var v && b.get(v.name()) == null ? FALSE : TRUE;
        }
        @Override public Expr bind(Binding binding) {
            Expr b = in.bind(binding);
            return b == in ? this : new Bound(b);
        }
    }

    class Iri extends UnaryFunction {
        public Iri(Expr in) { super(in); }
        @Override public Term eval(Binding binding) {
            Term value = in.eval(binding);
            if (value instanceof IRI) return value;
            else if (value instanceof Lit l) return new IRI("<"+l.lexical()+">");
            throw new InvalidExprTypeException(in, value, "IRI or string literal");
        }
        @Override public Expr bind(Binding binding) {
            Expr b = in.bind(binding);
            return b == in ? this : new Iri(b);
        }
    }

    class Rand extends Supplier {
        @Override public Term eval(Binding ignored) { return Lit.valueOf(Math.random()); }
    }

    class Abs extends UnaryFunction {
        public Abs(Expr in) { super(in); }
        @Override public Term eval(Binding b) { return in.evalAs(b, Lit.class).abs(); }
        @Override public Expr bind(Binding binding) {
            Expr b = in.bind(binding);
            return b == in ? this : new Abs(b);
        }
    }
    class Floor extends UnaryFunction {
        public Floor(Expr in) { super(in); }
        @Override public Term eval(Binding b) { return in.evalAs(b, Lit.class).floor(); }
        @Override public Expr bind(Binding binding) {
            Expr b = in.bind(binding);
            return b == in ? this : new Floor(b);
        }
    }
    class Ceil extends UnaryFunction {
        public Ceil(Expr in) { super(in); }
        @Override public Term eval(Binding b) { return in.evalAs(b, Lit.class).ceil(); }
        @Override public Expr bind(Binding binding) {
            Expr b = in.bind(binding);
            return b == in ? this : new Ceil(b);
        }
    }
    class Round extends UnaryFunction {
        public Round(Expr in) { super(in); }
        @Override public Term eval(Binding b) { return in.evalAs(b, Lit.class).round(); }
        @Override public Expr bind(Binding binding) {
            Expr b = in.bind(binding);
            return b == in ? this : new Round(b);
        }
    }

    class Concat extends NAryFunction {
        public Concat(Expr[] args) { super(args); }
        @Override public Term eval(Binding binding) {
            var sb = new StringBuilder();
            Lit first = args[0].evalAs(binding, Lit.class);
            sb.append(first.lexical());
            for (int i = 1; i < args.length; i++)
                sb.append(args[i].evalAs(binding, Lit.class).lexical());
            String lang = first.lang();
            return new Lit(sb.toString(), lang == null ? string : langString, lang);
        }
        @Override public Expr bind(Binding binding) {
            Expr[] b = boundArgs(binding);
            return b == args ? this : new Concat(b);
        }
    }

    class Strlen extends UnaryFunction {
        public Strlen(Expr in) { super(in); }
        @Override public Term eval(Binding binding) {
            return valueOf(in.evalAs(binding, Lit.class).lexical().length());
        }
        @Override public Expr bind(Binding binding) {
            Expr b = in.bind(binding);
            return b == in ? this : new Strlen(b);
        }
    }

    class UCase extends UnaryFunction {
        public UCase(Expr in) { super(in); }
        @Override public Term eval(Binding binding) {
            Lit lit = in.evalAs(binding, Lit.class);
            return new Lit(lit.lexical().toUpperCase(), lit.datatype(), lit.lang());
        }
        @Override public Expr bind(Binding binding) {
            Expr b = in.bind(binding);
            return b == in ? this : new UCase(b);
        }
    }

    class LCase extends UnaryFunction {
        public LCase(Expr in) { super(in); }
        @Override public Term eval(Binding binding) {
            Lit lit = in.evalAs(binding, Lit.class);
            return new Lit(lit.lexical().toLowerCase(), lit.datatype(), lit.lang());
        }
        @Override public Expr bind(Binding binding) {
            Expr b = in.bind(binding);
            return b == in ? this : new LCase(b);
        }
    }

    class Encode_for_uri extends UnaryFunction {
        public Encode_for_uri(Expr in) { super(in); }

        @Override public Term eval(Binding binding) {
            Lit lit = in.evalAs(binding, Lit.class);
            String lex = lit.lexical();
            String escaped = UriUtils.escapeQueryParam(lex);
            //noinspection StringEquality
            return escaped == lex ? lit : new Lit(escaped, lit.datatype(), lit.lang());
        }
        @Override public Expr bind(Binding binding) {
            Expr b = in.bind(binding);
            return b == in ? this : new Encode_for_uri(b);
        }
    }

    class Contains extends BinaryFunction {
        public Contains(Expr l, Expr r) { super(l, r); }
        @Override public Term eval(Binding b) {
            return l.evalAs(b, Lit.class).lexical().contains(r.evalAs(b, Lit.class).lexical()) ? TRUE : FALSE;
        }
        @Override public Expr bind(Binding binding) {
            Expr bl = l.bind(binding), br = r.bind(binding);
            return bl == l && br == r ? this : new Contains(bl, br);
        }
    }

    class Strstarts extends BinaryFunction {
        public Strstarts(Expr l, Expr r) { super(l, r); }
        @Override public Term eval(Binding b) {
            return l.evalAs(b, Lit.class).lexical().startsWith(r.evalAs(b, Lit.class).lexical()) ? TRUE : FALSE;
        }
        @Override public Expr bind(Binding binding) {
            Expr bl = l.bind(binding), br = r.bind(binding);
            return bl == l && br == r ? this : new Strstarts(bl, br);
        }
    }

    class Strends extends BinaryFunction {
        public Strends(Expr l, Expr r) { super(l, r); }
        @Override public Term eval(Binding b) {
            return l.evalAs(b, Lit.class).lexical().endsWith(r.evalAs(b, Lit.class).lexical()) ? TRUE : FALSE;
        }
        @Override public Expr bind(Binding binding) {
            Expr bl = l.bind(binding), br = r.bind(binding);
            return bl == l && br == r ? this : new Strends(bl, br);
        }
    }

    class Strbefore extends BinaryFunction {
        public Strbefore(Expr l, Expr r) { super(l, r); }
        @Override public Term eval(Binding b) {
            Lit outer = l.evalAs(b, Lit.class);
            int i = outer.lexical().indexOf(r.evalAs(b, Lit.class).lexical());
            return new Lit(outer.lexical().substring(0, i), outer.datatype(), outer.lang());
        }
        @Override public Expr bind(Binding binding) {
            Expr bl = l.bind(binding), br = r.bind(binding);
            return bl == l && br == r ? this : new Strbefore(bl, br);
        }
    }

    class Strafter extends BinaryFunction {
        public Strafter(Expr l, Expr r) { super(l, r); }
        @Override public Term eval(Binding b) {
            Lit outer = l.evalAs(b, Lit.class);
            String inner = r.evalAs(b, Lit.class).lexical();
            int i = outer.lexical().indexOf(inner);
            String substring = outer.lexical().substring(i + inner.length());
            return new Lit(substring, outer.datatype(), outer.lang());
        }
        @Override public Expr bind(Binding binding) {
            Expr bl = l.bind(binding), br = r.bind(binding);
            return bl == l && br == r ? this : new Strafter(bl, br);
        }
    }

    class Regex extends NAryFunction {
        static Pattern compile(Binding binding, Expr regex, @Nullable Expr flags) {
            try {
                String regexStr = regex.evalAs(binding, Lit.class).lexical();
                if (flags != null) {
                    String flagsStr = flags.evalAs(binding, Lit.class).lexical();
                    //noinspection StringBufferReplaceableByString
                    regexStr = new StringBuilder().append('(').append('?').append(flagsStr).append(')').append(regexStr).toString();
                }
                return Pattern.compile(regexStr);
            }  catch (PatternSyntaxException e) {
                throw new InvalidExprException("Bad REGEX: "+e.getMessage());
            }
        }

        private final @Nullable Pattern rx;
        public Regex(Expr... args) {
            super(args);
            if (args.length < 2 || args.length > 3)
                throw new IllegalArgumentException("REGEX takes 2 or 3 arguments, got "+args.length);
            Pattern rx = null;
            try {
                rx = compile(ArrayBinding.EMPTY, args[1], args.length > 2 ? args[2] : null);
            } catch (UnboundVarException ignored) {}
            this.rx = rx;
        }

        @Override public Term eval(Binding binding) {
            String text = args[0].evalAs(binding, Lit.class).lexical();
            var p = rx != null ? rx : compile(binding, args[1], args.length > 2 ? args[2] : null);
            return p.matcher(text).find() ? TRUE : FALSE;
        }
        @Override public Expr bind(Binding binding) {
            Expr[] b = boundArgs(binding);
            return b == args ? this : new Regex(b);
        }
    }

    class Replace extends NAryFunction {
        private final @Nullable Pattern rx;
        public Replace(Expr... args) {
            super(args);
            if (args.length < 3 || args.length > 4)
                throw new IllegalArgumentException("Replace takes 3 or 4 arguments, got "+args.length);
            Pattern p = null;
            try {
                p = Regex.compile(ArrayBinding.EMPTY, args[1], args.length > 3 ? args[3] : null);
            } catch (Throwable ignored ) {}
            this.rx = p;
        }
        @Override public Term eval(Binding b) {
            var p = rx != null ? rx : Regex.compile(b, args[1], args.length > 3 ? args[3] : null);
            Lit text = args[0].evalAs(b, Lit.class);
            String lex = text.lexical();
            String replacement = args[2].evalAs(b, Lit.class).lexical();
            return new Lit(p.matcher(lex).replaceAll(replacement), text.datatype(), text.lang());
        }
        @Override public Expr bind(Binding binding) {
            Expr[] b = boundArgs(binding);
            return b == args ? this : new Replace(b);
        }
    }

    class Substr extends NAryFunction {
        public Substr(Expr... args) {
            super(args);
            if (args.length < 2 || args.length > 3)
                throw new IllegalArgumentException("substr takes 2 or 3 arguments, got "+args.length);
        }

        @Override public Term eval(Binding b) {
            Lit text = args[0].evalAs(b, Lit.class);
            String lexical = text.lexical();
            int start = args[1].evalAs(b, Lit.class).asInt();
            int end = lexical.length();
            if (args.length > 2)
                end = start + args[2].evalAs(b, Lit.class).asInt();
            return new Lit(lexical.substring(start, end), text.datatype(), text.lang());
        }
        @Override public Expr bind(Binding binding) {
            Expr[] b = boundArgs(binding);
            return b == args ? this : new Substr(b);
        }
    }

    class Uuid extends Supplier {
        @Override public Term eval(Binding binding) {
            return new Lit("<urn:uuid:"+ UUID.randomUUID()+">", string, null);
        }
    }

    class Struuid extends Supplier {
        @Override public Term eval(Binding binding) {
            return new Lit(UUID.randomUUID().toString(), string, null);
        }
    }

    class Coalesce extends NAryFunction {
        public Coalesce(Expr[] args) {
            super(args);
            if (args.length == 0) throw new IllegalArgumentException("coalesce requires at least one argument");
        }
        @Override public Term eval(Binding binding) {
            RuntimeException first = null;
            for (Expr a : args) {
                try {
                    return a.eval(binding);
                } catch (RuntimeException e) { first = first == null ? e : first;
                } catch (Throwable ignored) {}
            }
            if (first != null)
                throw first;
            throw new IllegalArgumentException("All arguments evaluated with unexpected exceptions");
        }
        @Override public Expr bind(Binding binding) {
            Expr[] b = boundArgs(binding);
            return b == args ? this : new Coalesce(b);
        }
    }

    class If extends NAryFunction {
        public If(Expr... args) {
            super(args);
            if (args.length != 3)
                throw new IllegalArgumentException("IF requires 3 arguments, got "+ args.length);
        }
        @Override public Term eval(Binding b) {
            if (args[0].evalAs(b, Lit.class).asBool())
                return args[1].eval(b);
            else
                return args[2].eval(b);
        }
        @Override public Expr bind(Binding binding) {
            Expr[] b = boundArgs(binding);
            return b == args ? this : new If(b);
        }
    }

    class Strlang extends BinaryFunction {
        public Strlang(Expr l, Expr r) { super(l, r); }
        @Override public Term eval(Binding b) {
            String lex = l.evalAs(b, Lit.class).lexical();
            String lang = r.evalAs(b, Lit.class).lexical();
            return new Lit(lex, langString, lang);
        }
        @Override public Expr bind(Binding binding) {
            Expr bl = l.bind(binding), br = r.bind(binding);
            return bl == l && br == r ? this : new Strlang(bl, br);
        }
    }

    class Strdt extends BinaryFunction {
        public Strdt(Expr l, Expr r) { super(l, r); }
        @Override public Term eval(Binding b) {
            String lexical = l.evalAs(b, Lit.class).lexical();
            String dt = l.evalAs(b, IRI.class).str().lexical();
            return new Lit(lexical, dt, null);
        }
        @Override public Expr bind(Binding binding) {
            Expr bl = l.bind(binding), br = r.bind(binding);
            return bl == l && br == r ? this : new Strdt(bl, br);
        }
    }

    class SameTerm extends BinaryFunction {
        public SameTerm(Expr l, Expr r) { super(l, r); }
        @Override public Term eval(Binding b) {
            return l.eval(b).equals(r.eval(b)) ? TRUE : FALSE;
        }
        @Override public Expr bind(Binding binding) {
            Expr bl = l.bind(binding), br = r.bind(binding);
            return bl == l && br == r ? this : new SameTerm(bl, br);
        }
    }

    class IsIRI extends UnaryFunction {
        public IsIRI(Expr in) { super(in); }
        @Override public Term eval(Binding b) {
            return in.eval(b) instanceof IRI ? TRUE : FALSE;
        }
        @Override public Expr bind(Binding binding) {
            Expr b = in.bind(binding);
            return b == in ? this : new IsIRI(b);
        }
    }

    class IsBlank extends UnaryFunction {
        public IsBlank(Expr in) { super(in); }
        @Override public Term eval(Binding b) {
            return in.eval(b) instanceof BNode ? TRUE : FALSE;
        }
        @Override public Expr bind(Binding binding) {
            Expr b = in.bind(binding);
            return b == in ? this : new IsBlank(b);
        }
    }

    class IsLit extends UnaryFunction {
        public IsLit(Expr in) { super(in); }
        @Override public Term eval(Binding b) {
            return in.eval(b) instanceof Lit ? TRUE : FALSE;
        }
        @Override public Expr bind(Binding binding) {
            Expr b = in.bind(binding);
            return b == in ? this : new IsLit(b);
        }
    }

    class IsNumeric extends UnaryFunction {
        public IsNumeric(Expr in) { super(in); }
        @Override public Term eval(Binding b) {
            return in.eval(b) instanceof Lit l && l.asNumber() != null ? TRUE : FALSE;
        }
        @Override public Expr bind(Binding binding) {
            Expr b = in.bind(binding);
            return b == in ? this : new IsNumeric(b);
        }
    }
}
