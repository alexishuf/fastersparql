package com.github.alexishuf.fastersparql.sparql.expr;

import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.ByteRope;
import com.github.alexishuf.fastersparql.model.rope.Rope;
import com.github.alexishuf.fastersparql.model.rope.RopeDict;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import com.github.alexishuf.fastersparql.sparql.PrefixAssigner;
import com.github.alexishuf.fastersparql.sparql.binding.ArrayBinding;
import com.github.alexishuf.fastersparql.sparql.binding.Binding;
import com.github.alexishuf.fastersparql.util.UriUtils;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Objects;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import static com.github.alexishuf.fastersparql.batch.type.Batch.COMPRESSED;
import static com.github.alexishuf.fastersparql.model.rope.Rope.of;
import static com.github.alexishuf.fastersparql.model.rope.RopeDict.DT_integer;
import static com.github.alexishuf.fastersparql.sparql.expr.Term.*;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

@SuppressWarnings("SpellCheckingInspection")
public sealed interface Expr permits Term, Expr.Exists, Expr.Function {
    int argCount();
    Expr arg(int i);

    /** Get the set of vars mentioned anywhere in this expression */
    Vars vars();

    /** Evaluate this expression assigning the given values to variables.
     *  If some variable is not bound at evaluation time, an {@link UnboundVarException}
     *  will be thrown. Unbound vars do not raise an error on the discarded branches of
     *  {@code IF(cond, true, false)} and {@code COALESCE(...)} functions. */
    Term eval(Binding binding);

    /** Get an {@link Expr} with all vars in {@code binding} replaced with their
     *  mapped non-var {@link Term}s. If no var in {@code binding} appears in this {@link Expr},
     *  returns {@code this}*/
    Expr bound(Binding binding);

    /** Whether this is an RDF value (a non-variable {@link Term}). */
    default boolean isGround() { return this instanceof Term t && !t.isVar(); }

    /** Write this {@link Expr} in SPARQL syntax to {@code out} */
    void toSparql(ByteRope out, PrefixAssigner prefixAssigner);

    default Rope toSparql() {
        ByteRope r = new ByteRope();
        toSparql(r, PrefixAssigner.NOP);
        return r;
    }

    /** Add all vars used in {@code e} to {@code out} and return the number of vars
     *  effectively added (i.e., not already present in {@code out}). */
    static int addVars(Vars out, Expr e) {
        if (e instanceof Term t)
            return t.type() == Type.VAR && out.add(t) ? 1 : 0;
        int added = 0;
        for (int i = 0, n = e.argCount(); i < n; i++)
            added += addVars(out, e.arg(i));
        return added;
    }

    /**
     * Implements {@code FILTER EXISTS} and {@code FILTER NOT EXISTS} (if {@code negate == true}).
     *
     * <p>This is not a {@link Function} since it has no {@link Expr} instances as arguments and
     * its semantics is not that of a {@link Supplier}.</p>
     */
    record Exists(Plan filter, boolean negate) implements Expr {
        @Override public int argCount() { return 0; }

        @Override public Expr arg(int i) { throw new IndexOutOfBoundsException(i); }

        @Override public Vars vars() {
            return filter.allVars();
        }

        @Override public Term eval(Binding binding) {
            try (var it = filter.bound(binding).execute(COMPRESSED).eager()) {
                return (it.nextBatch(null) != null) ^ negate ? TRUE : FALSE;
            }
        }

        @Override public Expr bound(Binding binding) {
            Plan bound = filter.bound(binding);
            return bound == filter ? this : new Exists(bound, negate);
        }

        private static final byte[] NOT_EXISTS = "NOT EXISTS".getBytes(UTF_8);
        private static final byte[] EXISTS = "EXISTS".getBytes(UTF_8);
        @Override public void toSparql(ByteRope out, PrefixAssigner assigner) {
            int indent;
            if (out.len() == 0) {
                indent = 0;
            } else {
                int lineBegin = out.reverseSkip(0, out.len, Rope.UNTIL_LF);
                if (out.get(lineBegin) == '\n') lineBegin++;
                indent = out.skip(lineBegin, out.len(), Rope.WS) - lineBegin;
            }
            out.append(negate ? NOT_EXISTS : EXISTS);
            filter.groupGraphPattern(out, indent, assigner);
        }

        @Override public String toString() {
            var sb = new ByteRope(256);
            toSparql(sb, PrefixAssigner.NOP);
            return sb.toString();
        }
    }

    private static Term requireLiteral(Expr expr, Binding binding) {
        Term term = expr.eval(binding);
        if (term.type() != Type.LIT)
            throw new InvalidExprTypeException(expr, term, "literal");
        return term;
    }

    private static Rope requireLexical(Expr expr, Binding binding) {
        Term term = expr.eval(binding);
        Rope lex = term.escapedLexical();
        if (lex == null)
            throw new InvalidExprTypeException(expr, term, "literal");
        return lex;
    }

    sealed abstract class Function implements Expr permits BinaryFunction, NAryFunction, Supplier, UnaryFunction {
        private int hash;
        private @MonotonicNonNull Vars vars;

        public String sparqlName() { return getClass().getSimpleName().toLowerCase(); }

        @Override public Vars vars() {
            if (vars != null) return vars;
            Vars.Mutable set = new Vars.Mutable(10);
            Expr.addVars(set, this);
            return vars = set;
        }

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

        @Override public void toSparql(ByteRope out, PrefixAssigner assigner) {
            out.append(sparqlName()).append('(');
            int n = argCount();
            for (int i = 0; i < n; i++) {
                arg(i).toSparql(out, assigner);
                out.append(',').append(' ');
            }
            if (n > 0) out.unAppend(2);
            out.append(')');
        }

        @Override public final String toString() {
            var sb = new ByteRope(33);
            toSparql(sb, PrefixAssigner.NOP);
            return sb.toString();
        }
    }

    non-sealed abstract class Supplier extends Function {
        @Override public int argCount() { return 0; }
        @Override public Expr arg(int i) { throw new IndexOutOfBoundsException(i); }
        @Override public Expr bound(Binding binding) { return this; }
    }
    non-sealed abstract class UnaryFunction extends Function {
        protected final Expr in;
        public UnaryFunction(Expr in) { this.in = in; }
        @Override public int argCount() { return 1; }
        @Override public Expr arg(int i) { return in; }
    }

    abstract class UnaryOperator extends UnaryFunction {
        public UnaryOperator(Expr in) { super(in); }

        @Override public void toSparql(ByteRope out, PrefixAssigner assigner) {
            in.toSparql(out.append(sparqlName()), assigner);
        }
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
    }

    abstract class BinaryOperator extends BinaryFunction {
        public BinaryOperator(Expr l, Expr r) { super(l, r); }

        @Override public void toSparql(ByteRope out, PrefixAssigner assigner) {
            out.append('(');
            l.toSparql(out, assigner);
            out.append(' ').append(sparqlName()).append(' ');
            r.toSparql(out, assigner);
            out.append(')');
        }
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
                change |= (bound[i] = args[i].bound(binding)) != args[i];
            return change ? bound : args;
        }
    }

    class Or extends BinaryOperator {
        public Or(Expr l, Expr r) { super(l, r); }
        @Override public String sparqlName() { return "||"; }
        @Override public Term eval(Binding b) {
            return l.eval(b).asBool() || r.eval(b).asBool() ? TRUE : FALSE;
        }
        @Override public Expr bound(Binding binding) {
            Expr bl = l.bound(binding), br = r.bound(binding);
            return bl == l && br == r ? this : new Or(bl, br);
        }
    }

    class And extends BinaryOperator {
        public And(Expr l, Expr r) { super(l, r); }
        @Override public String sparqlName() { return "&&"; }
        @Override public Term eval(Binding b) {
            return l.eval(b).asBool() && r.eval(b).asBool() ? TRUE : FALSE;
        }
        @Override public Expr bound(Binding binding) {
            Expr bl = l.bound(binding), br = r.bound(binding);
            return bl == l && br == r ? this : new And(bl, br);
        }
    }

    class Eq extends BinaryOperator {
        public Eq(Expr l, Expr r) { super(l, r); }
        @Override public String sparqlName() { return "="; }
        @Override public Term eval(Binding b) { return l.eval(b).equals(r.eval(b)) ? TRUE : FALSE; }
        @Override public Expr bound(Binding binding) {
            Expr bl = l.bound(binding), br = r.bound(binding);
            return bl == l && br == r ? this : new Eq(bl, br);
        }
    }

    class Neq extends BinaryOperator {
        public Neq(Expr l, Expr r) { super(l, r); }
        @Override public String sparqlName() { return "!="; }
        @Override public Term eval(Binding b) { return l.eval(b).equals(r.eval(b)) ? FALSE : TRUE; }
        @Override public Expr bound(Binding binding) {
            Expr bl = l.bound(binding), br = r.bound(binding);
            return bl == l && br == r ? this : new Neq(bl, br);
        }
    }

    class Lt extends BinaryOperator {
        public Lt(Expr l, Expr r) { super(l, r); }
        @Override public String sparqlName() { return "<"; }
        @Override public Term eval(Binding b) {
            return l.eval(b).compareTo(r.eval(b)) < 0 ? TRUE : FALSE;
        }
        @Override public Expr bound(Binding binding) {
            Expr bl = l.bound(binding), br = r.bound(binding);
            return bl == l && br == r ? this : new Lt(bl, br);
        }
    }

    class Gt extends BinaryOperator {
        public Gt(Expr l, Expr r) { super(l, r); }
        @Override public String sparqlName() { return ">"; }
        @Override public Term eval(Binding b) {
            return l.eval(b).compareTo(r.eval(b)) > 0 ? TRUE : FALSE;
        }
        @Override public Expr bound(Binding binding) {
            Expr bl = l.bound(binding), br = r.bound(binding);
            return bl == l && br == r ? this : new Gt(bl, br);
        }
    }

    class Lte extends BinaryOperator {
        public Lte(Expr l, Expr r) { super(l, r); }
        @Override public String sparqlName() { return "<="; }
        @Override public Term eval(Binding b) {
            return l.eval(b).compareTo(r.eval(b)) <= 0 ? TRUE : FALSE;
        }
        @Override public Expr bound(Binding binding) {
            Expr bl = l.bound(binding), br = r.bound(binding);
            return bl == l && br == r ? this : new Lte(bl, br);
        }
    }

    class Gte extends BinaryOperator {
        public Gte(Expr l, Expr r) { super(l, r); }
        @Override public String sparqlName() { return ">="; }
        @Override public Term eval(Binding b) {
            return l.eval(b).compareTo(r.eval(b)) >= 0 ? TRUE : FALSE;
        }
        @Override public Expr bound(Binding binding) {
            Expr bl = l.bound(binding), br = r.bound(binding);
            return bl == l && br == r ? this : new Gte(bl, br);
        }
    }

    class Neg extends UnaryOperator {
        public Neg(Expr in) { super(in); }
        @Override public String sparqlName() { return "!"; }
        @Override public Term eval(Binding b) { return in.eval(b).negate(); }
        @Override public Expr bound(Binding binding) {
            Expr b = in.bound(binding);
            return b == in ? this : new Neg(b);
        }
    }

    class Minus extends UnaryOperator {
        public Minus(Expr in) { super(in); }
        @Override public String sparqlName() { return "-"; }
        @Override public Term eval(Binding b) { return in.eval(b).negate(); }
        @Override public Expr bound(Binding binding) {
            Expr b = in.bound(binding);
            return b == in ? this : new Minus(b);
        }
    }

    class Add extends BinaryOperator {
        public Add(Expr l, Expr r) { super(l, r); }
        @Override public String sparqlName() { return "+"; }
        @Override public Term eval(Binding b) {
            return l.eval(b).add(r.eval(b));
        }
        @Override public Expr bound(Binding binding) {
            Expr bl = l.bound(binding), br = r.bound(binding);
            return bl == l && br == r ? this : new Add(bl, br);
        }
    }

    class Subtract extends BinaryOperator {
        public Subtract(Expr l, Expr r) { super(l, r); }
        @Override public String sparqlName() { return "-"; }
        @Override public Term eval(Binding b) {
            return l.eval(b).subtract(r.eval(b));
        }
        @Override public Expr bound(Binding binding) {
            Expr bl = l.bound(binding), br = r.bound(binding);
            return bl == l && br == r ? this : new Subtract(bl, br);
        }
    }

    class Multiply extends BinaryOperator {
        public Multiply(Expr l, Expr r) { super(l, r); }
        @Override public String sparqlName() { return "*"; }
        @Override public Term eval(Binding b) {
            return l.eval(b).multiply(r.eval(b));
        }
        @Override public Expr bound(Binding binding) {
            Expr bl = l.bound(binding), br = r.bound(binding);
            return bl == l && br == r ? this : new Multiply(bl, br);
        }
    }

    class Divide extends BinaryOperator {
        public Divide(Expr l, Expr r) { super(l, r); }
        @Override public String sparqlName() { return "/"; }
        @Override public Term eval(Binding b) {
            return l.eval(b).divide(r.eval(b));
        }
        @Override public Expr bound(Binding binding) {
            Expr bl = l.bound(binding), br = r.bound(binding);
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
        @Override public Expr bound(Binding binding) {
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
        @Override public Expr bound(Binding binding) {
            Expr[] b = boundArgs(binding);
            return b == args ? this : new NotIn(b);
        }
    }

    class Str extends UnaryFunction {
        public Str(Expr in) { super(in); }
        @Override public Term eval(Binding b) {
            Term term = in.eval(b);
            return switch (term.type()) {
                case LIT -> {
                    if (term.datatypeId() == RopeDict.DT_string)
                        yield term;
                    Rope lexical = term.escapedLexical();
                    assert lexical != null;
                    yield Term.plainString(lexical);
                }
                case IRI -> Term.plainString(term.sub(1, term.len()-1));
                default -> throw new InvalidExprTypeException(in, term, "literal/IRI");
            };
        }
        @Override public Expr bound(Binding binding) {
            Expr b = in.bound(binding);
            return b == in ? this : new Str(b);
        }
    }

    class Lang extends UnaryFunction {
        public Lang(Expr in) { super(in); }
        @Override public Term eval(Binding binding) {
            Term term = Expr.requireLiteral(in, binding);
            Rope lang = term.lang();
            return lang == null ? EMPTY_STRING : Term.plainString(lang);
        }
        @Override public Expr bound(Binding binding) {
            Expr b = in.bound(binding);
            return b == in ? this : new Lang(b);
        }
    }

    class Datatype extends UnaryFunction {
        public Datatype(Expr in) { super(in); }
        @Override public Term eval(Binding b) {
            return Expr.requireLiteral(in, b).datatypeTerm();
        }
        @Override public Expr bound(Binding binding) {
            Expr b = in.bound(binding);
            return b == in ? this : new Datatype(b);
        }
    }

    class MakeBNode extends NAryFunction {
        public MakeBNode(Expr... args) {
            super(args);
            if (args.length > 1)
                throw new IllegalArgumentException("BNode can take at most one argument");
        }
        private static final byte[] BN_PREFIX = "_:".getBytes(UTF_8);
        @Override public Term eval(Binding binding) {
            Term term = args[0].eval(binding);
            Rope lex = term.escapedLexical();
            if (lex == null || !lex.has(0, BN_PREFIX))
                throw new InvalidExprTypeException(args[0], term, "literal with _:-prefixed lexical form");
            return valueOf(lex, 0, lex.len());
        }
        @Override public Expr bound(Binding binding) {
            Expr[] b = boundArgs(binding);
            return b == args ? this : new MakeBNode(b);
        }
    }

    class Bound extends UnaryFunction {
        public Bound(Expr in) { super(in); }
        @Override public Term eval(Binding b) {
            Term term = in instanceof Term t ? t : in.eval(b);
            return term.type() != Type.VAR || b.get(term) != null ? TRUE : FALSE;
        }
        @Override public Expr bound(Binding binding) {
            Expr b = in.bound(binding);
            return b == in ? this : new Bound(b);
        }
    }

    class Iri extends UnaryFunction {
        public Iri(Expr in) { super(in); }
        @Override public Term eval(Binding binding) {
            Term value = in.eval(binding);
            return switch (value.type()) {
                case IRI -> value;
                case LIT -> Term.iri(value.escapedLexical());
                default -> throw new InvalidExprTypeException(in, value, "IRI or string literal");
            };
        }
        @Override public Expr bound(Binding binding) {
            Expr b = in.bound(binding);
            return b == in ? this : new Iri(b);
        }
    }

    class Rand extends Supplier {
        @Override public Term eval(Binding ignored) {
            byte[] u8 = ("\"" + Math.random()).getBytes(UTF_8);
            return typed(u8, 0, u8.length, RopeDict.DT_DOUBLE);
        }
    }

    class Abs extends UnaryFunction {
        public Abs(Expr in) { super(in); }
        @Override public Term eval(Binding b) { return in.eval(b).abs(); }
        @Override public Expr bound(Binding binding) {
            Expr b = in.bound(binding);
            return b == in ? this : new Abs(b);
        }
    }
    class Floor extends UnaryFunction {
        public Floor(Expr in) { super(in); }
        @Override public Term eval(Binding b) { return in.eval(b).floor(); }
        @Override public Expr bound(Binding binding) {
            Expr b = in.bound(binding);
            return b == in ? this : new Floor(b);
        }
    }
    class Ceil extends UnaryFunction {
        public Ceil(Expr in) { super(in); }
        @Override public Term eval(Binding b) { return in.eval(b).ceil(); }
        @Override public Expr bound(Binding binding) {
            Expr b = in.bound(binding);
            return b == in ? this : new Ceil(b);
        }
    }
    class Round extends UnaryFunction {
        public Round(Expr in) { super(in); }
        @Override public Term eval(Binding b) { return in.eval(b).round(); }
        @Override public Expr bound(Binding binding) {
            Expr b = in.bound(binding);
            return b == in ? this : new Round(b);
        }
    }

    class Concat extends NAryFunction {
        public Concat(Expr[] args) { super(args); }
        @Override public Term eval(Binding binding) {
            ByteRope result = new ByteRope().append('"');
            Term first = null;
            for (Expr arg : args) {
                Term term = Expr.requireLiteral(arg, binding);
                if (first == null)
                    first = term;
                result.append(requireNonNull(term.escapedLexical()));
            }
            if (first == null)
                return EMPTY_STRING;
            int dt = first.datatypeId();
            if (dt == RopeDict.DT_langString) {
                result.append('"').append('@').append(requireNonNull(first.lang()));
                return Term.wrap(result);
            }
            return Term.typed(result, dt);
        }
        @Override public Expr bound(Binding binding) {
            Expr[] b = boundArgs(binding);
            return b == args ? this : new Concat(b);
        }
    }

    class Strlen extends UnaryFunction {
        public Strlen(Expr in) { super(in); }
        @Override public Term eval(Binding binding) {
            return Term.typed(Expr.requireLexical(in, binding), DT_integer);
        }
        @Override public Expr bound(Binding binding) {
            Expr b = in.bound(binding);
            return b == in ? this : new Strlen(b);
        }
    }

    class UCase extends UnaryFunction {
        public UCase(Expr in) { super(in); }
        @Override public Term eval(Binding binding) {
            Term lit = Expr.requireLiteral(in, binding);
            Rope lex = lit.escapedLexical();
            assert lex != null;
            return lit.withLexical(new ByteRope(lex.toString().toUpperCase()));
        }
        @Override public Expr bound(Binding binding) {
            Expr b = in.bound(binding);
            return b == in ? this : new UCase(b);
        }
    }

    class LCase extends UnaryFunction {
        public LCase(Expr in) { super(in); }
        @Override public Term eval(Binding binding) {
            Term lit = Expr.requireLiteral(in, binding);
            Rope lex = lit.escapedLexical();
            assert lex != null;
            return lit.withLexical(new ByteRope(lex.toString().toLowerCase()));
        }
        @Override public Expr bound(Binding binding) {
            Expr b = in.bound(binding);
            return b == in ? this : new LCase(b);
        }
    }

    class Encode_for_uri extends UnaryFunction {
        public Encode_for_uri(Expr in) { super(in); }

        @Override public Term eval(Binding binding) {
            Term lit = Expr.requireLiteral(in, binding);
            Rope lex = lit.escapedLexical();
            Rope escaped = UriUtils.escapeQueryParam(lex);
            return escaped == lex ? lit : lit.withLexical(escaped);
        }
        @Override public Expr bound(Binding binding) {
            Expr b = in.bound(binding);
            return b == in ? this : new Encode_for_uri(b);
        }
    }

    class Contains extends BinaryFunction {
        public Contains(Expr l, Expr r) { super(l, r); }
        @Override public Term eval(Binding b) {
            Rope lLex = requireLexical(l, b);
            Rope rLex = requireLexical(r, b);
            return lLex.skipUntil(0, lLex.len(), rLex) < lLex.len() ? TRUE : FALSE;
        }
        @Override public Expr bound(Binding binding) {
            Expr bl = l.bound(binding), br = r.bound(binding);
            return bl == l && br == r ? this : new Contains(bl, br);
        }
    }

    class Strstarts extends BinaryFunction {
        public Strstarts(Expr l, Expr r) { super(l, r); }
        @Override public Term eval(Binding b) {
            Rope lLex = requireLexical(l, b);
            Rope rLex = requireLexical(r, b);
            return lLex.has(0, rLex) ? TRUE : FALSE;
        }
        @Override public Expr bound(Binding binding) {
            Expr bl = l.bound(binding), br = r.bound(binding);
            return bl == l && br == r ? this : new Strstarts(bl, br);
        }
    }

    class Strends extends BinaryFunction {
        public Strends(Expr l, Expr r) { super(l, r); }
        @Override public Term eval(Binding b) {
            Rope lLex = requireLexical(l, b);
            Rope rLex = requireLexical(r, b);
            return lLex.has(lLex.len()-rLex.len(), rLex) ? TRUE : FALSE;
        }
        @Override public Expr bound(Binding binding) {
            Expr bl = l.bound(binding), br = r.bound(binding);
            return bl == l && br == r ? this : new Strends(bl, br);
        }
    }

    class Strbefore extends BinaryFunction {
        public Strbefore(Expr l, Expr r) { super(l, r); }
        @Override public Term eval(Binding b) {
            Term outerLit = Expr.requireLiteral(l, b);
            Rope outer = requireNonNull(outerLit.escapedLexical());
            Rope inner = requireLexical(r, b);
            int i = outer.skipUntil(0, outer.len(), inner);
            return outerLit.withLexical(outer.sub(0, i));
        }
        @Override public Expr bound(Binding binding) {
            Expr bl = l.bound(binding), br = r.bound(binding);
            return bl == l && br == r ? this : new Strbefore(bl, br);
        }
    }

    class Strafter extends BinaryFunction {
        public Strafter(Expr l, Expr r) { super(l, r); }
        @Override public Term eval(Binding b) {
            Term outerLit = Expr.requireLiteral(l, b);
            Rope outer = requireNonNull(outerLit.escapedLexical());
            Rope inner = requireLexical(r, b);
            int i = outer.skipUntil(0, outer.len(), inner);
            return outerLit.withLexical(outer.sub(i+inner.len(), outer.len()));
        }
        @Override public Expr bound(Binding binding) {
            Expr bl = l.bound(binding), br = r.bound(binding);
            return bl == l && br == r ? this : new Strafter(bl, br);
        }
    }

    class Regex extends NAryFunction {
        static Pattern compile(Binding binding, Expr regex, @Nullable Expr flags) {
            try {
                String regexStr = requireLexical(regex, binding).toString();
                if (flags != null) {
                    String flagsStr = requireLexical(flags, binding).toString();
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
            if (args[1].isGround() && (args.length < 3 || args[2].isGround()))
                rx = compile(ArrayBinding.EMPTY, args[1], args.length > 2 ? args[2] : null);
            else
                rx = null;
        }

        @Override public Term eval(Binding binding) {
            String text = requireLexical(args[0], binding).toString();
            var p = rx != null ? rx : compile(binding, args[1], args.length > 2 ? args[2] : null);
            return p.matcher(text).find() ? TRUE : FALSE;
        }
        @Override public Expr bound(Binding binding) {
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
            Term text = Expr.requireLiteral(args[0], b);
            String lex = requireNonNull(text.escapedLexical()).toString();
            String replacement = requireLexical(args[2], b).toString();
            return text.withLexical(new ByteRope(p.matcher(lex).replaceAll(replacement)));
        }
        @Override public Expr bound(Binding binding) {
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
            Term text = Expr.requireLiteral(args[0], b);
            int start = args[1].eval(b).asInt();
            Rope lex = text.escapedLexical();
            assert lex != null;
            int end = lex.len();
            if (args.length > 2)
                end = start + args[2].eval(b).asInt();
            return text.withLexical(lex.sub(start, end));
        }
        @Override public Expr bound(Binding binding) {
            Expr[] b = boundArgs(binding);
            return b == args ? this : new Substr(b);
        }
    }

    class Uuid extends Supplier {
        @Override public Term eval(Binding binding) {
            return Term.wrap(("<urn:uuid:" + UUID.randomUUID() + ">").getBytes(UTF_8));
        }
    }

    class Struuid extends Supplier {
        @Override public Term eval(Binding binding) {
            return Term.wrap(("\"" + UUID.randomUUID() + '"').getBytes(UTF_8));
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
        @Override public Expr bound(Binding binding) {
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
            if (args[0].eval(b).asBool())
                return args[1].eval(b);
            else
                return args[2].eval(b);
        }
        @Override public Expr bound(Binding binding) {
            Expr[] b = boundArgs(binding);
            return b == args ? this : new If(b);
        }
    }

    class Strlang extends BinaryFunction {
        public Strlang(Expr l, Expr r) { super(l, r); }
        @Override public Term eval(Binding b) {
            return Term.lang(requireLexical(l, b), requireLexical(r, b));
        }
        @Override public Expr bound(Binding binding) {
            Expr bl = l.bound(binding), br = r.bound(binding);
            return bl == l && br == r ? this : new Strlang(bl, br);
        }
    }

    class Strdt extends BinaryFunction {
        public Strdt(Expr l, Expr r) { super(l, r); }
        @Override public Term eval(Binding b) {
            Term dtTerm = l.eval(b);
            boolean isIRI = dtTerm.type() == Type.IRI;
            return Term.valueOf(of(ByteRope.DQ, requireLexical(l, b),
                                isIRI ? ByteRope.DT_MID : ByteRope.DT_MID_LT,
                                isIRI ? dtTerm : dtTerm.escapedLexical(),
                                isIRI ? ByteRope.EMPTY : ByteRope.GT));
        }
        @Override public Expr bound(Binding binding) {
            Expr bl = l.bound(binding), br = r.bound(binding);
            return bl == l && br == r ? this : new Strdt(bl, br);
        }
    }

    class SameTerm extends BinaryFunction {
        public SameTerm(Expr l, Expr r) { super(l, r); }
        @Override public Term eval(Binding b) {
            return l.eval(b).equals(r.eval(b)) ? TRUE : FALSE;
        }
        @Override public Expr bound(Binding binding) {
            Expr bl = l.bound(binding), br = r.bound(binding);
            return bl == l && br == r ? this : new SameTerm(bl, br);
        }
    }

    class IsIRI extends UnaryFunction {
        public IsIRI(Expr in) { super(in); }
        @Override public Term eval(Binding b) {
            return in.eval(b).type() == Type.IRI ? TRUE : FALSE;
        }
        @Override public Expr bound(Binding binding) {
            Expr b = in.bound(binding);
            return b == in ? this : new IsIRI(b);
        }
    }

    class IsBlank extends UnaryFunction {
        public IsBlank(Expr in) { super(in); }
        @Override public Term eval(Binding b) {
            return in.eval(b).type() == Type.BLANK ? TRUE : FALSE;
        }
        @Override public Expr bound(Binding binding) {
            Expr b = in.bound(binding);
            return b == in ? this : new IsBlank(b);
        }
    }

    class IsLit extends UnaryFunction {
        public IsLit(Expr in) { super(in); }
        @Override public Term eval(Binding b) {
            return in.eval(b).type() == Type.LIT ? TRUE : FALSE;
        }
        @Override public Expr bound(Binding binding) {
            Expr b = in.bound(binding);
            return b == in ? this : new IsLit(b);
        }
    }

    class IsNumeric extends UnaryFunction {
        public IsNumeric(Expr in) { super(in); }
        @Override public Term eval(Binding b) {
            return in.eval(b).asNumber() != null ? TRUE : FALSE;
        }
        @Override public Expr bound(Binding binding) {
            Expr b = in.bound(binding);
            return b == in ? this : new IsNumeric(b);
        }
    }
}
