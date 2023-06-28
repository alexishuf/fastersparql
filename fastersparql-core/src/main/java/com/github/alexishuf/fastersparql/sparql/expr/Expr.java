package com.github.alexishuf.fastersparql.sparql.expr;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.*;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import com.github.alexishuf.fastersparql.sparql.PrefixAssigner;
import com.github.alexishuf.fastersparql.sparql.binding.ArrayBinding;
import com.github.alexishuf.fastersparql.sparql.binding.BatchBinding;
import com.github.alexishuf.fastersparql.sparql.binding.Binding;
import com.github.alexishuf.fastersparql.util.UriUtils;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import static com.github.alexishuf.fastersparql.batch.type.Batch.COMPRESSED;
import static com.github.alexishuf.fastersparql.model.rope.ByteRope.EMPTY;
import static com.github.alexishuf.fastersparql.model.rope.Rope.of;
import static com.github.alexishuf.fastersparql.model.rope.SharedRopes.*;
import static com.github.alexishuf.fastersparql.sparql.expr.Term.*;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;

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

    ExprEvaluator evaluator(Vars vars);

    /** Get an {@link Expr} with all vars in {@code binding} replaced with their
     *  mapped non-var {@link Term}s. If no var in {@code binding} appears in this {@link Expr},
     *  returns {@code this}*/
    Expr bound(Binding binding);

    /** Whether this is an RDF value (a non-variable {@link Term}). */
    default boolean isGround() {
        return this instanceof Term t && t.type() != Type.VAR;
    }

    /** Write this {@link Expr} in SPARQL syntax to {@code out} */
    int toSparql(ByteSink<?, ?> out, PrefixAssigner prefixAssigner);

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
                var batch = it.nextBatch(null);
                COMPRESSED.recycle(batch);
                return (batch != null) ^ negate ? TRUE : FALSE;
            }
        }

        @Override public  ExprEvaluator
        evaluator(Vars vars) {
            return new Eval(vars, this);
        }

        private static final class Eval implements ExprEvaluator {
            private final Plan filter;
            private final boolean negate;
            private final BatchBinding<?> binding;

            public Eval(Vars vars, Exists e) {
                this.filter  = e.filter().toAsk();
                this.negate  = e.negate();
                this.binding = new BatchBinding<>(vars);
            }

            @Override public Term evaluate(Batch<?> batch, int row) {
                binding.setRow(batch, row);
                try (var it = filter.execute(COMPRESSED, binding, true).eager().tempEager()) {
                    var b = it.nextBatch(null);
                    COMPRESSED.recycle(b);
                    return (batch != null) ^ negate ? TRUE : FALSE;
                }
            }
        }

        @Override public Expr bound(Binding binding) {
            Plan bound = filter.bound(binding);
            return bound == filter ? this : new Exists(bound, negate);
        }
        private static final byte[] NOT_EXISTS = "NOT EXISTS".getBytes(UTF_8);
        private static final byte[] EXISTS = "EXISTS".getBytes(UTF_8);

        @Override public int toSparql(ByteSink<?, ?> out, PrefixAssigner assigner) {
            int oldLen = out.len();
            int indent = 0;
            if (out instanceof ByteRope r) {
                if (r.len() != 0) {
                    int lineBegin = r.reverseSkip(0, r.len, Rope.UNTIL_LF);
                    if (r.get(lineBegin) == '\n') lineBegin++;
                    indent = r.skip(lineBegin, r.len(), Rope.WS) - lineBegin;
                }
            }
            out.append(negate ? NOT_EXISTS : EXISTS);
            filter.groupGraphPattern(out, indent, assigner);
            return out.len()-oldLen;
        }

        @Override public String toString() {
            var sb = new ByteRope(256);
            toSparql(sb, PrefixAssigner.NOP);
            return sb.toString();
        }
    }

    private static Term requireLiteral(Expr expr, Term value) {
        if (value == null || value.type() != Type.LIT)
            throw new InvalidExprTypeException(expr, value, "literal");
        return value;
    }
    private static Term requireLiteral(Expr expr, Binding binding) {
        Term term = expr.eval(binding);
        if (term.type() != Type.LIT)
            throw new InvalidExprTypeException(expr, term, "literal");
        return term;
    }

    private static SegmentRope requireLexical(Expr expr, Term term) {
        SegmentRope lex = term.escapedLexical();
        if (lex == null)
            throw new InvalidExprTypeException(expr, term, "literal");
        return lex;
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

        @Override public int toSparql(ByteSink<?, ?> out, PrefixAssigner assigner) {
            int oldLen = out.len();
            out.append(sparqlName()).append('(');
            int n = argCount();
            for (int i = 0; i < n; i++) {
                if (i > 0) out.append(',').append(' ');
                arg(i).toSparql(out, assigner);
            }
            out.append(')');
            return out.len()-oldLen;
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
        public abstract Term eval(Term term);
        @Override public final Term eval(Binding binding) {return eval(in.eval(binding));}
        @Override
        public  ExprEvaluator evaluator(Vars vars) {
            return new Eval(vars, this);
        }
        @Override public int argCount() { return 1; }
        @Override public Expr arg(int i) { return in; }

        protected static class Eval implements ExprEvaluator {
            final UnaryFunction unaryFunction;
            final ExprEvaluator in;
            public Eval(Vars vars, UnaryFunction f) {
                this.unaryFunction = f;
                this.in = f.in.evaluator(vars);
            }

            @Override public Term evaluate(Batch<?> batch, int row) {
                return unaryFunction.eval(in.evaluate(batch, row));
            }
        }
    }

    abstract class UnaryOperator extends UnaryFunction {
        public UnaryOperator(Expr in) { super(in); }

        @Override public int toSparql(ByteSink<?, ?> out, PrefixAssigner assigner) {
            return in.toSparql(out.append(sparqlName()), assigner);
        }
    }

    non-sealed abstract class BinaryFunction extends Function {
        protected final Expr l;
        protected final Expr r;
        public BinaryFunction(Expr l, Expr r) { this.l = l; this.r = r; }
        protected abstract Term eval(Term l, Term r);
        @Override public final Term eval(Binding binding) {
            return eval(l.eval(binding), r.eval(binding));
        }
        @Override public int argCount() { return 2; }
        @Override public Expr arg(int i) {
            return switch (i) {
                case 0 -> l; case 1 -> r; default -> throw new IndexOutOfBoundsException(i);
            };
        }

        static abstract class Eval implements ExprEvaluator {
            protected final ExprEvaluator l, r;
            protected final Expr lExpr, rExpr;
            public Eval(Vars vars, Expr l, Expr r) {
                this.lExpr = l;
                this.rExpr = r;
                this.l = l.evaluator(vars);
                this.r = r.evaluator(vars);
            }
        }
    }

    abstract class BinaryOperator extends BinaryFunction {
        public BinaryOperator(Expr l, Expr r) { super(l, r); }

        @Override public int toSparql(ByteSink<?, ?> out, PrefixAssigner assigner) {
            int oldLen = out.len();
            out.append('(');
            l.toSparql(out, assigner);
            out.append(' ').append(sparqlName()).append(' ');
            r.toSparql(out, assigner);
            out.append(')');
            return out.len()-oldLen;
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
        @Override public Term eval(Term l, Term r) {
            return l.asBool() || r.asBool() ? TRUE : FALSE;
        }
        @Override public ExprEvaluator evaluator(Vars vars) {return new OrEval(vars, l, r);}
        @Override public Expr bound(Binding binding) {
            Expr bl = l.bound(binding), br = r.bound(binding);
            return bl == l && br == r ? this : new Or(bl, br);
        }
        private static final class OrEval extends Eval {
            public OrEval(Vars vars, Expr l, Expr r) {super(vars, l, r);}
            @Override public Term evaluate(Batch<?> b, int row) {
                return l.evaluate(b, row).asBool() || r.evaluate(b, row).asBool() ? TRUE : FALSE;
            }
        }
    }

    class And extends BinaryOperator {
        public And(Expr l, Expr r) { super(l, r); }
        @Override public String sparqlName() { return "&&"; }
        @Override public Term eval(Term l, Term r) {
            return l.asBool() && r.asBool() ? TRUE : FALSE;
        }
        @Override public ExprEvaluator evaluator(Vars vars) {return new AndEval(vars, l, r);}
        @Override public Expr bound(Binding binding) {
            Expr bl = l.bound(binding), br = r.bound(binding);
            return bl == l && br == r ? this : new And(bl, br);
        }
        private static final class AndEval extends Eval {
            public AndEval(Vars vars, Expr l, Expr r) {super(vars, l, r);}
            @Override public Term evaluate(Batch<?> b, int row) {
                return l.evaluate(b, row).asBool() && r.evaluate(b, row).asBool() ? TRUE : FALSE;
            }
        }
    }

    class Eq extends BinaryOperator {
        public Eq(Expr l, Expr r) { super(l, r); }
        @Override public String sparqlName() { return "="; }
        @Override public Term eval(Term l, Term r) { return l.equals(r) ? TRUE : FALSE; }
        @Override public ExprEvaluator evaluator(Vars vars) {return new EqEval(vars, l, r);}
        @Override public Expr bound(Binding binding) {
            Expr bl = l.bound(binding), br = r.bound(binding);
            return bl == l && br == r ? this : new Eq(bl, br);
        }
        private static final class EqEval extends Eval {
            public EqEval(Vars vars, Expr l, Expr r) {super(vars, l, r);}
            @Override public Term evaluate(Batch<?> b, int row) {
                return l.evaluate(b, row).equals(r.evaluate(b, row)) ? TRUE : FALSE;
            }
        }
    }

    class Neq extends BinaryOperator {
        public Neq(Expr l, Expr r) { super(l, r); }
        @Override public String sparqlName() { return "!="; }
        @Override public Term eval(Term l, Term r) { return !l.equals(r) ? TRUE : FALSE; }
        @Override public ExprEvaluator evaluator(Vars vars) {return new NeqEval(vars, l, r);}
        @Override public Expr bound(Binding binding) {
            Expr bl = l.bound(binding), br = r.bound(binding);
            return bl == l && br == r ? this : new Neq(bl, br);
        }
        private static final class NeqEval extends Eval {
            public NeqEval(Vars vars, Expr l, Expr r) {super(vars, l, r);}
            @Override public Term evaluate(Batch<?> b, int row) {
                return l.evaluate(b, row).equals(r.evaluate(b, row)) ? FALSE : TRUE;
            }
        }
    }

    class Lt extends BinaryOperator {
        public Lt(Expr l, Expr r) { super(l, r); }
        @Override public String sparqlName() { return "<"; }
        @Override public Term eval(Term l, Term r) {
            return l.compareTo(r) < 0 ? TRUE : FALSE;
        }
        @Override public ExprEvaluator evaluator(Vars vars) {return new LtEval(vars, l, r);}
        @Override public Expr bound(Binding binding) {
            Expr bl = l.bound(binding), br = r.bound(binding);
            return bl == l && br == r ? this : new Lt(bl, br);
        }
        private static final class LtEval extends Eval {
            public LtEval(Vars vars, Expr l, Expr r) {super(vars, l, r);}
            @Override public Term evaluate(Batch<?> b, int row) {
                return l.evaluate(b, row).compareTo(r.evaluate(b, row)) < 0 ? TRUE : FALSE;
            }
        }
    }

    class Gt extends BinaryOperator {
        public Gt(Expr l, Expr r) { super(l, r); }
        @Override public String sparqlName() { return ">"; }
        @Override public Term eval(Term l, Term r) {
            return l.compareTo(r) > 0 ? TRUE : FALSE;
        }
        @Override public ExprEvaluator evaluator(Vars vars) {return new GtEval(vars, l, r);}
        @Override public Expr bound(Binding binding) {
            Expr bl = l.bound(binding), br = r.bound(binding);
            return bl == l && br == r ? this : new Gt(bl, br);
        }
        private static final class GtEval extends Eval {
            public GtEval(Vars vars, Expr l, Expr r) {super(vars, l, r);}
            @Override public Term evaluate(Batch<?> b, int row) {
                return l.evaluate(b, row).compareTo(r.evaluate(b, row)) > 0 ? TRUE : FALSE;
            }
        }
    }

    class Lte extends BinaryOperator {
        public Lte(Expr l, Expr r) { super(l, r); }
        @Override public String sparqlName() { return "<="; }
        @Override public Term eval(Term l, Term r) {
            return l.compareTo(r) <= 0 ? TRUE : FALSE;
        }
        @Override public ExprEvaluator evaluator(Vars vars) {return new LteEval(vars, l, r);}
        @Override public Expr bound(Binding binding) {
            Expr bl = l.bound(binding), br = r.bound(binding);
            return bl == l && br == r ? this : new Lte(bl, br);
        }
        private static final class LteEval extends Eval {
            public LteEval(Vars vars, Expr l, Expr r) {super(vars, l, r);}
            @Override public Term evaluate(Batch<?> b, int row) {
                return l.evaluate(b, row).compareTo(r.evaluate(b, row)) <= 0 ? TRUE : FALSE;
            }
        }
    }

    class Gte extends BinaryOperator {
        public Gte(Expr l, Expr r) { super(l, r); }
        @Override public String sparqlName() { return ">="; }
        @Override public Term eval(Term l, Term r) {
            return l.compareTo(r) >= 0 ? TRUE : FALSE;
        }
        @Override public ExprEvaluator evaluator(Vars vars) {return new GteEval(vars, l, r);}
        @Override public Expr bound(Binding binding) {
            Expr bl = l.bound(binding), br = r.bound(binding);
            return bl == l && br == r ? this : new Gte(bl, br);
        }
        private static final class GteEval extends Eval {
            public GteEval(Vars vars, Expr l, Expr r) {super(vars, l, r);}
            @Override public Term evaluate(Batch<?> b, int row) {
                return l.evaluate(b, row).compareTo(r.evaluate(b, row)) >= 0 ? TRUE : FALSE;
            }
        }
    }

    class Neg extends UnaryOperator {
        public Neg(Expr in) { super(in); }
        @Override public String sparqlName() { return "!"; }
        @Override public Term eval(Term term) {return term.negate();}
        @Override public Expr bound(Binding binding) {
            Expr b = in.bound(binding);
            return b == in ? this : new Neg(b);
        }
    }

    class Minus extends UnaryOperator {
        public Minus(Expr in) { super(in); }
        @Override public String sparqlName() { return "-"; }
        @Override public Term eval(Term term) {
            return term.negate();
        }
        @Override public Expr bound(Binding binding) {
            Expr b = in.bound(binding);
            return b == in ? this : new Minus(b);
        }
        @Override
        public  ExprEvaluator evaluator(Vars vars) {
            return new MinusEval(vars, this);
        }

        private static final class MinusEval extends Eval {
            private final ByteRope tmpLocal = new ByteRope(12);
            private final Term result = Term.mutable();

            public MinusEval(Vars vars, UnaryFunction f) {
                super(vars, f);
            }
            @Override public Term evaluate(Batch<?> batch, int row) {
                Term in = this.in.evaluate(batch, row);
                Expr.requireLiteral(unaryFunction.in, in);
                SegmentRope sh = in.shared(), local = in.first();
                if (!in.isNumeric())
                    throw new InvalidExprTypeException(unaryFunction.in, in, "numeric");
                tmpLocal.clear().ensureFreeCapacity(local.len+1).append('"');
                if (local.get(1) == '-')
                    tmpLocal.append(local, 2, local.len);
                else
                    tmpLocal.append('-').append(local, 1, local.len);
                result.set(sh, tmpLocal, true);
                return result;
            }
        }
    }

    class Add extends BinaryOperator {
        public Add(Expr l, Expr r) { super(l, r); }
        @Override public String sparqlName() { return "+"; }
        @Override public Term eval(Term l, Term r) {return l.add(r);}
        @Override public ExprEvaluator evaluator(Vars vars) {return new AddEval(vars, l, r);}
        @Override public Expr bound(Binding binding) {
            Expr bl = l.bound(binding), br = r.bound(binding);
            return bl == l && br == r ? this : new Add(bl, br);
        }
        private static final class AddEval extends Eval {
            public AddEval(Vars vars, Expr l, Expr r) {super(vars, l, r);}
            @Override public Term evaluate(Batch<?> b, int row) {
                return l.evaluate(b, row).add(r.evaluate(b, row));
            }
        }
    }

    class Subtract extends BinaryOperator {
        public Subtract(Expr l, Expr r) { super(l, r); }
        @Override public String sparqlName() { return "-"; }
        @Override public Term eval(Term l, Term r) {return l.subtract(r);}
        @Override public ExprEvaluator evaluator(Vars vars) {return new SubtractEval(vars, l, r);}
        @Override public Expr bound(Binding binding) {
            Expr bl = l.bound(binding), br = r.bound(binding);
            return bl == l && br == r ? this : new Subtract(bl, br);
        }
        private static final class SubtractEval extends Eval {
            public SubtractEval(Vars vars, Expr l, Expr r) {super(vars, l, r);}
            @Override public Term evaluate(Batch<?> b, int row) {
                return l.evaluate(b, row).subtract(r.evaluate(b, row));
            }
        }
    }

    class Multiply extends BinaryOperator {
        public Multiply(Expr l, Expr r) { super(l, r); }
        @Override public String sparqlName() { return "*"; }
        @Override public Term eval(Term l, Term r) {return l.multiply(r);}
        @Override public ExprEvaluator evaluator(Vars vars) {return new MultiplyEval(vars, l, r);}
        @Override public Expr bound(Binding binding) {
            Expr bl = l.bound(binding), br = r.bound(binding);
            return bl == l && br == r ? this : new Multiply(bl, br);
        }
        private static final class MultiplyEval extends Eval {
            public MultiplyEval(Vars vars, Expr l, Expr r) {super(vars, l, r);}
            @Override public Term evaluate(Batch<?> b, int row) {
                return l.evaluate(b, row).multiply(r.evaluate(b, row));
            }
        }
    }

    class Divide extends BinaryOperator {
        public Divide(Expr l, Expr r) { super(l, r); }
        @Override public String sparqlName() { return "/"; }
        @Override public Term eval(Term l, Term r) {return l.divide(r);}
        @Override public ExprEvaluator evaluator(Vars vars) {return new DivideEval(vars, l, r);}
        @Override public Expr bound(Binding binding) {
            Expr bl = l.bound(binding), br = r.bound(binding);
            return bl == l && br == r ? this : new Divide(bl, br);
        }
        private static final class DivideEval extends Eval {
            public DivideEval(Vars vars, Expr l, Expr r) {super(vars, l, r);}
            @Override public Term evaluate(Batch<?> b, int row) {
                return l.evaluate(b, row).divide(r.evaluate(b, row));
            }
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

        @Override
        public  ExprEvaluator evaluator(Vars vars) {
            return new InEval(vars, args, false);
        }

        @Override public Expr bound(Binding binding) {
            Expr[] b = boundArgs(binding);
            return b == args ? this : new In(b);
        }
        private static final class InEval implements ExprEvaluator {
            private final Term onMatch, onMismatch;
            private final ExprEvaluator[] evals;
            private final Expr[] args;
            public InEval(Vars vars, Expr[] args, boolean negate) {
                this.onMatch    = negate ? FALSE : TRUE;
                this.onMismatch = negate ? TRUE  : FALSE;
                this.args       = args;
                this.evals = new ExprEvaluator[args.length];
                for (int i = 0; i < evals.length; i++)
                    evals[i] = args[i].evaluator(vars);
            }
            @Override public Term evaluate(Batch<?> batch, int row) {
                Term key = evals[0].evaluate(batch, row);
                if (key == null) throw new InvalidExprTypeException(args[0], key, "bound");
                for (int i = 1; i < evals.length; i++)
                    if (key.equals(evals[i].evaluate(batch, row))) return onMatch;
                return onMismatch;
            }
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
        @Override
        public  ExprEvaluator evaluator(Vars vars) {
            return new In.InEval(vars, args, true);
        }
        @Override public Expr bound(Binding binding) {
            Expr[] b = boundArgs(binding);
            return b == args ? this : new NotIn(b);
        }
    }

    class Str extends UnaryFunction {
        public Str(Expr in) { super(in); }
        @Override public Term eval(Term term) {
            return switch (term.type()) {
                case LIT -> {
                    if (term.datatypeSuff() == SharedRopes.DT_string)
                        yield term;
                    int endLex = term.endLex();
                    ByteRope tmp = new ByteRope(endLex + 1).append(term.first(), 0, endLex).append('"');
                    yield new Term(EMPTY, tmp, true);
                }
                case IRI -> {
                    ByteRope tmp = new ByteRope(term.toArray(0, term.len));
                    byte[] u8 = tmp.u8();
                    u8[0] = '"';
                    u8[term.len-1] = '"';
                    yield new Term(EMPTY, tmp, true);
                }
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
        @Override public Term eval(Term term) {
            Expr.requireLiteral(in, term);
            Rope lang = term.lang();
            if (lang == null) return EMPTY_STRING;
            return Term.wrap(new ByteRope().append('"').append(lang).append('"'), null);
        }
        @Override public Expr bound(Binding binding) {
            Expr b = in.bound(binding);
            return b == in ? this : new Lang(b);
        }
    }

    class Datatype extends UnaryFunction {
        public Datatype(Expr in) { super(in); }
        @Override public Term eval(Term term) {
            return Expr.requireLiteral(in, term).datatypeTerm();
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
            SegmentRope lex;
            if (args.length == 0) {
                lex = new ByteRope().append("_:").append(randomUUID().toString().getBytes(UTF_8));
            } else {
                Term term = args[0].eval(binding);
                lex = term.escapedLexical();
                if (lex == null || !lex.has(0, BN_PREFIX))
                    throw new InvalidExprTypeException(args[0], term, "literal with _:-prefixed lexical form");
            }
            return valueOf(lex, 0, lex.len());
        }

        @Override
        public  ExprEvaluator evaluator(Vars vars) {
            return new MakeBNodeEval(vars);
        }

        @Override public Expr bound(Binding binding) {
            Expr[] b = boundArgs(binding);
            return b == args ? this : new MakeBNode(b);
        }

        private final class MakeBNodeEval implements ExprEvaluator {
            private final ExprEvaluator argEval;
            private final ByteRope tmp = new ByteRope(39).append("\"_:");
            private final Term result = Term.mutable();
            public MakeBNodeEval(Vars vars) {
                argEval = args.length == 0 ? null : args[0].evaluator(vars);
            }

            @Override public Term evaluate(Batch<?> batch, int row) {
                SegmentRope local;
                int lexLen;
                if (argEval == null) {
                    tmp.clear().len = 2;
                    local = tmp.append(randomUUID().toString().getBytes(UTF_8));
                    lexLen = tmp.len-1;
                } else {
                    Term in = argEval.evaluate(batch, row);
                    Expr.requireLiteral(args[0], in);
                    local = in.local();
                    lexLen = local.len - (in.shared().len > 0 ? 1 : 2);
                }
                result.set(EMPTY, local.segment, local.utf8, local.offset+1, lexLen, true);
                return result;
            }
        }
    }

    class Bound extends UnaryFunction {
        public Bound(Expr in) { super(in); }
        @Override public Term eval(Term term) {
            return term != null && term.type() != Type.VAR ? TRUE : FALSE;
        }
        @Override public Expr bound(Binding binding) {
            Expr b = in.bound(binding);
            return b == in ? this : new Bound(b);
        }
    }

    class Iri extends UnaryFunction {
        public Iri(Expr in) { super(in); }
        @Override public Term eval(Term value) {
            return switch (value.type()) {
                case IRI -> value;
                case LIT -> {
                    int endLex = value.endLex();
                    var tmp = new ByteRope(endLex + 1);
                    tmp.append('<').append(value.first(), 1, endLex).append('>');
                    yield new Term(null, tmp, false);
                }
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
            return Term.wrap(new ByteRope().append('"').append(Math.random()), DT_DOUBLE);
        }

        @Override
        public  ExprEvaluator evaluator(Vars vars) {
            return new RandEval();
        }

        private static final class RandEval implements ExprEvaluator {
            private final ByteRope tmp = new ByteRope(32).append('"');
            private final Term result = Term.mutable();
            @Override public Term evaluate(Batch<?> batch, int row) {
                tmp.clear().len = 1;
                tmp.append(Math.random());
                result.set(DT_DOUBLE, tmp, true);
                return result;
            }
        }
    }

    class Abs extends UnaryFunction {
        public Abs(Expr in) { super(in); }
        @Override public Term eval(Term t) { return t.abs(); }
        @Override public Expr bound(Binding binding) {
            Expr b = in.bound(binding);
            return b == in ? this : new Abs(b);
        }
    }
    class Floor extends UnaryFunction {
        public Floor(Expr in) { super(in); }
        @Override public Term eval(Term t) { return t.floor(); }
        @Override public Expr bound(Binding binding) {
            Expr b = in.bound(binding);
            return b == in ? this : new Floor(b);
        }
    }
    class Ceil extends UnaryFunction {
        public Ceil(Expr in) { super(in); }
        @Override public Term eval(Term t) { return t.ceil(); }
        @Override public Expr bound(Binding binding) {
            Expr b = in.bound(binding);
            return b == in ? this : new Ceil(b);
        }
    }
    class Round extends UnaryFunction {
        public Round(Expr in) { super(in); }
        @Override public Term eval(Term t) { return t.round(); }
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
            SegmentRope dt = first.datatypeSuff();
            if (dt == DT_langString) {
                result.append('"').append('@').append(requireNonNull(first.lang()));
                return Term.splitAndWrap(result);
            }
            if (dt == null || dt == DT_string) {
                dt = null;
                result.append('"');
            }
            return new Term(dt, result, true);
        }

        @Override
        public  ExprEvaluator evaluator(Vars vars) {
            return new ConcatEval(vars);
        }

        @Override public Expr bound(Binding binding) {
            Expr[] b = boundArgs(binding);
            return b == args ? this : new Concat(b);
        }

        private final class ConcatEval implements ExprEvaluator {
            private final ExprEvaluator[] evals;
            private final Term[] terms;
            private final ByteRope tmp = new ByteRope(64-16).append('"');
            private final Term result = Term.mutable();
            public ConcatEval(Vars vars) {
                terms = new Term[args.length];
                evals = new ExprEvaluator[args.length];
                for (int i = 0; i < evals.length; i++)
                    evals[i] = args[i].evaluator(vars);
            }
            @Override public Term evaluate(Batch<?> batch, int row) {
                tmp.clear().len = 1; // "
                int required = 1/*"*/ + 6/*@en-US*/;
                for (int i = 0; i < evals.length; i++) {
                    terms[i] = Expr.requireLiteral(args[i], evals[i].evaluate(batch, row));
                    required += terms[i].local().len;
                }
                if (required == 1)
                    return EMPTY_STRING;
                tmp.ensureFreeCapacity(required);
                for (Term t : terms) {
                    SegmentRope local = t.local();
                    tmp.append(local, 1, local.len - (t.shared().len() == 0 ? 1 : 0));
                }
                tmp.append('"');
                SegmentRope dt = terms[0].datatypeSuff();
                if (dt == DT_langString)
                    tmp.append('@').append(terms[0].lang());
                result.set(EMPTY, tmp, true);
                return result;
            }
        }
    }

    class Strlen extends UnaryFunction {
        public Strlen(Expr in) { super(in); }
        @Override public Term eval(Term term) {
            int endLex = term.endLex();
            if (endLex < 0) throw new InvalidExprTypeException(in, term, "literal");
            return Term.wrap(new ByteRope().append('"').append(endLex-1), DT_integer);
        }
        @Override public Expr bound(Binding binding) {
            Expr b = in.bound(binding);
            return b == in ? this : new Strlen(b);
        }
    }

    class UCase extends UnaryFunction {
        public UCase(Expr in) { super(in); }
        @Override public Term eval(Term lit) {
            Rope lex = Expr.requireLiteral(this, lit).escapedLexical();
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
        @Override public Term eval(Term lit) {
            Rope lex = Expr.requireLiteral(this, lit).escapedLexical();
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

        @Override public Term eval(Term lit) {
            Rope lex = Expr.requireLiteral(this, lit).escapedLexical();
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
        @Override public Term eval(Term l, Term r) {
            Rope lLex = requireLexical(this.l, l);
            Rope rLex = requireLexical(this.r, r);
            return lLex.skipUntil(0, lLex.len(), rLex) < lLex.len() ? TRUE : FALSE;
        }
        @Override public ExprEvaluator evaluator(Vars vars) {return new ContainsEval(vars, l, r);}
        @Override public Expr bound(Binding binding) {
            Expr bl = l.bound(binding), br = r.bound(binding);
            return bl == l && br == r ? this : new Contains(bl, br);
        }
        private static final class ContainsEval extends Eval {
            public ContainsEval(Vars vars, Expr l, Expr r) {super(vars, l, r);}
            @Override public Term evaluate(Batch<?> b, int row) {
                Rope lLex = requireLexical(lExpr, l.evaluate(b, row));
                Rope rLex = requireLexical(rExpr, r.evaluate(b, row));
                return lLex.skipUntil(0, lLex.len(), rLex) < lLex.len() ? TRUE : FALSE;
            }
        }
    }

    class Strstarts extends BinaryFunction {
        public Strstarts(Expr l, Expr r) { super(l, r); }
        @Override public Term eval(Term l, Term r) {
            Rope lLex = requireLexical(this.l, l);
            Rope rLex = requireLexical(this.r, r);
            return lLex.has(0, rLex) ? TRUE : FALSE;
        }
        @Override public ExprEvaluator evaluator(Vars vars) {return new StrstartsEval(vars, l, r);}
        @Override public Expr bound(Binding binding) {
            Expr bl = l.bound(binding), br = r.bound(binding);
            return bl == l && br == r ? this : new Strstarts(bl, br);
        }
        private static final class StrstartsEval extends Eval {
            public StrstartsEval(Vars vars, Expr l, Expr r) {super(vars, l, r);}
            @Override public Term evaluate(Batch<?> batch, int row) {
                Rope lLex = requireLexical(lExpr, l.evaluate(batch, row));
                Rope rLex = requireLexical(rExpr, r.evaluate(batch, row));
                return lLex.has(0, rLex) ? TRUE : FALSE;
            }
        }
    }

    class Strends extends BinaryFunction {
        public Strends(Expr l, Expr r) { super(l, r); }
        @Override public Term eval(Term l, Term r) {
            Rope lLex = requireLexical(this.l, l);
            Rope rLex = requireLexical(this.r, r);
            return lLex.has(lLex.len()-rLex.len(), rLex) ? TRUE : FALSE;
        }
        @Override public ExprEvaluator evaluator(Vars vars) {return new StrendsEval(vars, l, r);}
        @Override public Expr bound(Binding binding) {
            Expr bl = l.bound(binding), br = r.bound(binding);
            return bl == l && br == r ? this : new Strends(bl, br);
        }
        private static final class StrendsEval extends Eval {
            public StrendsEval(Vars vars, Expr l, Expr r) {super(vars, l, r);}
            @Override public Term evaluate(Batch<?> batch, int row) {
                Rope lLex = requireLexical(lExpr, l.evaluate(batch, row));
                Rope rLex = requireLexical(rExpr, r.evaluate(batch, row));
                return lLex.has(lLex.len()-rLex.len(), rLex) ? TRUE : FALSE;
            }
        }
    }

    class Strbefore extends BinaryFunction {
        public Strbefore(Expr l, Expr r) { super(l, r); }
        @Override public Term eval(Term l, Term r) {
            Term outerLit = Expr.requireLiteral(this.l, l);
            Rope outer = requireNonNull(outerLit.escapedLexical());
            Rope inner = requireLexical(this.r, r);
            int i = outer.skipUntil(0, outer.len(), inner);
            return outerLit.withLexical(outer.sub(0, i));
        }
        @Override public ExprEvaluator evaluator(Vars vars) {return new StrbeforeEval(vars, l, r);}
        @Override public Expr bound(Binding binding) {
            Expr bl = l.bound(binding), br = r.bound(binding);
            return bl == l && br == r ? this : new Strbefore(bl, br);
        }
        private static final class StrbeforeEval extends Eval {
            public StrbeforeEval(Vars vars, Expr l, Expr r) {super(vars, l, r);}
            @Override public Term evaluate(Batch<?> batch, int row) {
                Term outerLit = Expr.requireLiteral(lExpr, l.evaluate(batch, row));
                Rope outer = requireNonNull(outerLit.escapedLexical());
                Rope inner = requireLexical(rExpr, r.evaluate(batch, row));
                int i = outer.skipUntil(0, outer.len(), inner);
                return outerLit.withLexical(outer.sub(0, i));
            }
        }
    }

    class Strafter extends BinaryFunction {
        public Strafter(Expr l, Expr r) { super(l, r); }
        @Override public Term eval(Term l, Term r) {
            Term outerLit = Expr.requireLiteral(this.l, l);
            Rope outer = requireNonNull(outerLit.escapedLexical());
            Rope inner = requireLexical(this.r, r);
            int i = outer.skipUntil(0, outer.len(), inner);
            return outerLit.withLexical(outer.sub(i+inner.len(), outer.len()));
        }
        @Override public ExprEvaluator evaluator(Vars vars) {return new StrafterEval(vars, l, r);}
        @Override public Expr bound(Binding binding) {
            Expr bl = l.bound(binding), br = r.bound(binding);
            return bl == l && br == r ? this : new Strafter(bl, br);
        }
        private static final class StrafterEval extends Eval {
            public StrafterEval(Vars vars, Expr l, Expr r) {super(vars, l, r);}
            @Override public Term evaluate(Batch<?> batch, int row) {
                Term outerLit = Expr.requireLiteral(lExpr, l.evaluate(batch, row));
                Rope outer = requireNonNull(outerLit.escapedLexical());
                Rope inner = requireLexical(rExpr, r.evaluate(batch, row));
                int i = outer.skipUntil(0, outer.len(), inner);
                return outerLit.withLexical(outer.sub(i+inner.len(), outer.len()));
            }
        }
    }

    class Regex extends NAryFunction {
        static Pattern compile(Binding binding, Expr regex, @Nullable Expr flags) {
            return compile(regex, regex.eval(binding),
                           flags, flags == null ? null : flags.eval(binding));
        }
        static Pattern compile(Expr regexExpr, Term regex,
                               @Nullable Expr flagsExpr, @Nullable Term flags) {
            try {
                String regexStr = requireLexical(regexExpr, regex).toString();
                if (flags != null) {
                    String flagsStr = requireLexical(flagsExpr, flags).toString();
                    //noinspection StringBufferReplaceableByString
                    regexStr = new StringBuilder().append('(').append('?').append(flagsStr).append(')').append(regexStr).toString();
                }
                return Pattern.compile(regexStr);
            }  catch (PatternSyntaxException e) {
                throw new InvalidExprException("Bad REGEX: "+e.getMessage());
            }
        }

        private final @Nullable Pattern rx;
        private static final int[] REGEX_SPECIAL_EX_OR = Rope.alphabet("$()*+.?[]^{}");
        private final byte[][] orBranches;
        public Regex(Expr... args) { // REGEX(text, pattern[, flags])
            super(args);
            if (args.length < 2 || args.length > 3)
                throw new IllegalArgumentException("REGEX takes 2 or 3 arguments, got "+args.length);
            Expr flags = args.length > 2 && args[2].isGround() ? args[2] : null;
            if (args[1].isGround() && flags != null) {
                rx = compile(ArrayBinding.EMPTY, args[1], flags);
                SegmentRope local = ((Term) args[1]).local();
                boolean isOr = true;
                for (int i = 1, end = local.len; i < end; i++) {
                    byte c = local.get(i);
                    if (c == '\\' || c < 0) {
                        ++i;
                    } else if (c != '|' && c > 0 && (REGEX_SPECIAL_EX_OR[c>>5] & (1<<c)) != 0) {
                        isOr = false;
                        break;
                    }
                }
                if (isOr) {
                    List<byte[]> branches = new ArrayList<>();
                    int end = local.skipUntilLast(1, local.len, '"');
                    for (int i = 1, j; i < end; i = j + 1) {
                        j = local.skipUntilUnescaped(i, end, '|');
                        branches.add(local.toArray(i, j));
                    }
                    orBranches = branches.toArray(new byte[0][]);
                } else {
                    orBranches = null;
                }
            } else {
                rx = null;
                orBranches = null;
            }
        }

        @Override public Term eval(Binding binding) {
            String text = requireLexical(args[0], binding).toString();
            var p = rx != null ? rx : compile(binding, args[1], args.length > 2 ? args[2] : null);
            return p.matcher(text).find() ? TRUE : FALSE;
        }

        @Override
        public  ExprEvaluator evaluator(Vars vars) {
            if (orBranches != null)
                return new OrEval(vars, orBranches);
            return new RegexEval(vars);
        }

        @Override public Expr bound(Binding binding) {
            Expr[] b = boundArgs(binding);
            return b == args ? this : new Regex(b);
        }

        private final class RegexEval implements ExprEvaluator {
            private final ExprEvaluator textEval;
            private final ExprEvaluator patternEval;
            private final @Nullable  ExprEvaluator flagsEval;

            public RegexEval(Vars vars) {
                textEval = args[0].evaluator(vars);
                patternEval = args[1].evaluator(vars);
                flagsEval = args.length > 2 ? args[2].evaluator(vars) : null;
            }

            private Pattern compile(Batch<?> batch, int row) {
                Term pattern = patternEval.evaluate(batch, row);
                Term flags = flagsEval == null ? null : flagsEval.evaluate(batch, row);
                return Regex.compile(args[1], pattern, args.length > 2 ? args[2] : null, flags);
            }

            @Override public Term evaluate(Batch<?> batch, int row) {
                Term text = Expr.requireLiteral(args[0], textEval.evaluate(batch, row));
                SegmentRope local = text.local();
                int end = local.len - (text.shared().len > 0 ? 1 : 0);
                Pattern pattern = rx;
                if (pattern == null) pattern = compile(batch, row);
                byte[] u8 = new byte[end - 1];
                local.copy(1, end, u8, 0);
                return pattern.matcher(new String(u8, UTF_8)).find() ? TRUE : FALSE;
            }
        }

        private final class OrEval implements ExprEvaluator {
            private final byte[][] branches;
            private final ExprEvaluator textEval;

            public OrEval(Vars vars, byte[][] branches) {
                this.textEval = args[0].evaluator(vars);
                this.branches = branches;
            }

            @Override public Term evaluate(Batch<?> batch, int row) {
                Term text = Expr.requireLiteral(args[0], textEval.evaluate(batch, row));
                SegmentRope local = text.local();
                int len = local.len - (text.shared().len == 0 ? 2 : 1);
                for (byte[] branch : branches) {
                    if (len == branch.length && local.has(1, branch)) return TRUE;
                }
                return FALSE;
            }
        }
    }

    class Replace extends NAryFunction {
        private final @Nullable Pattern rx;
        public Replace(Expr... args) { //REPLACE(text, pattern, replacement[, flags])
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

        @Override
        public  ExprEvaluator evaluator(Vars vars) {
            return new ReplaceEval(vars, args[0], args[1], args[2],
                                   args.length > 3 ? args[4] : null);
        }

        @Override public Expr bound(Binding binding) {
            Expr[] b = boundArgs(binding);
            return b == args ? this : new Replace(b);
        }
        private static final class ReplaceEval implements ExprEvaluator {
            private final Expr textExpr, patternExpr, replExpr;
            private final @Nullable Expr flagsExpr;
            private final @Nullable Pattern pattern;
            private final ExprEvaluator textEval, patternEval, replEval;
            private final ExprEvaluator flagsEval;
            private final StringBuilder sb = new StringBuilder(48);
            private final ByteRope tmpRope = new ByteRope(48);
            private final Term result = Term.mutable();

            public ReplaceEval(Vars vars, Expr text, Expr pattern, Expr replacement,
                               @Nullable Expr flags) {
                textExpr = text;
                patternExpr = pattern;
                if (pattern instanceof Term p && (flags == null || flags instanceof Term))
                    this.pattern = Regex.compile(pattern, p, flags, (Term)flags);
                else
                    this.pattern = null;
                replExpr = replacement;
                flagsExpr = flags;
                textEval = text.evaluator(vars);
                patternEval = pattern.evaluator(vars);
                replEval = replacement.evaluator(vars);
                flagsEval = flags == null ? null : flags.evaluator(vars);
            }

            private Pattern compile(Batch<?> batch, int row) {
                Term pattern = patternEval.evaluate(batch, row);
                Term flags = flagsEval == null ? null : flagsEval.evaluate(batch, row);
                return Regex.compile(patternExpr, pattern, flagsExpr, flags);
            }

            @Override public Term evaluate(Batch<?> batch, int row) {
                Term text = Expr.requireLiteral(textExpr, textEval.evaluate(batch, row));
                Term repl = Expr.requireLiteral(replExpr, replEval.evaluate(batch, row));
                SegmentRope textLocal = text.local();
                SegmentRope replLocal = repl.local();
                byte[] u8 = new byte[Math.max(textLocal.len, replLocal.len)];

                int endLex;
                replLocal.copy(1, (endLex = repl.endLex()), u8, 0);
                String replStr = new String(u8, 0, endLex-1, UTF_8);

                textLocal.copy(1, (endLex = text.endLex()), u8, 0);
                String textStr = new String(u8, 0, endLex-1, UTF_8);

                Pattern pattern = this.pattern;
                if (pattern == null)
                    pattern = compile(batch, row);

                sb.setLength(0);
                var m = pattern.matcher(textStr);
                while (m.find())
                    m.appendReplacement(sb, replStr);
                m.appendTail(sb);

                tmpRope.clear().append('"').append(sb).append(textLocal, endLex, textLocal.len);
                result.set(text.shared(), tmpRope, true);
                return result;
            }
        }
    }

    class Substr extends NAryFunction {
        public Substr(Expr... args) { //SUBSTR(source, start[, len])
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

        @Override
        public  ExprEvaluator evaluator(Vars vars) {
            return new SusbtrEval(vars, args);
        }

        @Override public Expr bound(Binding binding) {
            Expr[] b = boundArgs(binding);
            return b == args ? this : new Substr(b);
        }

        private static final class SusbtrEval implements ExprEvaluator {
            private final ExprEvaluator inEval, startEval;
            private final @Nullable ExprEvaluator lenEval;
            private final ByteRope tmp = new ByteRope(48).append('"');
            private final Expr[] args;
            private final Term result = Term.mutable();

            public SusbtrEval(Vars vars, Expr[] args) {
                this.args      = args;
                this.inEval    = args[0].evaluator(vars);
                this.startEval = args[1].evaluator(vars);
                this.lenEval   = args.length > 2 ? args[2].evaluator(vars) : null;
            }

            @Override public Term evaluate(Batch<?> batch, int row) {
                Term in = Expr.requireLiteral(args[0], inEval.evaluate(batch, row));
                Term startTerm = Expr.requireLiteral(args[1], startEval.evaluate(batch, row));
                int end, start = 1 + (int)startTerm.local().parseLong(1);
                SegmentRope local = in.local(), sh = in.shared();
                if (lenEval == null)
                    end = local.len - (sh.len > 0 ? 1 : 0);
                else {
                    Term lenTerm = Expr.requireLiteral(args[2], lenEval.evaluate(batch, row));
                    end = 1 + (int)lenTerm.local().parseLong(1);
                }
                tmp.clear().len = 1;
                tmp.append(local, start, end);
                if (sh.len == 0) tmp.append('"');
                result.set(sh, tmp, true);
                return result;
            }
        }
    }


    class Uuid extends Supplier {
        private static final SegmentRope UUID_IRI_PREF = new ByteRope("<urn:uuid:");

        @Override public Term eval(Binding binding) {
            return Term.wrap(UUID_IRI_PREF, new ByteRope(36+1).append(randomUUID()).append('>'));
        }

        @Override
        public  ExprEvaluator evaluator(Vars vars) {
            return new UuidEval();
        }

        private static final class UuidEval implements ExprEvaluator {
            private final Term result = Term.mutable();
            private final ByteRope tmp = new ByteRope(38);

            @Override public Term evaluate(Batch<?> batch, int row) {
                tmp.clear().append(randomUUID().toString()).append('>');
                result.set(UUID_IRI_PREF, tmp, false);
                return result;
            }
        }
    }

    class Struuid extends Supplier {
        @Override public Term eval(Binding binding) {
            return Term.wrap(new ByteRope().append('"').append(randomUUID()).append('"'), null);
        }

        @Override
        public  ExprEvaluator evaluator(Vars vars) {
            return new StruuidEval();
        }

        private static final class StruuidEval implements ExprEvaluator {
            private final Term result = Term.mutable();
            private final ByteRope tmp = new ByteRope(38).append('"');

            @Override public Term evaluate(Batch<?> batch, int row) {
                tmp.clear().len = 1;
                tmp.append(randomUUID().toString()).append('"');
                result.set(EMPTY, tmp, false);
                return result;
            }
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
                } catch (Throwable t) {
                    if (first == null)
                        first = t instanceof RuntimeException r ? r : new RuntimeException(t);
                }
            }
            if (first != null)
                throw first;
            throw new IllegalArgumentException("All arguments evaluated with unexpected exceptions");
        }

        @Override
        public  ExprEvaluator evaluator(Vars vars) {
            return new CoalesceEval(vars, args);
        }

        @Override public Expr bound(Binding binding) {
            Expr[] b = boundArgs(binding);
            return b == args ? this : new Coalesce(b);
        }

        private static final class CoalesceEval implements ExprEvaluator {
            private final ExprEvaluator[] evals;

            public CoalesceEval(Vars vars, Expr[] args) {
                evals = new ExprEvaluator[args.length];
                for (int i = 0; i < evals.length; i++)
                    evals[i] = args[i].evaluator(vars);
            }

            @Override public Term evaluate(Batch<?> batch, int row) {
                RuntimeException first = null;
                for (var e : evals) {
                    try {
                        return e.evaluate(batch, row);
                    } catch (Throwable t) {
                        if (first == null)
                            first = t instanceof RuntimeException r ? r : new RuntimeException(t);
                    }
                }
                if (first != null)
                    throw first;
                throw new IllegalArgumentException("All arguments evaluated with unexpected exceptions");
            }
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

        @Override
        public  ExprEvaluator evaluator(Vars vars) {
            return new IfEval(vars, args);
        }

        @Override public Expr bound(Binding binding) {
            Expr[] b = boundArgs(binding);
            return b == args ? this : new If(b);
        }

        private static final class IfEval implements ExprEvaluator {
            private final ExprEvaluator cond, onTrue, onFalse;
            public IfEval(Vars vars, Expr[] args) {
                cond    = args[0].evaluator(vars);
                onTrue  = args[1].evaluator(vars);
                onFalse = args[2].evaluator(vars);
            }

            @Override public Term evaluate(Batch<?> batch, int row) {
                Term cond = this.cond.evaluate(batch, row);
                return (cond != null && cond.asBool() ? onTrue : onFalse).evaluate(batch, row);
            }
        }
    }

    class Strlang extends BinaryFunction {
        public Strlang(Expr l, Expr r) { super(l, r); }
        @Override public Term eval(Term l, Term r) {
            Rope lex = requireLexical(this.l, l);
            Rope tag = requireLexical(this.r, r);
            var nt = new ByteRope(lex.len+3+tag.len).append('"').append(lex).append('"')
                                                    .append('@').append(tag);
            return Term.wrap(nt, null);
        }
        @Override public ExprEvaluator evaluator(Vars vars) {return new StrlangEval(vars, l, r);}
        @Override public Expr bound(Binding binding) {
            Expr bl = l.bound(binding), br = r.bound(binding);
            return bl == l && br == r ? this : new Strlang(bl, br);
        }
        private static final class StrlangEval extends Eval {
            public StrlangEval(Vars vars, Expr l, Expr r) {super(vars, l, r);}
            @Override public Term evaluate(Batch<?> batch, int row) {
                Rope lex = requireLexical(lExpr, l.evaluate(batch, row));
                Rope tag = requireLexical(rExpr, r.evaluate(batch, row));
                var nt = new ByteRope(lex.len+3+tag.len).append('"').append(lex).append('"')
                        .append('@').append(tag);
                return Term.wrap(nt, null);
            }
        }
    }

    class Strdt extends BinaryFunction {
        public Strdt(Expr l, Expr r) { super(l, r); }
        @Override public Term eval(Term string, Term dtTerm) {
            boolean isIRI = dtTerm.type() == Type.IRI;
            return Term.valueOf(of(ByteRope.DQ, requireLexical(this.r, string),
                                isIRI ? ByteRope.DT_MID : ByteRope.DT_MID_LT,
                                isIRI ? dtTerm : dtTerm.escapedLexical(),
                                isIRI ? EMPTY : ByteRope.GT));
        }
        @Override public ExprEvaluator evaluator(Vars vars) {return new StrdtEval(vars, l, r);}
        @Override public Expr bound(Binding binding) {
            Expr bl = l.bound(binding), br = r.bound(binding);
            return bl == l && br == r ? this : new Strdt(bl, br);
        }
        private static final class StrdtEval extends Eval {
            public StrdtEval(Vars vars, Expr l, Expr r) {super(vars, l, r);}
            @Override public Term evaluate(Batch<?> batch, int row) {
                Term string = l.evaluate(batch, row);
                Term dtTerm = r.evaluate(batch, row);
                boolean isIRI = dtTerm.type() == Type.IRI;
                return Term.valueOf(of(ByteRope.DQ, requireLexical(rExpr, string),
                        isIRI ? ByteRope.DT_MID : ByteRope.DT_MID_LT,
                        isIRI ? dtTerm : dtTerm.escapedLexical(),
                        isIRI ? EMPTY : ByteRope.GT));
            }
        }
    }

    class SameTerm extends BinaryFunction {
        public SameTerm(Expr l, Expr r) { super(l, r); }
        @Override public Term eval(Term l, Term r) {
            return l.equals(r) ? TRUE : FALSE;
        }
        @Override public ExprEvaluator evaluator(Vars vars) {return new SameTermEval(vars, l, r);}
        @Override public Expr bound(Binding binding) {
            Expr bl = l.bound(binding), br = r.bound(binding);
            return bl == l && br == r ? this : new SameTerm(bl, br);
        }
        private static final class SameTermEval extends Eval {
            public SameTermEval(Vars vars, Expr l, Expr r) {super(vars, l, r);}
            @Override public Term evaluate(Batch<?> batch, int row) {
                return l.evaluate(batch, row).equals(r.evaluate(batch, row)) ? TRUE : FALSE;
            }
        }
    }

    class IsIRI extends UnaryFunction {
        public IsIRI(Expr in) { super(in); }
        @Override public Term eval(Term term) {
            return term != null && term.type() == Type.IRI ? TRUE : FALSE;
        }
        @Override public Expr bound(Binding binding) {
            Expr b = in.bound(binding);
            return b == in ? this : new IsIRI(b);
        }
    }

    class IsBlank extends UnaryFunction {
        public IsBlank(Expr in) { super(in); }
        @Override public Term eval(Term term) {
            return term != null && term.type() == Type.BLANK ? TRUE : FALSE;
        }
        @Override public Expr bound(Binding binding) {
            Expr b = in.bound(binding);
            return b == in ? this : new IsBlank(b);
        }
    }

    class IsLit extends UnaryFunction {
        public IsLit(Expr in) { super(in); }
        @Override public Term eval(Term term) {
            return term != null && term.type() == Type.LIT ? TRUE : FALSE;
        }
        @Override public Expr bound(Binding binding) {
            Expr b = in.bound(binding);
            return b == in ? this : new IsLit(b);
        }
    }

    class IsNumeric extends UnaryFunction {
        public IsNumeric(Expr in) { super(in); }
        @Override public Term eval(Term term) {
            return term != null && term.asNumber() != null ? TRUE : FALSE;
        }
        @Override public Expr bound(Binding binding) {
            Expr b = in.bound(binding);
            return b == in ? this : new IsNumeric(b);
        }
    }
}
