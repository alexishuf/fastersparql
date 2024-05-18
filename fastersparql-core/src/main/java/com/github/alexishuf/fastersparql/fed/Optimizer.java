package com.github.alexishuf.fastersparql.fed;

import com.github.alexishuf.fastersparql.FS;
import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.operators.plan.*;
import com.github.alexishuf.fastersparql.sparql.SparqlQuery;
import com.github.alexishuf.fastersparql.sparql.binding.ArrayBinding;
import com.github.alexishuf.fastersparql.sparql.binding.Binding;
import com.github.alexishuf.fastersparql.sparql.expr.Expr;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import com.github.alexishuf.fastersparql.sparql.parser.SparqlParser;
import com.github.alexishuf.fastersparql.util.concurrent.Alloc;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.github.alexishuf.fastersparql.fed.PatternCardinalityEstimator.DEFAULT;
import static com.github.alexishuf.fastersparql.operators.plan.Operator.*;
import static com.github.alexishuf.fastersparql.sparql.expr.Term.GROUND;
import static java.lang.Long.*;
import static java.lang.Math.max;
import static java.lang.Math.min;

public class Optimizer extends CardinalityEstimator {
    private static final int MIN_VARS     = 22;
    private static final int INIT_OPS_LEN = 22;
    private static final int INIT_DEPTH   = 10;
    private final IdentityHashMap<SparqlClient, CardinalityEstimator> client2estimator = new IdentityHashMap<>();

    public Optimizer() {
        super(new CompletableFuture<>());
        ready.complete(this);
    }

    public void estimator(SparqlClient client, CardinalityEstimator estimator) {
        client2estimator.put(client, estimator);
    }

    private static boolean nonUniformVars(Plan union) {
        int nVars = union.publicVars().size();
        for (int i = 0, n = union.opCount(); i < n; i++) {
            if (union.op(i).publicVars().size() != nVars)
                return true;
        }
        return false;
    }

    public static List<Expr> splitFilters(List<Expr> filters) {
        int n = filters.size();
        if (n == 0) return filters;
        List<Expr> split = null;
        for (int i = 0; i < n; i++) {
            Expr e = filters.get(i);
            if (e instanceof Expr.And a) {
                if (split == null) {
                    split = new ArrayList<>(min(10, n));
                    for (int j = 0; j < i; j++)
                        split.add(filters.get(j));
                }
                add(split, a.l);
                add(split, a.r);
            } else if (split != null) {
                split.add(e);
            }
        }
        return split == null ? filters : split;
    }

    private static void add(List<Expr> list, Expr e) {
        if (e instanceof Expr.And a) {
            add(list, a.l);
            add(list, a.r);
        } else {
            list.add(e);
        }
    }

    private static final class State implements AutoCloseable {
        private static final int BYTES = 16 + 14*4 + 2*8
                + 2*Vars.BYTES + 3*(16+8) /*ArrayList*/
                + ArrayBinding.BYTES
                + 2*((INIT_DEPTH+1)*20) /*groundedStack, estimatesStack*/
                + 20 /*estimates*/;
        private static final Alloc<State> ALLOC = new Alloc<>(State.class,
                "Optimizer.State.ALLOC",
                Alloc.THREADS*8, State::new, State.BYTES);

        private Optimizer optimizer;
        private final Vars.Mutable tmpVars = new Vars.Mutable(10);
        private int upFiltersCount = 0;
        private Vars upFilterVars = Vars.EMPTY;
        private final ArrayList<Vars> upFilterVarsSets = new ArrayList<>();
        private final ArrayList<List<Expr>> upFilters = new ArrayList<>();
        private long upFiltersTaken = 0L;
        private long upFiltersTakenByChildren = 0L;
        private final ArrayList<Expr> tmpFilters = new ArrayList<>();
        private final ArrayBinding grounded = new ArrayBinding(Vars.EMPTY, new Term[MIN_VARS]);
        private Term[][] groundedStack = new Term[INIT_DEPTH][];
        private int groundedStackSize = 0;
        private int[] estimates = new int[INIT_OPS_LEN];
        private int[][] estimatesStack = new int[INIT_DEPTH][];
        private int estimatesStackSize = 0;
        private boolean inUse;


        private State() {
            for (int i = 0; i < groundedStack.length; i++)
                groundedStack[i] = new Term[MIN_VARS];
            for (int i = 0, n = INIT_DEPTH>>1; i < n; i++)
                estimatesStack[i] = new int[INIT_OPS_LEN];
        }

        @Override public void close() {
            if (inUse) {
                inUse = false;
                ALLOC.offer(this);
            } else {
                throw new IllegalStateException("duplicate/concurrent close()");
            }
        }

        public static State create(Optimizer optimizer, Plan plan) {
            State s = ALLOC.create();
            if (s.inUse)
                throw new IllegalStateException("State object already in-use");
            s.optimizer = optimizer;
            s.inUse = true;
            s.tmpVars.clear();
            s.tmpFilters.clear();
            s.upFilters.clear();
            s.upFiltersCount = 0;
            s.upFiltersTaken = 0;
            s.upFiltersTakenByChildren = 0;
            s.upFilterVars = Vars.EMPTY;
            s.upFilterVarsSets.clear();
            s.grounded.reset(plan.allVars());
            s.groundedStackSize = 0;
            return s;
        }

        private void saveGrounded() {
            if (groundedStackSize == groundedStack.length)
                groundedStack = Arrays.copyOf(groundedStack, groundedStack.length<<1);
            int varsSize = grounded.vars.size();
            Term[] values = groundedStack[groundedStackSize];
            if (values == null || values.length < varsSize)
                groundedStack[groundedStackSize] = values = new Term[max(MIN_VARS, varsSize)];
            groundedStackSize++;
            grounded.copyValuesInto(values);
        }

        private void restoreGrounded() {
            if (groundedStackSize == 0)
                throw new IllegalStateException("Mismatched saveGround/restoreGrounded()");
            Term[] saved = groundedStack[--groundedStackSize];
            groundedStack[groundedStackSize] = grounded.swapValues(saved);
        }

        private void saveEstimatesAndGrounded() {
            if (estimatesStackSize == estimatesStack.length)
                estimatesStack = Arrays.copyOf(estimatesStack, estimatesStack.length<<1);
            int[] ints = estimatesStack[estimatesStackSize];
            if (ints == null || ints.length < estimates.length)
                estimatesStack[estimatesStackSize] = ints = new int[estimates.length];
            estimatesStack[estimatesStackSize++] = estimates;
            estimates = ints;
            saveGrounded();
        }

        private void restoreEstimatesAndGrounded() {
            if (estimatesStackSize == 0)
                throw new IllegalStateException("Mismatched saveEstimates/restoreEstimates()");
            int[] saved = estimatesStack[--estimatesStackSize];
            estimatesStack[estimatesStackSize] = estimates;
            estimates = saved;
            restoreGrounded();
        }

        /**
         * Add all filters satisfied by {@code inVars} not in {@code upFiltersTakenByChildren}
         * into {@code tmpFilters}.
         *
         * @param inVars set of vars that are available to feed the filters.
         * @return bitset where bit {@code i} is set iff the {@code i}-th filter in
         *         {@code upFilters} was added to {@code tmpFilters}
         */
        private long findFilters(Vars inVars) {
            long mask = 1L, taken = 0L, children = upFiltersTakenByChildren;
            for (List<Expr> filterList : upFilters) {
                for (Expr filter : filterList) {
                    if ((children & mask) == 0 && inVars.containsAll(filter.vars())) {
                        tmpFilters.add(filter);
                        taken |= mask;
                    }
                    mask <<= 1;
                }
            }
            return taken;
        }

        /**
         * Add all filters in {@code tmpFilters} to {@code p} or wrap {@code p} in a
         * {@link Modifier} with said filters. {@code tmpFilters} will be cleared before return.
         *
         * @param p plan that will feed the filters in {@code tmpFilters} or a {@link Modifier}
         *          whose input will feed the filters.
         * @return {@code p} or a Modifier wrapping it.
         */
        private Plan addFilters(Plan p) {
            if (p instanceof Modifier mod) {
                List<Expr> filters = mod.filters;
                if (!(filters instanceof ArrayList<Expr>))
                    mod.filters = filters = new ArrayList<>(filters);
                filters.addAll(tmpFilters);
            } else {
                if (p instanceof Query q && q.sparql instanceof Plan sparql) {
                    addFilters(sparql);
                } else {
                    var filters = new ArrayList<>(tmpFilters);
                    p = new Modifier(p, null, null, 0, MAX_VALUE, filters);
                }
            }
            tmpFilters.clear();
            return p;
        }

        /**
         * Tries to apply filters to subsets of consecutive join operands that always start at
         * operand {@code 0} ({@code join.left} and stop growing before operand
         * {@code join.opCount()-1} is included.
         *
         * <p>Once the smallest such subset that can feed at least one filter is found,
         * {@code join=Join(o0, ..., oI, ...)} will become
         * {@code Join(Modifier(Join(o0, ..., oI), filters), oI+1, ...)}. The process is repeated
         * after such a rewrite occurs and stops when no more rewrites could be made or
         * {@code join} has only 2 operands.</p>
         *
         * @return bitset of filters taken by joins of subsets of {@code join} operands.
         */
        private long takeFiltersOnSubJoins(Plan join) {
            assert tmpVars.isEmpty() && tmpFilters.isEmpty();
            tmpVars.addAll(join.left().publicVars());
            long allTaken = 0;

            for (int childIdx = 1, last = join.opCount()-1; childIdx < last; childIdx++) {
                tmpVars.addAll(join.op(childIdx).publicVars());
                long bitset = findFilters(tmpVars);
                if (bitset != 0) {
                    allTaken |= bitset;
                    Join subJoin;
                    if (childIdx == 1) {
                        subJoin = new Join(join.left, join.right);
                    } else {
                        Plan[] ops = new Plan[childIdx + 1];
                        for (int i = 0; i < ops.length; i++)
                            ops[i] = join.op(i);
                        subJoin = new Join(ops);
                    }
                    join.replace(0, childIdx+1, new Modifier(subJoin, null, null, 0, MAX_VALUE, new ArrayList<>(tmpFilters)));
                    last -= childIdx;
                    childIdx = 0;
                    tmpFilters.clear();
                }
            }
            tmpVars.clear();
            return allTaken;
        }

        /** Publishes the filters from {@code m.filters} into {@code upFilter*} fields, allowing
         * descendants of {@code m} to apply them earlier. Every {@code pushFilter(m)} must have
         * a matching {@code popFilters(m)} call were filters taken by descendants will be
         * removed from {@code m.filters} and also from {@code upFilter*} fields */
        private boolean pushFilters(Modifier m) {
            List<Expr> filters = m.filters = splitFilters(m.filters);
            int filtersSize = filters.size();
            if ((upFiltersCount += filtersSize) > 64) {
                upFiltersCount -= filtersSize;
                return false; //no capacity in upFiltersTaken (also, why >64 filters!?)
            }
            upFilters.add(filters);
            tmpVars.addAll(upFilterVars);
            int before = tmpVars.size();
            for (Expr e : filters) tmpVars.addAll(e.vars());
            if (tmpVars.size() > before)
                upFilterVars = Vars.fromSet(tmpVars);
            tmpVars.clear();
            upFilterVarsSets.add(upFilterVars);
            return true;
        }

        /** Reverses a previous {@code pushFilters(m)}. Must be called in the reverse order.*/
        private void popFilters(Modifier m) {
            int depth = upFilters.size()-1;
            List<Expr> mFilters = upFilters.remove(depth);
            int begin = upFiltersCount-=mFilters.size();
            long taken = upFiltersTaken>>>begin;
            if (taken != 0) { // if filters were taken...
                // ensure m.filters is mutable
                if (!(mFilters instanceof ArrayList<Expr>))
                    m.filters = mFilters = new ArrayList<>();
                // remove each of the filters
                for (int i = 0, n = 0; (i+=numberOfTrailingZeros(taken>>>i)) < 64; i++,n++)
                    mFilters.remove(i-n);
                long mask = -1L >>> -begin;
                upFiltersTaken &= mask;
                upFiltersTakenByChildren &= mask;
            }
            upFilterVarsSets.remove(depth);
            upFilterVars = depth == 0 ? Vars.EMPTY : upFilterVarsSets.get(depth-1);
        }

        /**
         * Estimate cost of {@code plan} after binding it with {@code binding} and apply discounts
         * for each {@code upFilter} it can feed. This allows join-reordering to prioritize
         * feeding a filter when cost are otherwise close.
         */
        private int faEstimate(Plan plan, ArrayBinding binding) {
            int cost = optimizer.estimate(plan, binding);
            Vars planVars = plan.publicVars();
            if (upFilterVars.isEmpty())
                return cost; // no filters to test
            if (!planVars.intersects(upFilterVars))
                return cost + (cost>>4); // penalize for not providing any filter var

            //collect all vars that are known after the join with plan
            tmpVars.addAll(planVars);
            Vars bindingVars = binding.vars;
            for (int i = 0, n = bindingVars.size(); i < n; i++) {
                if (binding.get(i) != null) tmpVars.add(bindingVars.get(i));
            }

            int nFilters = 0; // count filters that can be evaluated with now known vars
            for (List<Expr> filterList : upFilters) {
                for (Expr e : filterList) {
                    if (tmpVars.containsAll(e.vars())) nFilters++;
                }
            }
            tmpVars.clear();
            return cost - min(nFilters * (cost >> 4), cost >> 2); // apply discount per filter
        }

        /**
         * Optimizes (filter-pushing and join-reordering) a tree rooted at {@code p}.
         *
         * @param p a {@link Plan}
         * @param canTakeFilters if true, {@code p} may receive additional filters from
         *                       indirect parents or be replaced with a Modifier that runs such
         *                       filters over its output. This should only be false when
         *                       {@code p}'s parent is a {@link Modifier} or a {@link Union}
         *                       with uniform vars across its operands: in such cases the parent
         *                       should absorb applicable filters.
         * @return {@code p}, possibly mutated, or a {@link Modifier} with a possibly mutated
         *         {@code p} as input.
         */
        public Plan optimize(Plan p, boolean canTakeFilters) {
            Operator type = p.type;
            if (type == UNION && (p.right == null || p.right.type == Operator.EMPTY))
                type = (p = p.left()).type;

            // push filters if p is a filtering Modifier and upFiltersCount + filters.size() <= 64
            Modifier pushed = p instanceof Modifier m ? m : null;
            if (pushed != null && (pushed.filters.isEmpty() || !pushFilters(pushed)))
                pushed = null;

            boolean childCTF; // whether our DIRECT children can take filters
            byte saveG; // whether we must save and restore tmpBinding
            switch (type) {
                case NOT_EXISTS,EXISTS,MINUS -> { childCTF = true; saveG = 2; }
                case JOIN  -> { childCTF = true;                   saveG = 0; saveGrounded(); }
                case UNION -> { childCTF = nonUniformVars(p);      saveG = 1; }
                default    -> { childCTF = type != MODIFIER;       saveG = 0; }
            }
            // store filters taken by left-side siblings
            long upFiltersTakenForParent = upFiltersTakenByChildren;
            upFiltersTakenByChildren = 0L; // by this point our children took no filters (yet)
            var arr = p.operandsArray;
            if (arr == null) {
                Plan o = p.left, oo;
                if (o == null) {
                    if (p instanceof Query q && q.sparql instanceof Plan query) {
                        if ((oo = optimize(query, childCTF)) != o)
                            q.sparql = oo;
                    }
                } else {
                    if (saveG == 1) saveGrounded();
                    if ((oo = optimize(o, childCTF)) != o)
                        p.left  = oo;
                    if (saveG == 1) restoreGrounded();
                    if (saveG != 0) saveGrounded();

                    if ((o = p.right) != null && (oo = optimize(o, childCTF)) != o)
                        p.right = oo;
                    if (saveG != 0 || type == JOIN) restoreGrounded();
                    if (type == JOIN) reorderBinary(p);
                }
            } else {
                for (int i = 0; i < arr.length; i++) {
                    if (saveG != 0) saveGrounded();
                    arr[i] = optimize(arr[i], childCTF);
                    if (saveG != 0) restoreGrounded();
                }
                p.left  = arr[0];
                p.right = arr[1];
                if (type == JOIN) {
                    restoreGrounded();
                    upFiltersTakenForParent |= reorderNary(p, arr);
                }
            }

            if (type != Operator.JOIN) {
                grounded.ground(p.publicVars()); // expose vars to subsequent operand
                if (pushed != null)
                    popFilters(pushed);
            } else {
                assert grounded.hasAll(p.publicVars());
            }

            // tries applying filters to p itself
            if (canTakeFilters) {
                long bitset = findFilters((p instanceof Modifier m ? m.left() : p).publicVars());
                if (bitset != 0) {
                    upFiltersTakenForParent |= bitset;
                    p = addFilters(p);
                }
            }
            if (pushed != null && pushed.isNoOp())
                p = p.left();
            upFiltersTaken |= upFiltersTakenForParent;
            upFiltersTakenByChildren  = upFiltersTakenForParent;
            return p;
        }

        void reorderBinary(Plan join) {
            Plan right = join.right();
            if (faEstimate(right, grounded) < faEstimate(join.left(), grounded)) {
                // do not swap if right has input vars fed by left
                boolean safe = true;
                Vars rPub = right.publicVars(), rAll = right.allVars();
                if (rAll.size() > rPub.size()) {
                    Vars lPub = join.left().publicVars();
                    for (var in : rAll) {
                        if (rPub.contains(in)) continue;
                        if (lPub.contains(in)) {
                            safe = false;
                            break;
                        }
                    }
                }
                if (safe) {
                    join.replace(1, join.left);
                    join.replace(0, right);
                }
            }
            grounded.ground(join.publicVars());
        }

        private static final int CURB_ACC_SHIFT     = 10;
        private static final int CURB_ACC_THRESHOLD = Integer.MAX_VALUE>>CURB_ACC_SHIFT;
        /** Reorder a join with more than 2 operands so that operands are executed by
         *  increasing cost. Like {@link CardinalityEstimator}, cost of the {@code i-th}
         *  operand assumes all join vars are ground as would occur in a bind join. */
        private long reorderNary(Plan p, Plan[] ops) {
            int[] estimates = this.estimates;
            if (estimates.length < ops.length)
                this.estimates = estimates = new int[ops.length];
            for (int i = 0; i < ops.length; i++)
                estimates[i] = faEstimate(ops[i], grounded);
            int acc = 1;
            for (int done = 0; done < ops.length; done++) {
                // find minIdx >= done with the lowest cost
                long minEstimate = MAX_VALUE;
                int minIdx = done; // on ties, input order prevails
                if (done == 0) {
                    // for the first operand we must ensure it has no input vars. The general
                    // case code would detect a false product and compute unnecessary stuff
                    minIdx = -1; // we use minIdx sign to signal whether minIdx has input vars
                    for (int i = 0; i < ops.length; i++) {
                        Vars pubVars = ops[i].publicVars(), allVars = ops[i].allVars();
                        boolean hasInputVars = false;
                        if (allVars.size() > pubVars.size()) {
                            for (var in : allVars) {
                                hasInputVars = !pubVars.contains(in) && !grounded.has(in);
                                if (hasInputVars) break;
                            }
                        }
                        if (estimates[i] < minEstimate && (!hasInputVars || minIdx < 0)) {
                            minEstimate = estimates[i];
                            minIdx = hasInputVars ? -(i+1) : i+1;
                        }
                    }
                    minIdx = Math.abs(minIdx)-1; // reverse the "sign as hasInputVars" hack
                } else if (done < ops.length-1) {
                    if (acc > CURB_ACC_THRESHOLD)
                        acc >>= CURB_ACC_SHIFT; // prevent overflow before comparison
                    for (int i = done; i < ops.length; i++) {
                        Vars pubVars = ops[i].publicVars(), allVars = ops[i].allVars();
                        long est;
                        int allVarsSize = allVars.size();
                        if (grounded.hasAll(allVars)) {
                            est = joinWithAsk(acc, allVarsSize);
                        } else {
                            // do not be optimistic about this candidate if another candidate
                            // can assign a **private** var in this one
                            boolean allowBonus = allVarsSize <= pubVars.size()
                                              || !hasInputFedByFuture(ops, done, i);
                            est = estimates[i];
                            est = allowBonus ? join(ops, i, acc, (int)est) : acc*est;
                        }
                        if (est < minEstimate && est > 0) {
                            minEstimate = est;
                            minIdx = i;
                        }
                    }
                }
                // shift [done, minIdx) to [done+1, minIdx+1) and put the best operand at ops[done]
                // while a swap would faster, it would make the reorder non-stable
                Plan best = ops[minIdx];
                for (int j = minIdx; j > done; j--) {
                    estimates[j] = estimates[j-1];
                    ops[j] = ops[j-1];
                }
                ops[done] = best;
                // mark all vars produced by best as ground in subsequent estimations
                Vars bestPubVars = best.publicVars();
                if (grounded.ground(bestPubVars)) { // best introduced new vars
                    for (int i = done+1; i < ops.length; i++) {
                        if (bestPubVars.intersects(ops[i].allVars())) {
                            saveEstimatesAndGrounded();
                            ops[i] = optimize(ops[i], true);
                            restoreEstimatesAndGrounded();
                            this.estimates[i] = faEstimate(ops[i], grounded);
                        }
                    }
                }
                acc = (int)min(I_MAX, minEstimate);
            }
            p.left  = ops[0];
            p.right = ops[1];
            // try replacements like Join(A, B, C) -> Join(Filter(Join(A, B), F), C)
            return upFiltersCount == 0 ? 0 : takeFiltersOnSubJoins(p);
        }

        private boolean hasInputFedByFuture(Plan[] ops, int selCount, int candidateIdx) {
            Plan candidate = ops[candidateIdx];
            Vars pubVars   = candidate.publicVars();
            for (var v : candidate.allVars()) {
                if (pubVars.contains(v) || grounded.has(v)) continue;
                for (int i = selCount; i < ops.length; i++) {
                    if (i == candidateIdx) continue;
                    if (ops[i].publicVars().contains(v)) return true;
                }
            }
            return false;
        }
    }

    /**
     * Recursively performs filter-pushing and join-reordering on the plan rooted at {@code plan}.
     *
     * @param plan a {@link Plan} tree
     * @return plan itself, with possibly mutated tree or a {@link Modifier} wrapping a
     *         possibly mutated tree.
     */
    public Plan optimize(Plan plan, Vars assumeGrounded) {
        Plan out;
        try (var state = State.create(this, plan)) {
            if (!assumeGrounded.isEmpty())
                groundVars(assumeGrounded, state);
            out = state.optimize(plan, true);
        }
        return out;
    }

    /** Equivalent to {@link #shallowOptimize(Plan, Vars)} with {@link Vars#EMPTY}. */
    public Plan shallowOptimize(Plan join) { return shallowOptimize(join, Vars.EMPTY); }

    /**
     * If {@code joinOrMod} is a {@link Join}, reorder operand to minimize the cost of
     * iterating over all results. If {@code joinOrMod} is a {@link Modifier} with filters
     * over a {@link Join}, tries to push filters to operands of the join and reorder the join.
     * Else do nothing.
     *
     * <p>Unlike {@link #optimize(Plan, Vars)}, the operands of the join themselves will not be optimized</p>
     *
     * @param boundVars set of vars to be considered as bound when estimating costs.
     * @param joinOrMod the join or `Modifier(Join)` to optimize
     * @return the new root of the plan, which will either be {@code joinOrMod} or
     *         {@code joinOrMod.left} in case all filters were pushed and the outer
     *         {@link Modifier} became a no-op.
     */
    public Plan shallowOptimize(Plan joinOrMod, Vars boundVars) {
        Modifier mod;
        Plan join;
        if (joinOrMod instanceof Modifier m) {
            join = (mod = m).left();
            m.filters = splitFilters(m.filters); // FILTER(L && R) -> FILTER(L) FILTER(R)
        } else {
            mod = null;
            join = joinOrMod;
        }
        if (join.type != Operator.JOIN)
            return joinOrMod;

        // FILTER(L && R) -> FILTER(L) FILTER(R) on join operands
        for (int i = 0, n = join.opCount(); i < n; ++i) {
            if (join.op(i) instanceof Modifier m && !m.filters.isEmpty()) {
                List<Expr> split = splitFilters(m.filters);
                if (split != m.filters) {
                    Modifier m2 = (Modifier)m.copy();
                    m2.filters = split;
                    join.replace(i, m2);
                }
            }
        }

        // get and init State object
        try (State st = State.create(this, join)) {
            groundVars(boundVars, st);

            // try to push filters on outer modifier
            List<Expr> filters = mod != null && !mod.filters.isEmpty()
                    ? mod.filters = new ArrayList<>(mod.filters) : List.of();
            long taken = 0;
            if (!filters.isEmpty()) {
                for (int i = 0, n = join.opCount(); i < n; i++) {
                    st.tmpVars.addAll(boundVars);
                    st.tmpVars.addAll(join.op(i).publicVars());
                    List<Expr> opFilters = null;
                    for (int j = 0; j < filters.size(); j++) {
                        Expr e = filters.get(j);
                        if (st.tmpVars.containsAll(e.vars())) {
                            taken |= 1L << j;
                            (opFilters == null ? opFilters = new ArrayList<>() : opFilters).add(e);
                        }
                    }
                    if (opFilters != null)
                        join.replace(i, FS.filter(join.op(i), opFilters));
                    st.tmpVars.clear();
                }
                // removed pushed filters from outer modifier
                for (int i; (i = 63 - numberOfLeadingZeros(taken)) >= 0; taken &= ~(1L << i))
                    filters.remove(i);
                if (mod.isNoOp()) // remove outer modifier if it became a no-op
                    joinOrMod = join;
            }
            // reorder join operands by cost. will not push filters to subsets of operands
            Plan[] arr = join.operandsArray;
            if (arr == null) st.reorderBinary(join);
            else st.reorderNary(join, arr);
        }
        return joinOrMod;
    }

    private static void groundVars(Vars boundVars, State st) {
        var grounded = st.grounded;
        for (var name : boundVars) {
            int i = grounded.vars.indexOf(name);
            if (i >= 0)
                grounded.set(i, GROUND);
        }
    }

    @Override public int estimate(TriplePattern tp, @Nullable Binding binding) {
        return DEFAULT.estimate(tp, binding);
    }

    @Override public int estimate(Query q, @Nullable Binding binding) {
        SparqlQuery sparql = q.sparql;
        Plan plan = sparql instanceof Plan p ? p : SparqlParser.parse(sparql);
        return client2estimator.getOrDefault(q.client, DEFAULT).estimate(plan, binding);
    }
}
