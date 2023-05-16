package com.github.alexishuf.fastersparql.fed;

import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.Rope;
import com.github.alexishuf.fastersparql.operators.plan.*;
import com.github.alexishuf.fastersparql.sparql.SparqlQuery;
import com.github.alexishuf.fastersparql.sparql.binding.ArrayBinding;
import com.github.alexishuf.fastersparql.sparql.binding.Binding;
import com.github.alexishuf.fastersparql.sparql.expr.Expr;
import com.github.alexishuf.fastersparql.sparql.parser.SparqlParser;
import com.github.alexishuf.fastersparql.util.concurrent.CheapThreadLocal;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import static com.github.alexishuf.fastersparql.fed.PatternCardinalityEstimator.DEFAULT;
import static java.lang.Long.MAX_VALUE;
import static java.lang.Long.numberOfTrailingZeros;

final class Optimizer extends CardinalityEstimator {
    private final Map<SparqlClient, CardinalityEstimator> client2estimator = new IdentityHashMap<>();
    private final CheapThreadLocal<State> stateThreadLocal = new CheapThreadLocal<>(State::new);

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

    private final class State {
        private final Vars.Mutable tmpVars = new Vars.Mutable(10);
        private int upFiltersCount = 0;
        private Vars upFilterVars = Vars.EMPTY;
        private final ArrayList<Vars> upFilterVarsSets = new ArrayList<>();
        private final ArrayList<List<Expr>> upFilters = new ArrayList<>();
        private long upFiltersTaken = 0L;
        private long upFiltersTakenByChildren = 0L;
        private final ArrayList<Expr> tmpFilters = new ArrayList<>();
        private ArrayBinding tmpBinding = null;

        private boolean isEmptyState() {
            return tmpFilters.isEmpty()
                    && upFilterVars == Vars.EMPTY
                    && upFiltersCount == 0
                    && upFiltersTaken == 0
                    && upFiltersTakenByChildren == 0
                    && upFilterVarsSets.isEmpty()
                    && upFilters.isEmpty();
        }

        public void setup(Plan plan) {
            assert isEmptyState();
            Vars allVars = plan.allVars();
            if (tmpBinding == null || !tmpBinding.vars.equals(allVars))
                tmpBinding = new ArrayBinding(allVars);
            else
                tmpBinding.clear();
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
                var filters = new ArrayList<>(tmpFilters);
                p = new Modifier(p, null, 0, 0, MAX_VALUE, filters);
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
            tmpVars.addAll(join.left().publicVars());
            long allTaken = 0;

            for (int childIdx = 1, n = join.opCount(); childIdx < n; childIdx++) {
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
                    join.replace(0, childIdx + 1, new Modifier(subJoin, null, 0, 0, MAX_VALUE, new ArrayList<>(tmpFilters)));
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
            List<Expr> filters = m.filters;
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

        /** Estimate cost of {@code plan} and apply discounts for each {@code upFilter} it
         *  can feed. This allows join-reordering to prioritize feeding a filter when cost are
         *  otherwise close. */
        private int faEstimate(Plan plan) {
            int cost = estimate(plan);
            Vars inVars = plan.publicVars();
            if (upFilterVars.isEmpty())
                return cost; // no filters to evaluate
            if (!inVars.intersects(upFilterVars))
                return cost + (cost>>4); //penalty for not contributing any filter var
            int nFilters = 0;
            for (List<Expr> filterList : upFilters) {
                for (Expr e : filterList) {
                    if (inVars.containsAll(e.vars())) nFilters++;
                }
            }
            return cost - Math.min(nFilters * (cost >> 4), cost >> 2);
        }


        /** Similar to {@link State#faEstimate(Plan)} but uses {@code binding} to estimate the
         * cost of {@code plan} and also considers that bound vars in {@code binding} can be used
         * to feed candidate filters in {@code upFilters}. */
        private int faEstimate(Plan plan, ArrayBinding binding) {
            int cost = estimate(plan, binding);
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
            return cost - Math.min(nFilters * (cost >> 4), cost >> 2); // apply discount per filter
        }

        /** Reorder a join with more than 2 operands so that operands are executed by
         *  increasing cost. Like {@link CardinalityEstimator}, cost of the {@code i-th}
         *  operand assumes all join vars are ground as would occur in a bind join. */
        private void reorderNaryJoin(Plan[] ops) {
            for (int i = 0; i < ops.length; i++) {
                // find minIdx >= i with lowest faEstimate
                int minEstimate = faEstimate(ops[i], tmpBinding), minIdx = i;
                for (int j = i+1; j < ops.length; j++) {
                    int estimate = faEstimate(ops[j]);
                    if (estimate < minEstimate) { minEstimate = estimate; minIdx = j; }
                }
                // shift [i, minIdx) to [i+1, minIdx+1) and put best operand at ops[i]
                // while a swap would faster, it would make the reorder non-stable by changing
                // the ordering of operands with same cost estimate.
                Plan best = ops[minIdx];
                for (int j = minIdx; j > i; j--)
                    ops[j] = ops[j-1];
                ops[i] = best;
                // mark all vars produced by best as ground in subsequent estimations
                for (Rope var : best.publicVars()) {
                    int varIdx = tmpBinding.vars.indexOf(var);
                    if (tmpBinding.get(varIdx) == null)
                        tmpBinding.set(varIdx, GROUND);
                }
            }
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

            // push filters if p is a filtering Modifier and upFiltersCount + filters.size() <= 64
            Modifier pushed = p instanceof Modifier m ? m : null;
            if (pushed != null && (pushed.filters.isEmpty() || !pushFilters(pushed)))
                pushed = null;

            boolean childrenCan; // whether our DIRECT children can take filters
            if   (type == Operator.UNION) childrenCan = nonUniformVars(p);
            else                          childrenCan = type != Operator.MODIFIER;
            // store filters taken by left-side siblings
            long upFiltersTakenForParent = upFiltersTakenByChildren;
            upFiltersTakenByChildren = 0L; // by this point our children took no filters (yet)
            var arr = p.operandsArray;
            if (arr == null) {
                Plan o, oo;
                if ((o = p.left ) != null && (oo = optimize(o, childrenCan)) != o) p.left  = oo;
                if ((o = p.right) != null && (oo = optimize(o, childrenCan)) != o) p.right = oo;
            } else {
                for (int i = 0; i < arr.length; i++)
                    arr[i] = optimize(arr[i], childrenCan);
                p.left  = arr[0];
                p.right = arr[1];
            }

            if (pushed != null) {
                popFilters(pushed);
            } else if (type == Operator.JOIN) {
                if (arr == null) {
                    Plan right = p.right;
                    if (faEstimate(right) < faEstimate(p.left())) {
                        p.replace(1, p.left);
                        p.replace(0, right);
                    }
                } else {
                    reorderNaryJoin(p.operandsArray);
                    // try replacements like Join(A, B, C) -> Join(Filter(Join(A, B), F), C)
                    if (upFiltersCount != 0)
                        upFiltersTakenForParent |= takeFiltersOnSubJoins(p);
                }
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
    }

    /**
     * Performs filter-pushing and join-reordering on the plan rooted at {@code plan}.
     *
     * @param plan a {@link Plan} tree
     * @return plan itself, with possibly mutated tree or a {@link Modifier} wrapping a
     *         possibly mutated tree.
     */
    public Plan optimize(Plan plan) {
        State state = stateThreadLocal.get();
        state.setup(plan);
        return state.optimize(plan, true);
    }

    @Override public int estimate(TriplePattern tp, @Nullable Binding binding) {
        return DEFAULT.estimate(tp, binding);
    }

    @Override public int estimate(Query q, @Nullable Binding binding) {
        SparqlQuery sparql = q.sparql;
        Plan plan = sparql instanceof Plan p ? p : new SparqlParser().parse(sparql);
        return client2estimator.getOrDefault(q.client, DEFAULT).estimate(plan, binding);
    }
}
