package com.github.alexishuf.fastersparql.batch.dedup;

import com.github.alexishuf.fastersparql.batch.type.*;
import com.github.alexishuf.fastersparql.model.rope.FinalSegmentRope;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import com.github.alexishuf.fastersparql.sparql.expr.TermView;
import com.github.alexishuf.fastersparql.util.ThrowingConsumer;
import com.github.alexishuf.fastersparql.util.concurrent.Alloc;
import com.github.alexishuf.fastersparql.util.concurrent.Primer;
import com.github.alexishuf.fastersparql.util.owned.AbstractOwned;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import com.github.alexishuf.fastersparql.util.owned.Owned;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;

import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Arrays;
import java.util.HashSet;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static com.github.alexishuf.fastersparql.model.rope.FinalSegmentRope.EMPTY;
import static com.github.alexishuf.fastersparql.util.owned.SpecialOwner.RECYCLED;
import static java.lang.System.arraycopy;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElse;

public class BTreeDedup<B extends Batch<B>> extends Dedup<B, BTreeDedup<B>> {
    static boolean EXPENSIVE_VALIDATION = Dedup.DEBUG;

    private static final int MAX_COLS = 32;
    private static final int EXPECTED_COLS = 8;
    private static final int EXPECTED_MAX_COLS = 16;
    static { assert Integer.bitCount(MAX_COLS) == 1 : "MAX_COLS not a power of 2"; }
    private static final int INVALID_COLS_MASK = -MAX_COLS;
    private static final int NODE_KEYS = 8;
    private static final int NODE_SPLIT_KEYS = NODE_KEYS/2;
    static {//noinspection ConstantValue
        assert (NODE_KEYS&1) == 0 : "Keys not a pair number";
    }
    private static final int NODE_MAX_CHILDREN = NODE_KEYS+1;
    private static final int NODE_TERMS = MAX_COLS*NODE_KEYS;
    private static final int NODE_TERMS_2 = NODE_TERMS*2;
    static {//noinspection ConstantValue
        assert NODE_TERMS > NODE_MAX_CHILDREN : "NODE_TERMS must be > NODE_MAX_CHILDREN";
    }
    private static final long[] EMPTY_IDS = new long[0];
    private static final int NODE_IDS_LEN = EXPECTED_MAX_COLS*NODE_KEYS;
    private static final int NODE_DATA_LEN = 2048;
    static {//noinspection ConstantValue
        assert NODE_DATA_LEN >= NODE_KEYS*EXPECTED_MAX_COLS*16 : "NODE_DATA_LEN may be too short";
    }

    private static final int NODE_POOL_CAPACITY;
    private static final Alloc<Node> NODE_ALLOC;
    static {
        int bytesBudget    = (int)(Runtime.getRuntime().maxMemory()*0.1);
        int sharedCapacity = bytesBudget/Node.BYTES - Alloc.THREADS*(128/4);
        NODE_ALLOC = new Alloc<>(Node.class, "BTreeDedup.NODE_ALLOC",
                sharedCapacity, Node.FAC, Node.BYTES);
        NODE_POOL_CAPACITY = NODE_ALLOC.sharedCapacity() + NODE_ALLOC.allThreadsLocalCapacity();
        assert NODE_POOL_CAPACITY > 1 : "Excessively small Node pool size";
        Primer.INSTANCE.schedOnce(() -> NODE_ALLOC.prime(10, 0));
    }

    private static final class NodeSharedData {
        private static final int MAX_TENANTS;
        static {
            int safeTerms = 1<<14; // safeTerms*2 (slices) == Short.MAX_VALUE+1
            MAX_TENANTS = safeTerms/NODE_TERMS_2;
            //noinspection ConstantValue
            assert MAX_TENANTS > 8 : "too few tenants, review and reconsider sharing arrays";
        }
        final             short[] slices   = new short           [MAX_TENANTS*NODE_TERMS_2];
        final  FinalSegmentRope[] shared   = new FinalSegmentRope[MAX_TENANTS*NODE_TERMS];
        final              Node[] children = new Node            [MAX_TENANTS*NODE_MAX_CHILDREN];
        private int tenants;
        NodeSharedData addTenant() { return ++tenants < MAX_TENANTS ? this : new NodeSharedData(); }
        short   slicesBegin() { return (short)(tenants*NODE_TERMS_2); }
        short   sharedBegin() { return (short)(tenants*NODE_TERMS); }
        short childrenBegin() { return (short)(tenants*NODE_MAX_CHILDREN); }
    }

    private static sealed abstract class Node extends AbstractOwned<Node> {
        private static final int BYTES = 16/* headers */
                +4*6 /* fields */
                +16+NODE_DATA_LEN /* data contents */
                +(NODE_KEYS+1)*4 /* children contents */
                +NODE_KEYS*4 /*slices contents*/;
        private static final class Fac implements Supplier<Node> {
            private static final VarHandle LOCK;
            static {
                try {
                    LOCK = MethodHandles.lookup().findVarHandle(Fac.class, "lockPlain", int.class);
                } catch (NoSuchFieldException|IllegalAccessException e) {
                    throw new ExceptionInInitializerError(e);
                }
            }
            @SuppressWarnings("unused") private int lockPlain;
            private int nodesSpawned;
            private NodeSharedData shared = new NodeSharedData();
            @Override public Node get() {
                Node node = null;
                byte[] data = new byte[NODE_DATA_LEN];
                while ((int)LOCK.compareAndExchangeAcquire(this, 0, 1) != 0) Thread.onSpinWait();
                if (nodesSpawned < NODE_POOL_CAPACITY) {
                    node = new Node.Concrete(true,
                                             shared.slices,   shared.slicesBegin(),
                                             shared.shared,   shared.sharedBegin(),
                                             shared.children, shared.childrenBegin(),
                                             data);
                    shared = shared.addTenant();
                    nodesSpawned++;
                }
                LOCK.setRelease(this, 0);
                if (node == null) {
                    node = new Node.Concrete(false,
                                             new short[NODE_KEYS*2],           (short)0,
                                             new FinalSegmentRope[NODE_TERMS], (short)0,
                                             new Node[NODE_KEYS+1],            (short)0,
                                             data);
                }
                node.takeOwnership0(RECYCLED);
                return node;
            }

            @Override public String toString() {return "BTreeDedup.Node.FAC";}
        }
        private static final Fac FAC      = new Fac();
        private static final int SL_OFF   = 0;
        private static final int SL_LEN   = 1;
        private static final int LEN_MASK = 0x7fff;
        private static final int LIT_MASK = 0x8000;
        private static final int LIT_MASK_MAGIC0 = Term.maskIfLitMagicCookie(15);
        private static final int LIT_MASK_MAGIC1 = LIT_MASK_MAGIC0+1;
        private static int litMask(Term t) {
            return t.maskIfLit(LIT_MASK_MAGIC0, LIT_MASK_MAGIC1);
        }
        static { assert litMask(Term.EMPTY_STRING) == LIT_MASK; }

        private final short childBegin, slicesBegin, sharedBegin;
        private short dataUsed;
        private byte keysCount;
        private byte cols;
        private final short[] slices;
        private final FinalSegmentRope[] shared;
        private byte[] data;
        private final Node[] child;
        private boolean copyStr;
        private boolean copyId;
        private boolean visited;
        private final boolean pooled;
        private long[] ids;
        private MemorySegment dataSegment;
        private final MemorySegment ownedData;
        private long[] ownedIds;
        private @Nullable IdBatchType<?> idType;

        private Node(boolean pooled, short[] slices, short slicesBegin,
                     FinalSegmentRope[] shared, short sharedBegin,
                     Node[] child, short childBegin, byte[] data) {
            this.pooled      = pooled;
            this.slices      = slices;
            this.slicesBegin = slicesBegin;
            this.shared      = shared;
            this.sharedBegin = sharedBegin;
            this.child       = child;
            this.childBegin  = childBegin;
            this.ids         = EMPTY_IDS;
            this.ownedIds    = EMPTY_IDS;
            this.data        = data;
            this.dataSegment = MemorySegment.ofArray(data);
            this.ownedData   = dataSegment;
        }

        public static Node acquireChild(Node parent) {
            return acquire(parent.cols, parent.idType,
                           parent.copyId, parent.copyStr).takeOwnership(parent);
        }

        private static Orphan<Node> acquire(int cols, IdBatchType<?> idType,
                                            boolean copyId, boolean copyStr) {
            var node = NODE_ALLOC.create();
            if (((cols-1)&INVALID_COLS_MASK) != 0)
                throw new IllegalArgumentException("cols < 0 || cols > BTreeDedup.MAX_COLS");
            if (node.keysCount != 0 || node.dataSegment != node.ownedData)
                throw new IllegalStateException("acquire()d non-clean node (double free?)");
            node.cols    = (byte) cols;
            node.copyId  = copyId;
            node.copyStr = copyStr;
            node.idType = idType;
            if (copyId)
                node.initIds(cols);
            if (DEBUG) node.validate(-1);
            return node.releaseOwnership(RECYCLED);
        }

        private void initIds(int cols) {
            int terms = Math.max(cols*NODE_KEYS, NODE_IDS_LEN);
            if (ids.length < terms) {
                ids = new long[terms];
                if (terms == NODE_IDS_LEN)
                    ownedIds = this.ids;
            }
        }

        @Override public @Nullable Node recycle(Object currentOwner) {
            reset(1, false);
            if (pooled) {
                internalMarkRecycled(currentOwner);
                if (NODE_ALLOC.offer(this) == null)
                    return null;
                assert false : "unreachable code";
                currentOwner = RECYCLED; // should never happen
            }
            return internalMarkGarbage(currentOwner);
        }

        private static final class Concrete extends Node implements Orphan<Node> {
            public Concrete(boolean pooled, short[] slices, short slicesBegin,
                            FinalSegmentRope[] shared, short sharedBegin, Node[] child,
                            short childBegin, byte[] data) {
                super(pooled, slices, slicesBegin, shared, sharedBegin, child, childBegin, data);
            }
            @Override public Node takeOwnership(Object o) {return takeOwnership0(o);}
        }

        private void reset(int cols, boolean useIds) {
            if (((cols-1)&INVALID_COLS_MASK) != 0)
                throw new IllegalArgumentException("columns does not fit in a byte");
            int oldTerms = keysCount*this.cols;
            Arrays.fill(shared, sharedBegin, sharedBegin+oldTerms, null);
            Arrays.fill(slices, slicesBegin, slicesBegin+(oldTerms<<1), (short)0);
            if (this.copyId)
                Arrays.fill(ids, 0, oldTerms, 0L);
            for (int i = childBegin, end = childBegin+keysCount+1; i < end; i++)
                child[i] = Owned.safeRecycle(child[i], this);
            if (DEBUG)
                checkExtraneousKeysAndChildren();
            if (this.dataSegment != ownedData)
                this.data = (byte[])(this.dataSegment = ownedData).heapBase().orElseThrow();
            this.dataUsed  = 0;
            this.keysCount = 0;
            this.cols      = (byte)cols;
            this.copyId    = useIds;
            if (useIds)
                initIds(cols);
            else
                ids = ownedIds;
        }


        private void validate(int splitMedian) {
            if (EXPENSIVE_VALIDATION) {
                if (keysCount == 0) {
                    checkExtraneousKeysAndChildren();
                } else {
                    // validate RDF terms
                    var view0 = View.acquire(this, 0).takeOwnership(this);
                    var view1 = View.acquire(this, 0).takeOwnership(this);
                    try {
                        validate(view0, view1, splitMedian);
                    } finally {
                        view0.recycle(this);
                        view1.recycle(this);
                    }
                }
            } else {
                validateCheap();
            }
        }
        private void validateCheap() {
            if (visited)
                throw new AssertionError("Cycle detected");
            visited = true;
            for (int i = 0; i <= keysCount; i++) {
                Node c = child[childBegin+i];
                if (c != null) {
                    c.requireOwner(this);
                    if (c.keysCount <= 0)
                        throw new AssertionError("Empty child node at index "+i);
                    if (c.cols != cols)
                        throw new AssertionError("Bad cols for child at index "+i);
                    c.validateCheap();
                }
            }
            visited = false;
        }
        private void validate(View v0, View v1, int splitMedian) {
            validateCheap();
            if (splitMedian >= 0) { // validate only the median key and its 2 children
                // keys other than the median are in the median children, causing the node
                // keys to be unordered
                v0.wrap(this, splitMedian);
                Node left = child[childBegin+splitMedian], right = child[childBegin+splitMedian+1];
                if (left == null)
                    throw new AssertionError("Missing left split node for median="+splitMedian);
                if (right == null)
                    throw new AssertionError("Missing right split node for median="+splitMedian);
                if (v0.compareTo(cols, left.rightmost(v1)) <= 0)
                    throw new AssertionError("left node maximum >= median at "+splitMedian);
                if (v0.compareTo(cols, right.leftmost(v1)) >= 0)
                    throw new AssertionError("right node minimum <= median at "+splitMedian);
            } else {
                // this node was not split by add(), enforce keys are sorted
                for (int k = 0; k < keysCount; k++) {
                    v0.wrap(this, k); // will throw if invalid terms
                    if (copyId && copyStr) {
                        for (int c = 0, base=k*cols; c < cols; c++) {
                            int t = base + c;
                            if (!v0.nodeView(this, t).equals(v1.nodeViewId(this, t)))
                                throw new AssertionError("id != string at k="+k+", c="+c);
                        }
                    }
                    if (k > 0 && v0.compareTo(this, k-1) <= 0)
                        throw new AssertionError("key at index "+k+" <= than key at "+(k-1));
                    Node left = child[childBegin+k], right = child[childBegin+k+1];
                    if (left != null && v0.compareTo(cols, left.rightmost(v1)) <= 0)
                        throw new AssertionError("left node maximum >= key at index"+k);
                    if (right != null && v0.compareTo(cols, right.leftmost(v1)) >= 0)
                        throw new AssertionError("right node minimum <= key at index"+k);
                }
            }
            checkExtraneousKeysAndChildren();
        }
        private View leftmost(View out) {
            if (child[childBegin] != null)
                return child[childBegin].leftmost(out);
            return out.wrap(this, 0);
        }
        private View rightmost(View out) {
            if (child[childBegin+keysCount] != null)
                return child[childBegin+keysCount].rightmost(out);
            return out.wrap(this, keysCount-1);
        }

        private void checkExtraneousKeysAndChildren() {
            int oldTerms = keysCount*cols;
            int maxTerms = NODE_KEYS*cols;
            if (copyId) {
                for (int i = oldTerms; i < maxTerms; i++) {
                    if (ids[i] != 0)
                        throw new AssertionError("extraneous non-zero ID");
                }
            }
            if (copyStr) {
                for (int i = oldTerms; i < maxTerms; i++) {
                    if (shared[sharedBegin+i] != null)
                        throw new AssertionError("extraneous non-null shared");
                }
                for (int i = oldTerms*2+SL_LEN, end = maxTerms*2; i < end; i += 2) {
                    if (slices[slicesBegin+i] != 0)
                        throw new AssertionError("extraneous non-zero slice");
                }
            }
            for (int i = childBegin+keysCount+1; i < NODE_KEYS; i++)
                if (child[i] != null)
                    throw new AssertionError("extraneous non-null child");
        }

        private byte[] growData(int required) {
            if (required > Short.MAX_VALUE)
                throw new UnsupportedOperationException("A Node can only keep 32KiB of string data");
            var d       = Arrays.copyOf(data, required + (-required&0x7));
            data        = d;
            dataSegment = MemorySegment.ofArray(d);
            return d;
        }

        @SuppressWarnings("unused") // used for debug purposes
        String dump() {return dump(new StringBuilder(), 0, new HashSet<>()).toString();}

        private String hexId() { return Integer.toHexString(System.identityHashCode(this)); }

        StringBuilder dump(StringBuilder out, int indent, HashSet<Node> visited) {
            if (!visited.add(this))
                return out.append("Cycle on Node@").append(hexId()).append('\n');
            indent(out, indent).append("Node@").append(hexId());
            if (keysCount == 0) {
                out.append("[EMPTY]\n");
            } else {
                out.append('\n');
                int indent2 = indent+2;
                for (int k = 0; k < keysCount; k++) {
                    if (child[childBegin+k] != null)
                        child[childBegin+k].dump(out, indent2, visited);
                    indent(out, indent2).append(k).append(": [");
                    if (copyStr) {
                        int shb = sharedBegin+k*cols, slb = slicesBegin+k*cols*2;
                        for (int c = 0, sli; c < cols; c++) {
                            short fLen = slices[(sli=slb+c*2)+SL_LEN];
                            var sh = requireNonNullElse(shared[shb+c], EMPTY);
                            if ((fLen&LIT_MASK) == 0) out.append(sh);
                            out.append(new String(data, slices[sli+SL_OFF],
                                                  fLen&LEN_MASK, UTF_8));
                            if ((fLen&LIT_MASK) != 0) out.append(sh);
                            out.append(", ");
                        }
                    } else {
                        assert copyId;
                        for (int c = 0, idb = k*cols; c < cols; c++)
                            out.append(ids[idb+c]).append(", ");
                    }
                    if (cols != 0)
                        out.setLength(out.length()-2);
                    out.append("]\n");
                }
                if (child[childBegin+keysCount] != null)
                    child[childBegin+keysCount].dump(out, indent2, visited);
            }
            return out;
        }

        private static final String[] INDENTS =
                IntStream.range(0, 20).mapToObj(" "::repeat).toArray(String[]::new);
        private static StringBuilder indent(StringBuilder out, int spaces) {
            return out.append(spaces < INDENTS.length ? INDENTS[spaces] : " ".repeat(spaces));
        }

        public <B extends Batch<B>, E extends Throwable>
        void forEachIds(IdBatch<?> b, ThrowingConsumer<B, E> consumer) throws E {
            for (int key = 0; key < keysCount; key++) {
                Node child = this.child[childBegin+key];
                if (key != 0) {
                    if (!b.hasCapacity((b.rows+1)*cols, 0)) {
                        //noinspection unchecked
                        consumer.accept((B)b);
                        b.clear();
                    }
                    b.beginPut();
                    for (int c = 0, ti = key*cols; c < cols; c++)
                        b.putTerm(c, ids[ti+c]);
                    b.commitPut();
                }
                if (child != null) child.forEachIds(b, consumer);
            }
        }

        public <B extends Batch<B>, E extends Throwable>
        void forEachStrings(B b, ThrowingConsumer<B, E> consumer) throws E {
            int rowLenBase = slicesBegin + keysCount*cols;
            for (int key = 0; key < keysCount; key++) {
                Node child = this.child[childBegin+key];
                if (key != 0) {
                    int localBytes = b.localBytesUsed();
                    if (localBytes > 0)
                        localBytes += slices[rowLenBase+key];
                    if (!b.hasCapacity((b.rows+1)*cols, localBytes)) {
                        consumer.accept(b);
                        b.clear();
                    }
                    b.beginPut();
                    short tb  = (short)(sharedBegin + key*cols);
                    short slb = (short)(slicesBegin + key*(cols<<1));
                    for (int c = 0; c < cols; c++) {
                        var sh   = shared[tb+c];
                        var fLen = slices[slb+SL_LEN];
                        b.putTermLocalByReference(c, sh, dataSegment, data,
                                slices[slb+SL_OFF], fLen&LEN_MASK,
                                SharedKind.make(sh!=null, (fLen&LIT_MASK)!=0));
                    }
                    b.commitPut();
                }
                if (child != null) child.forEachStrings(b, consumer);
            }
        }

        public int add(View view) {
            int lo = 0, ret;
            for (int hi = keysCount-1, mid; lo <= hi; ) {
                int diff = view.compareTo(this,  mid=(byte)((lo+hi)>>>1));
                if      (diff < 0) hi = (byte)(mid-1); // view <  this[mid]
                else if (diff > 0) lo = (byte)(mid+1); // view >  this[mid]
                else               return ADD_FOUND;   // view == this[mid]
            }
            var c = child[childBegin+lo];
            if (c == null) {
                if (keysCount < NODE_KEYS) {
                    shiftRight(lo);
                    setKey(view, lo);
                    keysCount++;
                    ret = ADD_NOT_SPLIT;
                } else { // must split node
                    Node left  = split(Side.LEFT,  view, lo);
                    Node right = split(Side.RIGHT, view, lo);
                    int median = NODE_SPLIT_KEYS-((lo-NODE_SPLIT_KEYS)>>>31);
                    if (lo == NODE_SPLIT_KEYS)  // add view to parent
                        setKey(view, median);
                    if (DEBUG)
                        requireNullChildren(median, 2);
                    child[childBegin+median  ] = left;
                    child[childBegin+median+1] = right;
                    ret = median; // parent will copy this[median] and recycle this
                }
            } else {
                int cMedian = c.add(view);
                if (cMedian < 0)
                    return cMedian; // view found or added without splitting c
                // view was added to c. c[Median], c.child[cMedian] and c.child[cMedian+1]
                // must be added to this
                child[childBegin+lo] = null;
                if (keysCount < NODE_KEYS) { // insert c[cMedian] at this[lo]
                    shiftRight(lo);
                    takeSplitNodesAndKey(c, cMedian, lo);
                    keysCount++;
                    ret = ADD_NOT_SPLIT;
                } else { // split this node, adding c[cMedian]
                    Node left  = split(Side.LEFT,  c, cMedian, lo);
                    Node right = split(Side.RIGHT, c, cMedian, lo);
                    ret = NODE_SPLIT_KEYS-((lo-NODE_SPLIT_KEYS)>>>31);
                    if (lo == NODE_SPLIT_KEYS) {// c[cMedian] goes to parent
                        left .takeChild(c, cMedian, left.keysCount);
                        right.takeChild(c, cMedian+1, 0);
                        setKey(c, cMedian, ret);
                    }
                    if (DEBUG)
                        requireNullChildren(ret, 2);
                    child[childBegin+ret  ] = left;
                    child[childBegin+ret+1] = right;
                }
                Owned.safeRecycle(c, this);
            }
            if (DEBUG) validate(ret);
            return ret;
        }
        private static final int ADD_FOUND = -1;
        private static final int ADD_NOT_SPLIT = -2;


        private void shiftRight(int keyPos) {
            int n = keysCount-keyPos;
            if (n <= 0)
                return;
            byte cols = this.cols;
            int termSrc = keyPos*cols, termDst = termSrc+cols;
            arraycopy(child, childBegin+keyPos,
                      child, childBegin+keyPos+1, n+1);
            n *= cols;
            if (copyId)
                arraycopy(ids, termSrc, ids, termDst, n);
            if (copyStr) {
                arraycopy(shared, sharedBegin+termSrc,
                          shared, sharedBegin+termDst, n);
                arraycopy(slices, slicesBegin+(termSrc<<1),
                          slices, slicesBegin+(termDst<<1), n <<1);
            }
        }

        private void takeChild(Node src, int srcPos, int dstPos) {
            int srcIdx = src.childBegin+srcPos;
            Node c = src.child[srcIdx];
            if (DEBUG)
                requireNullChildren(dstPos, 1);
            child[childBegin+dstPos] = c;
            if (c != null) {
                c.transferOwnership(src, this);
                src.child[srcIdx] = null;
            }
        }
        private void takeChildren(Node src, int srcPos, int dstPos, int n) {
            int srcIdx = src.childBegin+srcPos;
            if (DEBUG)
                requireNullChildren(dstPos, n);
            arraycopy(src.child, srcIdx, child, childBegin+dstPos, n);
            for (int i = 0; i < n; i++) {
                Node c = src.child[srcIdx+i];
                if (c != null) {
                    c.transferOwnership(src, this);
                    src.child[srcIdx+i] = null;
                }
            }
        }
        private void requireNullChildren(int beginPos, int n) {
            for (int i = childBegin+beginPos, end = i+n; i < end; i++) {
                if (child[i] != null)
                    throw new AssertionError("Overwriting child node");
            }
        }

        private void takeSplitNodesAndKey(Node src, int srcKey, int dstKey) {
            int dstIdx = childBegin+dstKey, srcIdx = src.childBegin+srcKey;
            child[dstIdx  ] = src.child[srcIdx  ].transferOwnership(src, this);
            child[dstIdx+1] = src.child[srcIdx+1].transferOwnership(src, this);
            src.child[srcIdx  ] = null;
            src.child[srcIdx+1] = null;
            setKey(src, srcKey, dstKey);
        }

        private Node split(Side side, Node srcNode, int srcKey, int insPos) {
            boolean copyStr = this.copyStr, copyId = this.copyId;
            byte cols = this.cols, beginKey = side.beginKey(insPos);
            byte keysBfrIns = Side.keysBeforeInsert(insPos, beginKey);
            byte keysAftIns = Side.keysAfterInsert(keysBfrIns);
            var node = Node.acquireChild(this);
            // copy children
            node.takeChildren(this, beginKey, 0,
                              keysBfrIns+((NODE_SPLIT_KEYS-1-keysBfrIns)>>>31));
            if (keysBfrIns != NODE_SPLIT_KEYS) {
                node.takeChild(srcNode, srcKey, keysBfrIns);
                node.takeChild(srcNode, srcKey+1, keysBfrIns+1);
                node.takeChildren(this, beginKey+keysBfrIns+1,
                                  keysBfrIns+2, keysAftIns);
            }
            if (copyId) {
                int termsBfrIns = keysBfrIns*cols;
                int beginTerm = beginKey*cols;
                arraycopy(ids, beginTerm, node.ids, 0, termsBfrIns);
                if (keysBfrIns != NODE_SPLIT_KEYS) {
                    arraycopy(srcNode.ids, srcKey*cols, node.ids, termsBfrIns, cols);
                    arraycopy(ids, beginTerm+termsBfrIns, node.ids,
                              termsBfrIns + cols, keysAftIns*cols);
                }
            }
            if (copyStr) {
                side.reserveData(node, this, insPos);
                for (int i = 0; i < keysBfrIns; i++)
                    node.setKeyStr(this, beginKey+i, i);
                if (keysBfrIns != NODE_SPLIT_KEYS) {
                    node.setKeyStr(srcNode, srcKey, keysBfrIns);
                    for (int i = keysBfrIns+1; i < NODE_SPLIT_KEYS; i++)
                        node.setKeyStr(this, beginKey+i-1, i);
                }
            }
            node.keysCount = NODE_SPLIT_KEYS;
            if (DEBUG) node.validate(-1);
            return node;
        }
        private Node split(Side side, View view, int insPos) {
            boolean copyStr = this.copyStr, copyId = this.copyId;
            byte cols       = this.cols;
            byte beginKey   = side.beginKey(insPos);
            byte keysBfrIns = Side.keysBeforeInsert(insPos, beginKey);
            byte keysAftIns = Side.keysAfterInsert(keysBfrIns);
            int beginTerm   = beginKey*cols;
            int termsBfrIns = cols*keysBfrIns;
            var node        = Node.acquireChild(this);
            // copy children
            node.takeChildren(this, beginKey, 0, keysBfrIns);
            node.takeChildren(this, beginKey+keysBfrIns,
                              keysBfrIns+1, keysAftIns+1);
            if (copyId) {
                arraycopy(ids, beginTerm, node.ids, 0, termsBfrIns);
                if (keysBfrIns != NODE_SPLIT_KEYS) {
                    arraycopy(view.ids, 0, node.ids, termsBfrIns, cols);
                    arraycopy(ids, beginTerm+termsBfrIns, node.ids,
                              termsBfrIns+cols, cols*keysAftIns);
                }
            }
            if (copyStr) {
                side.reserveData(node, this, insPos);
                for (int i = 0; i < keysBfrIns; i++)
                    node.setKeyStr(this, beginKey+i, i);
                if (keysBfrIns != NODE_SPLIT_KEYS) {
                    node.setKeyStr(view, keysBfrIns);
                    for (int i = keysBfrIns+1; i < NODE_SPLIT_KEYS; i++)
                        node.setKeyStr(this, beginKey+i-1, i);
                }
            }
            node.keysCount = NODE_SPLIT_KEYS;
            if (DEBUG) node.validate(-1);
            return node;
        }

        private void setKey(View view, int dstKey) {
            if (copyId)
                arraycopy(view.ids, 0, ids, dstKey*cols, cols);
            if (copyStr)
                setKeyStr(view, dstKey);
        }
        private void setKeyStr(View view, int dstKey) {
            byte cols      = this.cols;
            short termBase = (short)(dstKey*cols);
            short shb      = (short)(sharedBegin+termBase);
            short slb      = (short)(slicesBegin+termBase*2);
            short lOut     = dataUsed;
            byte[] data    = this.data;
            for (int c = 0; c < cols; c++) {
                var term = view.term[c];
                shared[shb+c] = term.finalShared();
                var local = term.local();
                int lLen = local.len;
                if (lOut+lLen > this.data.length)
                    data = growData(lOut+lLen);
                local.copy(0, lLen, data, lOut);
                int slOut = slb+(c<<1);
                slices[slOut+SL_OFF] = lOut;
                slices[slOut+SL_LEN] = (short)(lLen|litMask(term));
                lOut += (short)lLen;
            }
            dataUsed = lOut;
        }
        private void setKey(Node src, int srcKey, int dstKey) {
            if (copyId)
                arraycopy(src.ids, srcKey*cols, ids, dstKey*cols, cols);
            if (copyStr)
                setKeyStr(src, srcKey, dstKey);
        }
        private void setKeyStr(Node src, int srcKey, int dstKey) {
            byte cols    = this.cols;
            int   cols2  = (byte)(cols<<1);
            short slb    = (short)(src.slicesBegin + srcKey*cols2);
            short dstSlb = (short)(slicesBegin + dstKey*cols2);
            short dBegin = src.slices[slb+SL_OFF];
            short dLen   = (short)(src.slices[slb+cols2-2+SL_OFF]
                               +  (src.slices[slb+cols2-2+SL_LEN]&LEN_MASK)
                               -   dBegin);
            short dataUsed = this.dataUsed;
            short adj    = (short)(dataUsed - dBegin);
            byte[] data = this.data;
            if (dataUsed+dLen > data.length)
                data = growData(this.dataUsed +dLen);
            arraycopy(src.data, dBegin, data, dataUsed, dLen);
            this.dataUsed += dLen;
            arraycopy(src.shared, src.sharedBegin+srcKey*cols, shared,
                      sharedBegin+dstKey*cols, cols);
            for (int i = 0; i < cols2; i+=2) {
                slices[dstSlb+i+SL_OFF] = (short)(src.slices[slb+i+SL_OFF] + adj);
                slices[dstSlb+i+SL_LEN] =         src.slices[slb+i+SL_LEN];
            }
        }

        private enum Side {
            LEFT, RIGHT;
            public byte beginKey(int insPos) {
                if (this == LEFT)
                    return 0;
                // insPos <= NODE_SPLIT_KEYS ---> +0: median key will move to right child
                // insPos >  NODE_SPLIT_KEYS ---> +1: median key will move to parent
                return (byte)(NODE_SPLIT_KEYS+((NODE_SPLIT_KEYS-insPos)>>>31));
            }
            public static byte keysBeforeInsert(int insPos, byte beginKey) {
                // 0 1 2 3 4 5 6 7  | Special case: the key to be inserted
                //         beginKey | will be placed causing the current
                //         insPos   | median to be copied into the right split
                //                  | node. Although insPos >= beginKey, there
                //                  | is no insertion. Return 4
                // ===========================================================
                // 0 1 2 3 4 5 6 7    | The key to be inserted will be the
                //           beginKey | second key in the right split node.
                //             insPos | Return 1
                // ===========================================================
                // 0 1 2 3 4 5 6 7    | The key to be inserted will be the
                //           beginKey | first key in the right split node.
                //           insPos   | Return 0
                // ===========================================================
                // 0 1 2 3 4 5 6 7    | The key to be inserted will NOT be in
                //         beginKey   | the right split node.
                //       insPos       | Return 4
                // ===========================================================
                int d = insPos-beginKey;
                if ((d == 0 && insPos == NODE_SPLIT_KEYS) || (d&NODE_SPLIT_KEYS_NOT_MASK) != 0)
                    return NODE_SPLIT_KEYS; // old median goes to right, inserted to parent
                return (byte)d;
            }
            private static final int NODE_SPLIT_KEYS_NOT_MASK = -NODE_SPLIT_KEYS;
            public static byte keysAfterInsert(int keysBeforeInsert) {
                int n = NODE_SPLIT_KEYS - keysBeforeInsert - 1;
                return (byte)(n + (n>>>31)); // n is in [-1, NODE_SPLIT_KEYS)
            }
            public void reserveData(Node dst, Node src, int insPos) {
                byte cols             = src.cols;
                byte beginKey         = beginKey(insPos);
                byte keysBeforeInsert = keysBeforeInsert(insPos, beginKey);
                byte keysAfterInsert  = keysAfterInsert(keysBeforeInsert);
                int required = 0, cols2 = cols*2;
                int i = src.slicesBegin + beginKey*cols2 + SL_LEN;
                for (int end = i + keysBeforeInsert*cols2; i < end; i += 2)
                    required += src.slices[i]&LEN_MASK;
                if (keysAfterInsert > 0) {
                    for (int end = (i+=cols2) + keysAfterInsert*cols2; i < end; i += 2)
                        required += src.slices[i]&LEN_MASK;
                    required += required/cols;
                }
                if (required > dst.data.length)
                    dst.growData(required);
            }
        }

        static {
            Side L = Side.LEFT, R = Side.RIGHT;
            int median = NODE_SPLIT_KEYS, bfrMed = median-1, aftMed = median+1;
            int first = 0, second = 1, last = NODE_KEYS-1, end = NODE_KEYS;

            assert L.beginKey(first)  == first;
            assert L.beginKey(second) == first;
            assert L.beginKey(bfrMed) == first;
            assert L.beginKey(median) == first;
            assert L.beginKey(aftMed) == first;
            assert L.beginKey(end)    == first;

            assert R.beginKey(first)  == median;
            assert R.beginKey(bfrMed) == median;
            assert R.beginKey(median) == median; // median goes to right, view to parent
            assert R.beginKey(aftMed) == aftMed; // median goes to parent
            assert R.beginKey(last)   == aftMed;
            assert R.beginKey(end)    == aftMed;

            assert Side.keysBeforeInsert(first,  L.beginKey(first))  == 0;
            assert Side.keysBeforeInsert(second, L.beginKey(second)) == 1;
            assert Side.keysBeforeInsert(bfrMed, L.beginKey(bfrMed)) == NODE_SPLIT_KEYS-1;
            assert Side.keysBeforeInsert(median, L.beginKey(median)) == NODE_SPLIT_KEYS;
            assert Side.keysBeforeInsert(end,    L.beginKey(end))    == NODE_SPLIT_KEYS;

            assert Side.keysBeforeInsert(first,  R.beginKey(first))  == NODE_SPLIT_KEYS;
            assert Side.keysBeforeInsert(bfrMed, R.beginKey(bfrMed)) == NODE_SPLIT_KEYS;
            assert Side.keysBeforeInsert(median, R.beginKey(median)) == NODE_SPLIT_KEYS;
            assert Side.keysBeforeInsert(aftMed, R.beginKey(aftMed)) == 0;
            assert Side.keysBeforeInsert(last,   R.beginKey(last))   == NODE_SPLIT_KEYS-2;
            assert Side.keysBeforeInsert(end,    R.beginKey(end))    == NODE_SPLIT_KEYS-1;

            for (int i = 0; i < NODE_KEYS; i++) {
                for (Side side : Side.values()) {
                    byte kbi = Side.keysBeforeInsert(i, side.beginKey(i));
                    byte kai = Side.keysAfterInsert(kbi);
                    byte ins = side == L && i < median || side == R && i > median ? (byte)1 : 0;
                    assert kbi + ins + kai == NODE_SPLIT_KEYS;
                }
            }
        }

        public int find(View view) {
            int lo = 0, hi = (byte)(keysCount-1), mid;
            while (lo <= hi) {
                int diff = view.compareTo(this, mid=(byte)((lo+hi)>>>1));
                if      (diff < 0) hi = (byte)(mid-1); // view <  this[mid]
                else if (diff > 0) lo = (byte)(mid+1); // view >  this[mid]
                else               return mid;         // view == this[mid]
            }
            var c = child[childBegin+lo];
            return lo | (c == null ? NOT_FOUND : c.find(view)&NOT_FOUND);
        }
        public static final int NOT_FOUND = 0x80000000;
    }

    private static abstract sealed class View extends AbstractOwned<View> {
        private static final int BYTES = 16 + 16+MAX_COLS*(4+TermView.BYTES) + 16+MAX_COLS*8;
        private static final class Fac implements Supplier<View> {
            @Override public View get() {return new View.Concrete().takeOwnership(RECYCLED);}
            @Override public String toString() {return "BTreeDedup.Row.FAC";}
        }
        private static final Fac FAC = new Fac();
        private static final Alloc<View> ALLOC = new Alloc<>(View.class,
                "BTreeDedup.Row.ALLOC", Alloc.THREADS*(128/4), FAC, BYTES);

        public final TermView[] term;
        public final long[] ids;
        public final TermView nodeView;
        public final TermView nodeView2;
        private byte debugCols;

        private View() {
            term = new TermView[MAX_COLS];
            for (int i = 0; i < EXPECTED_COLS; i++)
                term[i] = new TermView().wrap(Term.EMPTY_STRING);
            ids = new long[MAX_COLS];
            nodeView = new TermView().wrap(Term.EMPTY_STRING);
            nodeView2 = new TermView().wrap(Term.EMPTY_STRING);
        }
        private static final class Concrete extends View implements Orphan<View> {
            @Override public View takeOwnership(Object o) {return takeOwnership0(o);}
        }
        public static Orphan<View> acquire(Batch<?> b, int r) {
            int cols = b.cols;
            View view = ALLOC.create().init(cols);
            if (b instanceof IdBatch<?> ib)
                ib.copyIds(r, view.ids, 0);
            for (int c = 0; c < cols; c++) {
                TermView v = view.term[c];
                if (!b.getView(r, c, v))
                    v.wrap(Term.EMPTY_STRING);
            }
            return view.releaseOwnership(RECYCLED);
        }
        private @This View init(int cols) {
            if (DEBUG)
                debugCols = (byte)cols;
            for (int c = EXPECTED_COLS; c < cols; c++) {
                var v = term[c];
                if (v == null)
                    term[c] = new TermView().wrap(Term.EMPTY_STRING);
            }
            return this;
        }
        public static Orphan<View> acquire(Node node, int key) {
            return ALLOC.create().init(node.cols).wrap(node, key).releaseOwnership(RECYCLED);
        }
        public @This View wrap(Node node, int key) {
            byte cols = node.cols;
            if (DEBUG)
                debugCols = cols;
            if (node.copyId)
                arraycopy(node.ids, key*cols, this.ids, 0, cols);
            if (node.copyStr) {
                short shb   = (short)(node.sharedBegin+(key*cols));
                short slb   = (short)(node.slicesBegin+(key*cols)*2);
                for (int c = 0; c < cols; c++) {
                    int   sli = slb + c*2;
                    short len = node.slices[sli+Node.SL_LEN];
                    TermView termView = term[c];
                    if (len == 0) {
                        termView.wrap(Term.EMPTY_STRING);
                    } else {
                        termView.wrap(node.shared[shb+c], node.dataSegment, node.data,
                                node.slices[sli+Node.SL_OFF], len&Node.LEN_MASK,
                                (len&Node.LIT_MASK) != 0);
                    }
                }
            } else {
                var ibt = requireNonNull(node.idType);
                int base = key*cols;
                for (int c = 0; c < cols; c++) {
                    var termView = term[c];
                    if (!ibt.view(node.ids[base+c], termView))
                        termView.wrap(Term.EMPTY_STRING);
                }
            }
            return this;
        }

        @Override public @Nullable View recycle(Object currentOwner) {
            internalMarkRecycled(currentOwner);
            if (ALLOC.offer(this) != null)
                internalMarkGarbage(RECYCLED);
            return null;
        }

        @Override public String toString() {
            if (!DEBUG)
                return super.toString();
            var sb = new StringBuilder().append('[');
            for (int c = 0; c < debugCols; c++)
                term[c].appendTo(c == 0 ? sb : sb.append(", "));
            return sb.append(']').toString();
        }

        public int compareTo(Node node, int key) {
            int cols = node.cols, tb = key*cols;
            for (int c = 0; c < cols; c++) {
                int diff = term[c].compareTo(nodeView(node, tb+c));
                if (diff != 0)
                    return diff;
            }
            return 0;
        }

        public int compareTo(int cols, @NonNull View rhs) {
            for (int c = 0; c < cols; c++) {
                int diff = term[c].compareTo(rhs.term[c]);
                if (diff != 0)
                    return diff;
            }
            return 0;
        }

        public TermView nodeViewId(Node node, int term) {
            if (!requireNonNull(node.idType).view(node.ids[term], nodeView))
                nodeView.wrap(Term.EMPTY_STRING);
            return nodeView;
        }

        public TermView nodeView(Node node, int term) {
            if (node.copyStr) {
                int sli = node.slicesBegin + term*2;
                short len = node.slices[sli+Node.SL_LEN];
                if ((len&Node.LEN_MASK) == 0) {
                    nodeView.wrap(Term.EMPTY_STRING);
                } else {
                    nodeView.wrap(node.shared[node.sharedBegin+term], node.dataSegment, node.data,
                                  node.slices[sli+Node.SL_OFF], len&Node.LEN_MASK,
                                  (len&Node.LIT_MASK) != 0);
                }
            } else {
                if (!requireNonNull(node.idType).view(node.ids[term], nodeView))
                    nodeView.wrap(Term.EMPTY_STRING);
            }
            return nodeView;
        }
    }

    private Node root;
    private int size;
    private final int maxSize;

    private BTreeDedup(BatchType<B> batchType, int cols, int maxSize) {
        super(batchType, cols);
        var ibt = batchType instanceof IdBatchType<?> i ? i : null;
        boolean copyId  = ibt != null;
        boolean copyStr = ibt == null || ibt.isId2StrSlow();
        this.root = Node.acquire(cols, ibt, copyId, copyStr).takeOwnership(this);
        this.maxSize = maxSize;
    }
    public static <B extends Batch<B>> Orphan<BTreeDedup<B>> create(BatchType<B> type, int cols) {
        return new Concrete<>(type, cols, Integer.MAX_VALUE);
    }
    public static <B extends Batch<B>> Orphan<BTreeDedup<B>>
    create(BatchType<B> type, int cols, int maxSize) {
        return new Concrete<>(type, cols, maxSize);
    }
    private static final class Concrete<B extends Batch<B>> extends BTreeDedup<B>
            implements Orphan<BTreeDedup<B>>{
        private Concrete(BatchType<B> batchType, int cols, int maxSize) {
            super(batchType, cols, maxSize);
        }
        @Override public BTreeDedup<B> takeOwnership(Object o) {return takeOwnership0(o);}
    }

    @Override public @Nullable BTreeDedup<B> recycle(Object currentOwner) {
        internalMarkGarbage(currentOwner);
        root = Owned.safeRecycle(root, this);
        return null;
    }

    @Override public int capacity() {return Integer.MAX_VALUE;}

    @Override public void clear(int cols) {
        requireAlive();
        root.reset(cols, root.copyId);
    }

    @Override public boolean isWeak() { return false; }

    @Override public boolean isDuplicate(B batch, int row, int ignoredSource) {
        if (batch.cols != root.cols)
            throw new IllegalArgumentException("cols mismatch");
        View view = View.acquire(batch, row).takeOwnership(this);
        Node root = this.root;
        int cols = root.cols;
        IdBatchType<?> idType = root.idType;
        boolean copyId = root.copyId;
        boolean copyStr = root.copyStr;
        lock();
        try {
            if (size >= maxSize)
                return (root.find(view)&Node.NOT_FOUND) == 0;
            int median = root.add(view);
            if (median == Node.ADD_FOUND)
                return true;
            ++size;
            if (median >= 0) { // root was split
                Node parent = Node.acquire(cols, idType, copyId, copyStr).takeOwnership(this);
                parent.takeSplitNodesAndKey(root, median, 0);
                parent.keysCount = 1;
                this.root = parent;
                Owned.safeRecycle(root, this);
                if (DEBUG) parent.validate(-1);
            }
            return false;
        } finally {
            unlock();
            view.recycle(this);
        }
    }

    @Override public boolean contains(B batch, int row) {
        if (batch.cols != root.cols)
            throw new IllegalArgumentException("cols mismatch");
        var root = this.root;
        var view = View.acquire(batch, row).takeOwnership(this);
        lock();
        try {
            return (root.find(view)&Node.NOT_FOUND) == 0;
        } finally {
            unlock();
            view.recycle(this);
        }
    }

    @Override public <E extends Throwable> void forEach(ThrowingConsumer<B, E> consumer) throws E {
        B b = batchType().create(cols).takeOwnership(this);
        try {
            if (root.copyId)
                root.forEachIds((IdBatch<?>)b, consumer);
            else
                root.forEachStrings(b, consumer);
        } finally {
            b.recycle(this);
        }
    }

}
