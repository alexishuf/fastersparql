package com.github.alexishuf.fastersparql.util;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.*;

import static com.github.alexishuf.fastersparql.util.concurrent.ArrayAlloc.intsAtLeast;
import static com.github.alexishuf.fastersparql.util.concurrent.ArrayAlloc.recycleIntsAndGetEmpty;
import static java.lang.System.arraycopy;

public class IntList extends AbstractList<Integer> {
    private @NonNull Node head, tail;
    private int size;
    private final int chunkSize;

    private final static class Node {
        int[] data;
        int size;
        @Nullable Node prev, next;

        Node(int capacity) { this.data = intsAtLeast(Math.max(16, capacity)); }
    }

    public IntList(int initialCapacity) { head = tail = new Node(chunkSize = initialCapacity); }

    private Node grow() { return linkAfter(tail, chunkSize); }

    private Node linkAfter(Node prev, int capacity) {
        Node next = prev.next;
        if (prev == head && size == 0 && prev.data.length == 0) {
            // instead of adding a new node, only replace head.data
            head.data = intsAtLeast(capacity);
            return head;
        }
        Node node = new Node(capacity);
        prev.next = node;
        node.prev = prev;
        if (next != null) {
            node.next = next;
            next.prev = node;
        }
        if (prev == tail)
            tail = node;
        return node;
    }

    private Node unlinkAndGetNext(Node node) {
        Node next = node.next, prev = node.prev;
        node.data = recycleIntsAndGetEmpty(node.data);
        node.size = 0;
        node.prev = null;
        node.next = null;
        if (node == head) {
            assert prev == null : "node.prev != null, but node is this.head";
            if (next == null)
                return head;
            next.prev = null;
            head = next;
        } else {
            Objects.requireNonNull(prev).next = next;
            if (next == null) {
                assert node == tail : "node.next == null, but node is not this.tail";
                tail = next = prev;
            } else {
                next.prev = prev;
            }
        }
        return next;
    }

    @Override public Integer get(int index) {
        int relative = index - (size-tail.size);
        if (relative > 0)
            return tail.data[relative];
        var n = head;
        for (relative = index; n != null && relative >= n.size; n = n.next)
            relative -= n.size;
        if (n != null)
            return n.data[relative];
        throw new IndexOutOfBoundsException(index);
    }

    @Override public int indexOf(Object o) {
        Number number = isInt(o);
        if (number == null)
            return -1; // not an integer
        int integer = number.intValue(), absolute = 0;
        for (var node = head; node != null; node = node.next) {
            int[] data = node.data;
            for (int i = 0, nSize = node.size; i < nSize; i++, ++absolute) {
                if (data[i] == integer) return absolute;
            }
        }
        return -1; // not found
    }

    @Override public boolean contains(Object o) {
        return indexOf(o) >= 0;
    }

    @Override public int size() { return size; }

    private enum Move {
        NEXT, PREVIOUS
    }

    @Override public @NonNull It     iterator() {return new It(head, -1, -1);}
    @Override public @NonNull It listIterator() {return new It(head, -1, -1);}

    @Override public @NonNull It listIterator(int index) {
        if (index < 0 || index > size)
            throw new IndexOutOfBoundsException(index);
        Node node = head;
        int relative = index;
        for (; relative > node.size; node = node.next) {
            relative -= node.size;
            if (node.next == null)
                break;
        }
        return new It(node, relative-1, index-1);
    }

    public final class It implements ListIterator<Integer> {
        private @NonNull Node node;
        private int relative, absolute;
        private @Nullable Move lastMove;

        private It(@NonNull Node node, int relative, int absolute) {
            this.node     = node;
            this.relative = relative;
            this.absolute = absolute;
        }

        @Override public boolean hasNext() {
            return relative+1 < node.size || (node.next != null && node.size > 0);
        }

        @Override public Integer next() { return nextInt(); }
        public int nextInt() {
            if (!moveRight())
                throw new NoSuchElementException();
            lastMove = Move.NEXT;
            return node.data[relative];
        }

        private boolean moveRight() {
            ++absolute;
            int next = relative+1;
            if (next >= node.size) {
                if (node.next == null)
                    return false;
                node = node.next;
                next = 0;
            }
            relative = next;
            return true;
        }

        private void moveLeft() {
            --absolute;
            if (--relative < 0 && node.prev != null)
                relative = (node = node.prev).size-1;
        }

        @Override public boolean hasPrevious() {
            return relative >= 0 || node.prev != null;
        }

        @Override public Integer previous() { return previousInt(); }
        public Integer previousInt() {
            if (relative < 0) throw new NoSuchElementException();
            int value = node.data[relative];
            moveLeft();
            return value;
        }

        @Override public int     nextIndex() {return absolute;}
        @Override public int previousIndex() {return absolute-1;}

        @Override public void remove() {
            if (lastMove == null)
                throw new IllegalStateException("no previous next()/previous()");
            int next = relative+1, tailSize = node.size-next;
            if (tailSize > 0)
                arraycopy(node.data, next, node.data, relative, tailSize);
            --size;
            if (--node.size == 0) {
                node = unlinkAndGetNext(node);
                relative = 0;
            }
            if (lastMove == Move.NEXT)
                moveLeft();
            else
                moveRight();
            lastMove = null;
        }

        @Override public void set(Integer integer) { set(integer.intValue()); }
        public void set(int integer) {
            node.data[relative] = integer;
        }

        @Override public void add(Integer integer) { add(integer.intValue()); }
        public void add(int integer) {
            ++absolute;
            if (++relative >= node.data.length) {
                relative = 0;
                node = node.next == null ? grow() : node.next;
            }
            int orphans = (node.data.length - node.size - 1)>>>31;
            int orphanInt = node.data[node.size-1];
            int tailSize = node.size-relative-orphans;
            if (tailSize > 0)
                arraycopy(node.data, relative, node.data, relative+1, tailSize);
            node.data[relative] = integer;
            ++size;
            if (orphans == 0) {
                ++node.size;
            } else {
                Node next = node.next;
                if (next != null && next.size < next.data.length)
                    arraycopy(next.data, 0, next.data, 1, next.size);
                else
                    next = linkAfter(node, node.data.length);
                next.size++;
                next.data[0] = orphanInt;
            }
        }
    }

    @Override public boolean add(Integer integer) { return add(integer.intValue()); }
    public boolean add(int integer) {
        var tail = this.tail;
        if (tail.size >= tail.data.length)
            tail = grow();
        tail.data[tail.size++] = integer;
        ++size;
        return true;
    }

    @Override public Integer set(int index, Integer e) { return set(index, e.intValue()); }
    public int set(int index, int e) {
        int relative = index;
        var n = head;
        for (; n != null && relative >= n.size; n = n.next)
            relative -= n.size;
        if (n == null)
            throw new IndexOutOfBoundsException(index);
        int old = n.data[relative];
        n.data[relative] = e;
        return old;
    }

    @Override public void add(int index, Integer e) {add(index, e.intValue());}
    public void add(int index, int e) {
        int relative = index - (size-tail.size);
        Node n = tail;
        if (relative < 0) {
            relative = index;
            for (n = head; n != null && relative >= n.size; n = n.next)
                relative -= n.size;
            if (n == null)
                throw new IllegalStateException("concurrent reads/writes to IntQueue");
        }
        if (relative > n.size)
            throw new IndexOutOfBoundsException(index);
        int tailSize = n.size-relative;
        if (n.size >= n.data.length) {
            Node next = linkAfter(n, n.data.length);
            if (tailSize-- > 0)
                next.data[next.size++] = n.data[--n.size];
            else
                relative = (n = next).size;
        }
        if (tailSize > 0)
            arraycopy(n.data, relative, n.data, relative+1, tailSize);
        n.data[relative] = e;
        ++n.size;
        ++size;
    }

    @Override public void clear() {
        size = 0;
        tail = head;
        for (Node node = head, next; node != null; node = next) {
            next = node.next;
            node.data = recycleIntsAndGetEmpty(node.data);
            node.size = 0;
            node.prev = null;
            node.next = null;
        }
    }

    @Override public Integer remove(int index) {
        if (index < 0 || index >= size)
            throw new IndexOutOfBoundsException(index);
        int relative = index - (size-tail.size);
        Node n;
        if (relative > 0) {
            n = tail;
        } else {
            relative = index;
            for (n = head; n != null && relative > n.size; n = n.next)
                relative -= n.size;
            if (n == null)
                throw new IllegalStateException("concurrent reads/writes on IntQueue");
        }
        int old = n.data[relative], tailSize = n.size-(relative+1);
        if (tailSize > 0)
            arraycopy(n.data, n.size-tailSize, n.data, relative, tailSize);
        --n.size;
        --size;
        return old;
    }

    @Override public Integer removeFirst() { return removeFirstInt(); }

    public int removeFirstInt() {
        if (head.size == 0 || size == 0)
            throw new NoSuchElementException();
        int value = head.data[0];
        --size;
        if (--head.size == 0)
            unlinkAndGetNext(head);
        return value;
    }

    @Override public Integer removeLast() {return removeLastInt();}
    public int removeLastInt() {
        if (tail.size == 0 || size == 0)
            throw new NoSuchElementException();
        --size;
        int value = tail.data[--tail.size];
        if (tail.size == 0)
            unlinkAndGetNext(tail);
        return value;
    }

    public boolean removeFirst(int value) {
        for (Node node = head; node != null; node = node.next) {
            int[] data = node.data;
            for (int i = 0, nSize = node.size; i < nSize; i++) {
                if (data[i] == value) {
                    arraycopy(data, i+1, data, i, nSize-(i+1));
                    --size;
                    if (--node.size == 0)
                        unlinkAndGetNext(node);
                    return true;
                }
            }
        }
        return false;
    }

    public boolean removeAll(int value) {
        boolean modified = false;
        for (Node prev = head, node = head; node != null; node = (prev=node).next) {
            int[] data = node.data;
            for (int i = node.size-1; i >= 0; --i) {
                if (data[i] != value)
                    continue;
                modified = true;
                int tailSize = node.size - (i+1);
                if (tailSize > 0)
                    arraycopy(data, i+1, data, i, tailSize);
                --size;
                if (--node.size == 0) { // node became empty, remove it from linked list
                    unlinkAndGetNext(node);
                    node = prev; // compensate for node = (prev=node).next
                }
            }
        }
        return modified;
    }

    private @Nullable Number isInt(Object o) {
        if (o instanceof Number num) {
            if (num instanceof Integer || num instanceof Byte || num instanceof Short)
                return num;
            else if (num instanceof Long l && l >= Integer.MIN_VALUE && l <= Integer.MAX_VALUE)
                return num;
        }
        return null;
    }

    @Override public boolean removeAll(@NonNull Collection<?> c) {
        boolean modified = false;
        for (Object o : c) {
            var n = isInt(o);
            if (n != null)
                modified |= removeFirst(n.intValue());
        }
        return modified;
    }

    public void addFirst(int integer) { add(0, integer); }
    public void addLast(int integer) { add(integer); }

    @SuppressWarnings("unused") void addAll(IntList other) {
        var dstNode = this.tail;
        for (var srcNode = other.head; srcNode != null; srcNode = srcNode.next) {
            int fitsInDst = Math.min(dstNode.data.length-dstNode.size, srcNode.size);
            int excess = srcNode.size - fitsInDst;
            if (fitsInDst > 0) {
                arraycopy(srcNode.data, 0, dstNode.data, dstNode.size, fitsInDst);
                dstNode.size += fitsInDst;
            }
            if (excess > 0) {
                dstNode = linkAfter(dstNode, Math.max(excess, dstNode.data.length));
                arraycopy(srcNode.data, fitsInDst, dstNode.data, dstNode.size, excess);
                dstNode.size += excess;
            }
            size += srcNode.size;
        }
    }

    @SuppressWarnings("unused") void addAll(int[] other, int begin, int end) {
        for (int i = begin; i < end; i++) add(other[i]);
    }

    @SuppressWarnings("unused") void addAll(int[] other) {
        for (int i : other) add(i);
    }

    @Override public String toString() {
        var sb = new StringBuilder().append('[');
        for (var node = head; node != null; node = node.next) {
            int[] data = node.data;
            for (int i = 0, nSize = node.size; i < nSize; i++)
                sb.append(data[i]).append(", ");
        }
        if (size > 0)
            sb.setLength(sb.length()-2);
        return sb.append(']').toString();
    }

    @Override public boolean equals(Object o) {
        if (o == this) {
            return true;
        } else if (o instanceof IntList list) {
            if (list.size != size)
                return false;
            It l = iterator(), r = list.iterator();
            while (l.hasNext()) {
                if (l.nextInt() != r.nextInt()) return false;
            }
        } else  if (o instanceof Collection<?> coll) {
            if (coll.size() != size)
                return false;
            var it = coll.iterator();
            for (var node = head; node != null; node = node.next) {
                int[] data = node.data;
                for (int i = 0, nSize = node.size; i < nSize; i++) {
                    Number num = isInt(it.next());
                    if (num == null || num.intValue() != data[i])
                        return false;
                }
            }
        } else {
            return false;
        }
        return true;
    }

    @Override public int hashCode() {
        int h = 0;
        for (var n = head; n != null; n = n.next) {
            int[] data = n.data;
            for (int i = 0, nSize = n.size; i < nSize; i++)
                h = 31*h + data[i];
        }
        return h;
    }
}
