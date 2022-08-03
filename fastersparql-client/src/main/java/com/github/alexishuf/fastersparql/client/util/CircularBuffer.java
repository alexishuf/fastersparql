package com.github.alexishuf.fastersparql.client.util;

public class CircularBuffer<T> {
    private Object[] buffer;
    private int first, last = -1, size;

    public CircularBuffer() {
        this.buffer = new Object[10];
    }

    public CircularBuffer(int capacity) {
        this.buffer = new Object[Math.max(10, capacity)];
    }

    /** Equivalent to {@code size() == 0}. */
    public boolean isEmpty() { return size == 0; }

    /** Get the number of items in this buffer, not be confused with its inner capacity  */
    public int     size()    { return size; }

    /** Remove all elements from the buffer, i.e., making {@code size() == 0} */
    public void    clear()   { removeFirst(size()); }

    /** Add {@code item} to the end of the buffer, i.e. {@code get(size()-1) == item} */
    public void add(T item) {
        if (size == buffer.length) {
            int current = buffer.length;
            // ArrayList grows in these steps: 10 -> 15 -> 22
            // for small buffers, advance along those steps, else grow by 50%, limited to maxGrowth
            int newCapacity;
            if (current <= 16) {
                newCapacity = current <= 10 ? 15 : 22;
            } else {
                // grow at most 2^17 (131_072) but stay below max array size of MAX_VALUE-8
                int maxGrowth = Math.min(0x20000, Integer.MAX_VALUE - 8 - current);
                if (maxGrowth <= 0) {
                    String msg = "Array of " + current +" reached 32-bit size limit";
                    throw new IllegalStateException(msg);
                }
                // grow by 50% limited by maxGrowth
                newCapacity = current + Math.min(maxGrowth, current>>2);
            }
            Object[] copy = new Object[newCapacity];
            System.arraycopy(buffer, first, copy, 0, current - first);
            System.arraycopy(buffer, 0, copy, current - first, first);
            buffer = copy;
            first = 0;
            last = size-1;
        }
        last = (last + 1) % buffer.length;
        ++size;
        buffer[last] = item;
    }

    /** Get the i-th item in this buffer, where {@code 0 <= i < size()}. */
    public T get(int i) {
        if (i < 0 || i >= size)
            throw new IndexOutOfBoundsException("index="+i+", size="+size);
        //noinspection unchecked
        return (T)buffer[(first + i) % buffer.length];
    }

    /**  Remove the first {@code n} elements of this buffer. */
    public void removeFirst(int n) {
        if (n < 0 || n > size)
            throw new IllegalArgumentException("Cannot remove first "+n+" elements out of "+size);
        first = (first + n) % buffer.length;
        size -= n;
    }

    @Override public String toString() {
        StringBuilder b = new StringBuilder().append('[');
        int cap = buffer.length, boundSize = Math.min(1_000, size);
        for (int i = 0; i < boundSize; i++)
            b.append(buffer[(first +i) % cap]).append(", ");
        if (size != boundSize)
            b.append(" ...");
        else if (size > 0)
            b.setLength(b.length()-2);
        return b.append(']').toString();
    }
}
