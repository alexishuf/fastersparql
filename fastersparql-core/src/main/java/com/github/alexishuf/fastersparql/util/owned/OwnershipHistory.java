package com.github.alexishuf.fastersparql.util.owned;

import com.github.alexishuf.fastersparql.FSProperties;
import com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal;
import com.github.alexishuf.fastersparql.util.owned.OwnershipEvent.*;
import org.checkerframework.checker.nullness.qual.Nullable;

import static com.github.alexishuf.fastersparql.FSProperties.ownedTraceMax;
import static com.github.alexishuf.fastersparql.util.owned.SpecialOwner.GARBAGE;
import static com.github.alexishuf.fastersparql.util.owned.SpecialOwner.RECYCLED;
import static java.lang.Integer.numberOfLeadingZeros;

public class OwnershipHistory {
    private static final boolean TRACE        = FSProperties.ownedTrace();
    private static final boolean STACK_TRACE  = FSProperties.ownedStackTrace();
    public  static final boolean ENABLED      = TRACE || STACK_TRACE;
    private static final int     CAPACITY     = 1<<(32-numberOfLeadingZeros(ownedTraceMax()-1));
    private static final int     MASK         = CAPACITY-1;
    static {
        assert Integer.bitCount(CAPACITY) == 1;
        assert CAPACITY < Short.MAX_VALUE;
    }

    private @Nullable Object lastOwner;
    private final @Nullable Object[] owners = new Object[CAPACITY];
    private short head, size;
    private @Nullable OwnershipEvent lastEvent;

    public static @Nullable OwnershipHistory createIfEnabled() {
        return ENABLED ? new OwnershipHistory() : null;
    }

    private void owner(@Nullable Object owner) {
        lastOwner = owner;
        short head = this.head;
        // store journalName() instead of Owned<?> to avoid blocking collection of owner
        owners[(head+size)&MASK] = owner instanceof Owned<?> o ? o.journalName() : owner;
        if (size == CAPACITY) this.head = (short)((this.head+1)&MASK);
        else                  ++size;
    }

    public @Nullable Object lastOwner    () { return                 lastOwner ; }
    public @Nullable Object lastRootOwner() { return Owned.rootOwner(lastOwner); }

    public OwnershipEvent lastEvent() { return lastEvent; }

    public void taken(Owned<?> owned, Object newOwner) {
        if (STACK_TRACE)
            lastEvent = new TakenOwnershipEvent(owned, newOwner, lastEvent);
        owner(newOwner);
    }
    public void released(Owned<?> owned) {
        if (STACK_TRACE)
            lastEvent = new ReleasedOwnershipEvent(owned, lastOwner(), lastEvent);
        owner(null);
    }
    public void recycled(Owned<?> owned) {
        if (STACK_TRACE)
            lastEvent = new RecycledEvent(owned, lastOwner(), lastEvent);
        owner(RECYCLED);
    }
    public void garbage(Owned<?> owned) {
        if (STACK_TRACE)
            lastEvent = new GarbageEvent(owned, lastOwner(), lastEvent);
        owner(GARBAGE);
    }
    public void transfer(Owned<?> owned, Object newOwner) {
        if (STACK_TRACE)
            lastEvent = new TransferOwnershipEvent(owned, lastOwner(), newOwner, lastEvent);
        owner(newOwner);
    }

    public static @Nullable OwnershipEvent lastEvent(@Nullable OwnershipHistory history) {
        return history == null ? null : history.lastEvent();
    }

    public String toString() {return history();}

    public StringBuilder history(StringBuilder sb) {
        for (int i = 0; i < size; i++)
            sb.append(ThreadJournal.render(owners[(head+i)&MASK])).append(", ");
        sb.setLength(Math.max(sb.length()-2, 0));
        return sb;
    }
    public String history() { return history(new StringBuilder()).toString(); }

    public StringBuilder reverseHistory(StringBuilder sb) {
        for (int i = size-1; i >= 0; --i)
            sb.append(ThreadJournal.render(owners[(head+i)&MASK])).append(", ");
        sb.setLength(Math.max(sb.length()-2, 0));
        return sb;
    }

    @SuppressWarnings("unused") // used from debugger console
    public String reverseHistory() { return reverseHistory(new StringBuilder()).toString(); }
}
