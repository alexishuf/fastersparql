package com.github.alexishuf.fastersparql.util.owned;

import com.github.alexishuf.fastersparql.FSProperties;
import com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;

import static com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal.journal;
import static com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal.render;

public abstract sealed class OwnershipEvent extends Throwable {
    private static final boolean STACK_TRACES = FSProperties.ownedStackTrace();
    private static final int     MAX_CAUSES = FSProperties.ownedStackTraceMax();

    private @Nullable OwnershipEvent cause;

    protected OwnershipEvent(String message) {
        super(message, null, true, STACK_TRACES);
    }
    protected OwnershipEvent(String message, boolean stackTrace) {
        super(message, null, true, stackTrace);
    }

    public boolean isOmitted() { return this instanceof OmittedEventHistory; }

    @Override public @Nullable OwnershipEvent getCause() {return cause;}

    @Override public @This OwnershipEvent initCause(Throwable cause) {
        if (cause instanceof OwnershipEvent e)
            this.cause = e;
        else if (cause == null)
            this.cause = null;
        else
            throw new IllegalArgumentException("cause is not a OwnershipEvent");
        return this;
    }

    @Override public String toString() {
        return getClass().getSimpleName() + ": " + getMessage();
    }

    protected static String mkMsg(Owned<?> owned, String verbBy, Object owner) {
        StringBuilder sb = new StringBuilder();
        String ownedName = owned.journalName();
        String ownerName = render(owner);
        sb.append(ownedName).append(verbBy).append(ownerName);
        Object root = Owned.rootOwner(owner);
        if (root != owner) {
            sb.append(", root=").append(render(root));
            journal(ownedName, verbBy, ownerName, root);
        } else {
            journal(ownedName, verbBy, owner);
        }
        return sb.toString();
    }

    protected static String mkTransferMsg(Owned<?> owned, Object oldOwner, Object newOwner) {
        var ownedName = owned.journalName();
        var oldName = render(oldOwner);
        var newName = render(newOwner);
        var sb = new StringBuilder(ownedName.length()+oldName.length()+newName.length()+40);
        sb.append(ownedName);
        sb.append(" transfer from ").append(oldName);
        Object oldRoot = Owned.rootOwner(oldOwner);
        if (oldRoot != oldOwner)
            sb.append(" (root=").append(render(oldRoot)).append(')');
        sb.append(" to ").append(newName);
        Object newRoot = Owned.rootOwner(newOwner);
        if (newRoot != newOwner)
            sb.append(" (root=").append(render(newRoot)).append(')');
        if (ThreadJournal.ENABLED) {
            journal(ownedName, "transfer from/to", oldName, newName);
            if (oldRoot != oldOwner || newRoot != newOwner)
                journal(ownedName, "transfer from/to roots", oldRoot, newRoot);
        }
        return sb.toString();
    }

    public static sealed class ReleasedOwnershipEvent extends OwnershipEvent {
        public ReleasedOwnershipEvent(Owned<?> owned, Object oldOwner,
                                      @Nullable OwnershipEvent takenEvent) {
            this(mkMsg(owned, " released by ", oldOwner), takenEvent);
        }

        protected ReleasedOwnershipEvent(String message, @Nullable OwnershipEvent takenEvent) {
            super(message);
            if (takenEvent != null)
                initCause(capCauseTraces(takenEvent));
        }
    }

    public static sealed class RecycledEvent extends ReleasedOwnershipEvent {
        public RecycledEvent(Owned<?> owned, Object oldOwner,
                             @Nullable OwnershipEvent takenEvent) {
            super(mkMsg(owned, " recycled by ", oldOwner), takenEvent);
        }

        protected RecycledEvent(String message, OwnershipEvent takenEvent) {
            super(message, takenEvent);
        }
    }

    public static final class GarbageEvent extends RecycledEvent {
        public GarbageEvent(Owned<?> owned, Object oldOwner,
                             @Nullable OwnershipEvent takenEvent) {
            super(mkMsg(owned, " recycled into garbage by ", oldOwner), takenEvent);
        }
    }

    public static final class TakenOwnershipEvent extends OwnershipEvent {
        public TakenOwnershipEvent(Owned<?> owned, Object newOwner,
                                   @Nullable OwnershipEvent releaseEvent) {
            super(mkMsg(owned, " taken by ", newOwner));
            if (releaseEvent != null)
                initCause(capCauseTraces(releaseEvent));
        }
    }

    public static final class TransferOwnershipEvent extends OwnershipEvent {
        public TransferOwnershipEvent(Owned<?> owned, Object oldOwner, Object newOwner,
                                      @Nullable OwnershipEvent lastEvent) {
            super(mkTransferMsg(owned, oldOwner, newOwner));
            if (lastEvent != null)
                initCause(capCauseTraces(lastEvent));
        }
    }

    protected static @Nullable OwnershipEvent capCauseTraces(@Nullable OwnershipEvent cause) {
        if (cause == null || MAX_CAUSES < 2)
            return null;
        Throwable node = cause, next;
        for (int i = 1; (next=node.getCause()) != null && i < MAX_CAUSES; node = next)
            ++i;
        if (next != null)
            node.initCause(OmittedEventHistory.INSTANCE);
        return cause;
    }

    public static final class OmittedEventHistory extends OwnershipEvent {
        private static final OmittedEventHistory INSTANCE = new OmittedEventHistory();
        private OmittedEventHistory() {super("previous events omitted", false);}
    }
}
