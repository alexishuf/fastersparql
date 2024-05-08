package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal;
import com.github.alexishuf.fastersparql.util.owned.Owned;
import com.github.alexishuf.fastersparql.util.owned.OwnershipHistory;
import org.checkerframework.checker.nullness.qual.Nullable;

import static com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal.journal;
import static com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal.render;
import static com.github.alexishuf.fastersparql.util.owned.OwnershipHistory.lastEvent;

public class OwnershipException extends IllegalStateException {
    private static String mkMsg(Owned<?> owned, @Nullable Object expectedOwner,
                                @Nullable Object actualOwner,
                                @Nullable OwnershipHistory history) {
        var sb = new StringBuilder();
        sb.append(render(owned)).append(" owned by ").append(render(actualOwner));
        Object actualRoot = Owned.rootOwner(actualOwner), expectedRoot = expectedOwner;
        if (actualRoot != actualOwner)
            sb.append(", root=").append(render(actualRoot));
        if (expectedOwner != null) {
            sb.append(" not by ").append(render(expectedOwner));
            expectedRoot = Owned.rootOwner(expectedOwner);
            if (expectedRoot != expectedOwner)
                sb.append(", root=").append(render(expectedRoot));
        }
        if (history != null) {
            sb.append(", last owners: ");
            history.reverseHistory(sb);
        }
        if (ThreadJournal.ENABLED) {
            journal(owned, "owned by/not", actualOwner, expectedOwner);
            if (actualRoot != actualOwner || expectedRoot != expectedOwner)
                journal(owned, "owned by/not root=", actualRoot, expectedRoot);
        }
        return sb.toString();
    }

    public OwnershipException(Owned<?> owned, Object expectedOwner, Object actualOwner,
                              @Nullable OwnershipHistory history) {
        super(mkMsg(owned, expectedOwner, actualOwner, history), lastEvent(history));
    }

    public OwnershipException(Owned<?> owned, Object actualOwner,
                              @Nullable OwnershipHistory history) {
        super(mkMsg(owned, null, actualOwner, history), lastEvent(history));
    }

    @Override public String toString() {
        return getClass().getSimpleName()+": "+getMessage();
    }
}
