package com.github.alexishuf.fastersparql.store.index;

import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;

import static com.github.alexishuf.fastersparql.model.rope.ByteRope.EMPTY;

public class CompositeDictBuilder implements AutoCloseable, NTVisitor {
    private static final Logger log = LoggerFactory.getLogger(CompositeDictBuilder.class);

    private final Path tempDir, destDir;
    private final DictSorter sharedSorter;
    private SecondPass secondPass;
    private final Splitter split;

    public CompositeDictBuilder(Path tempDir, Path destDir, Splitter.Mode splitMode) {
        this.tempDir = tempDir;
        this.destDir = destDir;
        this.split = new Splitter(splitMode);
        this.sharedSorter = new DictSorter(tempDir);
        this.sharedSorter.copy(EMPTY);
    }

    @Override public void close() {
        if (secondPass != null)
            secondPass.close();
        sharedSorter.close();
    }

    @Override public String toString() {
        return String.format("CompositeDictBuilder@%x[%s]", System.identityHashCode(this), destDir);
    }

    @Override public void visit(SegmentRope string) {
        if (split.split(string) != Splitter.SharedSide.NONE)
            sharedSorter.copy((SegmentRope) split.shared());
    }

    public SecondPass nextPass() throws IOException {
        Path sharedPath = destDir.resolve("shared");
        sharedSorter.writeDict(sharedPath);
        sharedSorter.close();
        return secondPass = new SecondPass(new StandaloneDict(sharedPath));
    }

    public class SecondPass implements AutoCloseable, NTVisitor {
        private final DictSorter sorter;
        private final StandaloneDict sharedDict;
        private final Dict.AbstractLookup shared;
        private boolean sharedOverflow;

        public SecondPass(StandaloneDict shared) {
            this.sorter = new DictSorter(tempDir);
            this.shared = (this.sharedDict = shared).lookup();
        }

        @Override public void close() {
            sharedDict.close();
            sorter.close();
            secondPass = null;
        }

        @Override public String toString() {
            return CompositeDictBuilder.this+"$SecondPass";
        }

        private void onSharedOverflow() {
            if (sharedOverflow) return;
            sharedOverflow = true;
            log.warn("Some shared strings have ids above 24 bits. They will be copied instead of referenced to");
        }

        @Override public void visit(SegmentRope string) {
            split.split(string);
            long shId = shared.find(split.shared()); // will not call split.split()
            if (shId == Dict.NOT_FOUND)
                throw new IllegalStateException("shared string not found");
            else if (shId > Splitter.MAX_SHARED_ID)
                onSharedOverflow();
            sorter.copy(split.b64(shId), (SegmentRope) split.local());
        }

        public void write() throws IOException {
            sorter.usesShared = true;
            sorter.sharedOverflow = sharedOverflow;
            sorter.split = split.mode();
            sorter.writeDict(destDir.resolve("strings"));
            try (var shared = new StandaloneDict(destDir.resolve("shared"));
                 var strings = new CompositeDict(destDir.resolve("strings"), shared)) {
                shared.validate();
                strings.validate();
            }
        }
    }
}
