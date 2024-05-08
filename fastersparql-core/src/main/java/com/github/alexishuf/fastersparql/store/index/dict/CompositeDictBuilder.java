package com.github.alexishuf.fastersparql.store.index.dict;

import com.github.alexishuf.fastersparql.model.rope.FinalSegmentRope;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;

public class CompositeDictBuilder implements AutoCloseable, NTVisitor {
    private static final Logger log = LoggerFactory.getLogger(CompositeDictBuilder.class);

    private final Path tempDir, destDir;
    private final DictSorter sharedSorter;
    private SecondPass secondPass;
    private final boolean optimizeLocality;
    private Splitter split;

    public CompositeDictBuilder(Path tempDir, Path destDir, Splitter.Mode splitMode,
                                boolean optimizeLocality) {
        this.tempDir = tempDir;
        this.destDir = destDir;
        this.split = Splitter.create(splitMode).takeOwnership(this);
        this.optimizeLocality = optimizeLocality;
        this.sharedSorter = new DictSorter(tempDir, false, optimizeLocality);
        this.sharedSorter.copy(FinalSegmentRope.EMPTY);
    }

    @Override public void close() {
        if (secondPass != null)
            secondPass.close();
        sharedSorter.close();
        split = split.recycle(this);
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
        return secondPass = new SecondPass(Dict.loadStandalone(sharedPath));
    }

    public class SecondPass implements AutoCloseable, NTVisitor {
        private final DictSorter sorter;
        private final Dict sharedDict;
        private final Dict.AbstractLookup<?> shared;
        private boolean sharedOverflow;

        public SecondPass(Dict shared) {
            this.sorter = new DictSorter(tempDir, true, optimizeLocality);
            this.sharedDict = shared;
            this.shared = shared.polymorphicLookup().takeOwnership(this);
        }

        @Override public void close() {
            sharedDict.close();
            sorter.close();
            shared.recycle(this);
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
            sorter.sharedOverflow = sharedOverflow;
            sorter.split = split.mode();
            sorter.writeDict(destDir.resolve("strings"));
        }
    }
}
