package com.github.alexishuf.fastersparql.store.index;

import com.github.alexishuf.fastersparql.batch.Timestamp;
import com.github.alexishuf.fastersparql.hdt.batch.IdAccess;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import org.checkerframework.common.returnsreceiver.qual.This;
import org.rdfhdt.hdt.dictionary.Dictionary;
import org.rdfhdt.hdt.dictionary.DictionarySection;
import org.rdfhdt.hdt.enums.TripleComponentRole;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.triples.IteratorTripleID;
import org.rdfhdt.hdt.triples.TripleID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.alexishuf.fastersparql.hdt.batch.IdAccess.encode;
import static com.github.alexishuf.fastersparql.hdt.batch.IdAccess.toNT;
import static com.github.alexishuf.fastersparql.util.concurrent.Async.uninterruptiblePut;
import static com.github.alexishuf.fastersparql.util.concurrent.Async.uninterruptibleTake;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.ForkJoinPool.commonPool;
import static org.rdfhdt.hdt.enums.TripleComponentRole.*;

public class HdtConverter {
    private static final Logger log = LoggerFactory.getLogger(HdtConverter.class);

    public static void convert(Path hdtPath, Path destDir) throws IOException {
        convert(hdtPath, destDir, destDir);
    }
    public static void convert(Path hdtPath, Path destDir, Path tempDir) throws IOException {
        try (HDT hdt = HDTManager.mapHDT(hdtPath.toString())) {
            convert(hdt, destDir, tempDir);
        }
    }

    public static void convert(HDT hdt, Path destDir) throws IOException {
        convert(hdt, destDir, destDir);
    }
    public static void convert(HDT hdt, Path destDir, Path tempDir) throws IOException {
        int dictId = IdAccess.register(hdt.getDictionary());
        try (var db = new CompositeDictBuilder(tempDir, destDir)) {
            commonPool().invoke(new VisitStringsAction(db, dictId));
            log.info("{}: writing shared dict...", destDir);
            var sndPass = db.nextPass();
            log.info("{}: Second pass on HDT Dictionary...", destDir);
            commonPool().invoke(new VisitStringsAction(sndPass, dictId));
            sndPass.write();
            log.info("{}: writing strings dict...", destDir);
            try (var shared = new Dict(destDir.resolve("shared"));
                 var strings = new Dict(destDir.resolve("strings"), shared);
                 var sorter = new TriplesSorter(tempDir)) {
                var it = hdt.getTriples().searchAll();
                long triples = hdt.getTriples().getNumberOfElements();
                commonPool().invoke(new TriplesAction(triples, it, sorter, dictId, strings));
                log.info("{}: writing spo, pso and ops indices...", destDir);
                sorter.write(destDir);
            }
        } finally {
            IdAccess.release(dictId);
        }
    }

    private static final class TriplesAction  extends RecursiveAction {
        private static final Logger log = LoggerFactory.getLogger(TriplesAction.class);
        private static final int Q_SIZE = 64;
        private static final T T_DIE = new T().set(-1L, -1L, -1L);
        private final long triples;
        private final IteratorTripleID it;
        private final TriplesSorter sorter;
        private final int dictId;
        private final Dict strings;
        private final int translators = Math.max(1, Runtime.getRuntime().availableProcessors()-2);
        private final AtomicInteger activeTranslators = new AtomicInteger(translators);
        private final ArrayBlockingQueue<T> raw = new ArrayBlockingQueue<>(Q_SIZE);
        private final ArrayBlockingQueue<T> recycled = new ArrayBlockingQueue<>(Q_SIZE);
        private final ArrayBlockingQueue<T> ready = new ArrayBlockingQueue<>(Q_SIZE);

        public TriplesAction(long triples, IteratorTripleID it, TriplesSorter sorter,
                             int dictId, Dict strings) {
            this.triples = triples;
            this.it = it;
            this.sorter = sorter;
            this.dictId = dictId;
            this.strings = strings;
        }

        private static final class T {
            long s, p, o;
            public @This T set(long s, long p, long o) {
                this.s = s; this.p = p; this.o = o;
                return this;
            }
            @Override public String toString() { return "["+s+' '+p+' '+o+"]"; }
            @Override public int hashCode() { return (int) (31*(31*s + p) + o); }
            @Override public boolean equals(Object obj) {
                return obj instanceof T t && t.s == s && t.p == p && t.o == o;
            }
        }

        private final class Iterator extends RecursiveAction {
            @Override protected void compute() {
                try {
                    while (it.hasNext()) {
                        T t = recycled.poll();
                        if (t == null)
                            t = new T();
                        TripleID tid = it.next();
                        t.set(tid.getSubject(), tid.getPredicate(), tid.getObject());
                        uninterruptiblePut(raw, t);
                    }
                } finally {
                    for (int i = 0; i < translators; i++)
                        uninterruptiblePut(raw, T_DIE);
                }
            }
        }
        private final class Translator extends RecursiveAction {
            @Override protected void compute() {
                try {
                    var tmp = new StringSplitStrategy();
                    for (T t; (t = uninterruptibleTake(raw)) != T_DIE; ) {
                        var sNT = toNT(encode(t.s, dictId, SUBJECT));
                        var pNT = toNT(encode(t.p, dictId, PREDICATE));
                        var oNT = toNT(encode(t.o, dictId, OBJECT));
                        if (sNT == null || pNT == null || oNT == null)
                            throw new RuntimeException("Terms for " + t + " not found in dict");
                        long s = strings.find(sNT, tmp);
                        long p = strings.find(pNT, tmp);
                        long o = strings.find(oNT, tmp);
                        if (s == Dict.NOT_FOUND || p == Dict.NOT_FOUND || o == Dict.NOT_FOUND)
                            throw new RuntimeException("Strings missing for "+sNT+" "+pNT+" "+oNT);
                        T translated = recycled.poll();
                        if (translated == null) translated = new T();
                        uninterruptiblePut(ready, translated.set(s, p, o));
                    }
                    if (activeTranslators.decrementAndGet() == 0)
                        uninterruptiblePut(ready, T_DIE);
                } catch (Throwable t) {
                    commonPool().externalSubmit(new RecursiveAction() {
                        @Override protected void compute() {
                            try {
                                while (raw.take() != T_DIE) Thread.onSpinWait();
                            } catch (InterruptedException ignored) {}
                        }
                    });
                    if (activeTranslators.decrementAndGet() == 0) {
                        uninterruptiblePut(ready, T_DIE);
                    }
                    log.error("Translator for sorter={} failed", sorter, t);
                    throw t;
                }
            }
        }

        private final class Sorter extends RecursiveAction {
            @Override protected void compute()  {
                try {
                    long i = 0, last = Timestamp.nanoTime();
                    for (T t; (t = uninterruptibleTake(ready)) != T_DIE; ) {
                        if (Timestamp.nanoTime()-last > 10_000_000_000L) {
                            log.info("Visited {}/{} triples", i, triples);
                            last = Timestamp.nanoTime();
                        }
                        sorter.addTriple(t.s, t.p, t.o);
                        recycled.offer(t);
                        ++i;
                    }
                } catch (IOException e) {
                    completeExceptionally(e);
                    // consume from ready, until the producers finish
                    try {
                        for (T t; (t = ready.take()) != T_DIE; )
                            recycled.offer(t);
                    } catch (InterruptedException ignored) { }
                }
            }
        }

        @Override protected void compute() {
            ArrayList<RecursiveAction> tasks = new ArrayList<>(translators+2);
            tasks.add(new Sorter());
            tasks.add(new Iterator());
            for (int i = 0; i < translators; i++)
                tasks.add(new Translator());
            invokeAll(tasks);
        }
    }

    private static final class VisitStringsAction extends RecursiveAction {
        private static final SegmentRope DIE_ROPE = new SegmentRope("DIE".getBytes(UTF_8), 0, 3);
        private final int dictId;
        private final NTVisitor visitor;
        private final ArrayBlockingQueue<SegmentRope> queue = new ArrayBlockingQueue<>(128);
        private final AtomicInteger activeDecoders = new AtomicInteger(4);

        public VisitStringsAction(NTVisitor visitor, int dictId) {
            this.dictId = dictId;
            this.visitor = visitor;
        }

        private final class Decoder extends RecursiveAction {
            private final DictionarySection section;
            private final TripleComponentRole role;
            private final long offset;

            public Decoder(DictionarySection section, TripleComponentRole role,
                           long offset) {
                this.section = section;
                this.role = role;
                this.offset = offset;
            }

            @Override protected void compute() {
                try {
                    for (long i = 1, n = section.getNumberOfElements(); i <= n; i++) {
                        long id = i + offset;
                        uninterruptiblePut(queue, toNT(encode(id, dictId, role)));
                    }
                } finally {
                    if (activeDecoders.decrementAndGet() == 0)
                        uninterruptiblePut(queue, DIE_ROPE);
                }
            }
        }
        private final class Consumer extends RecursiveAction {
            private final long strings;

            private Consumer(long strings) {
                this.strings = strings;
            }

            @Override protected void compute() {
                long i = 0, last  = Timestamp.nanoTime();
                try {
                    for (SegmentRope r; (r = uninterruptibleTake(queue)) != DIE_ROPE; ) {
                        if (Timestamp.nanoTime()-last > 10_000_000_000L) {
                            last = Timestamp.nanoTime();
                            log.info("visited {}/{} strings", i, strings);
                        }
                        ++i;
                        visitor.visit(r);
                    }
                } catch (Throwable t) {
                    try {
                        while (queue.take() != DIE_ROPE) Thread.onSpinWait();
                    } catch (InterruptedException ignored) {}
                    throw t;
                }
            }
        }

        @Override protected void compute() {
            Dictionary d = IdAccess.dict(dictId);
            long shared = d.getNshared();
            long stringCount = shared + d.getSubjects().getNumberOfElements()
                                      + d.getPredicates().getNumberOfElements()
                                      + d.getObjects().getNumberOfElements();
            invokeAll(new Decoder(d.getShared(), SUBJECT, 0),
                      new Decoder(d.getSubjects(), SUBJECT, shared),
                      new Decoder(d.getPredicates(), TripleComponentRole.PREDICATE, 0),
                      new Decoder(d.getObjects(), TripleComponentRole.OBJECT, shared),
                      new Consumer(stringCount));
        }
    }

}
