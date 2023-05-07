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
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.hdt.batch.IdAccess.encode;
import static com.github.alexishuf.fastersparql.hdt.batch.IdAccess.toNT;
import static com.github.alexishuf.fastersparql.util.concurrent.Async.uninterruptiblePut;
import static com.github.alexishuf.fastersparql.util.concurrent.Async.uninterruptibleTake;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.ForkJoinPool.commonPool;
import static org.rdfhdt.hdt.enums.TripleComponentRole.*;

public class HdtConverter {
    private static final Logger log = LoggerFactory.getLogger(HdtConverter.class);

    private Path tempDir;
    private Splitter.Mode splitMode = Splitter.Mode.LAST;
    private boolean optimizeLocality = true;
    private boolean validate = false;
    private boolean standaloneDict = false;

    public @This HdtConverter   tempDir(Path v)           { this.tempDir = v; return this; }
    public @This HdtConverter splitMode(Splitter.Mode v)  { this.splitMode = v; return this; }
    public @This HdtConverter  validate(boolean v)        { this.validate = v; return this; }
    public @This HdtConverter optimizeLocality(boolean v) { this.optimizeLocality = v; return this; }
    public @This HdtConverter standaloneDict(boolean v)   { this.standaloneDict = v; return this; }

    public void convert(Path hdtPath, Path destDir) throws IOException {
        try (HDT hdt = HDTManager.mapHDT(hdtPath.toString())) {
            convert(hdt, destDir);
        }
    }

    public void convert(HDT hdt, Path destDir) throws IOException {
        int dictId = IdAccess.register(hdt.getDictionary());
        Path tempDir = this.tempDir == null ? destDir : this.tempDir;
        try {
            if (standaloneDict) {
                try (DictSorter sorter = new DictSorter(tempDir, false, optimizeLocality)) {
                    log.info("{}: writing standalone dict...", destDir);
                    commonPool().invoke(new VisitStringsAction(sorter, dictId));
                    sorter.writeDict(destDir.resolve("strings"));
                }
            } else {
                try (var db = new CompositeDictBuilder(tempDir, destDir, splitMode, optimizeLocality)) {
                    commonPool().invoke(new VisitStringsAction(db, dictId));
                    log.info("{}: writing shared dict...", destDir);
                    var sndPass = db.nextPass();
                    log.info("{}: Second pass on HDT Dictionary...", destDir);
                    commonPool().invoke(new VisitStringsAction(sndPass, dictId));
                    sndPass.write();
                }
            }
            if (validate && !standaloneDict) {
                try (Dict d = Dict.loadStandalone(destDir.resolve("shared"))) {
                    log.info("{} Validating shared...", destDir);
                    d.validate();
                }
            }
            try (var strings = Dict.load(destDir.resolve("strings"));
                 var sorter = new TriplesSorter(tempDir)) {
                if (validate) {
                    log.info("{} Validating strings dict...", destDir);
                    strings.validate();
                }
                log.info("{}: iterating/sorting triples", destDir);
                var it = hdt.getTriples().searchAll();
                long triples = hdt.getTriples().getNumberOfElements();
                commonPool().invoke(new TriplesAction(triples, it, sorter, dictId, strings));
                log.info("{}: writing spo, pso and ops indices...", destDir);
                sorter.write(destDir);
            }
            if (validate) {
                boolean valid = Stream.of("spo", "pso", "ops").parallel().map(name -> {
                    Path path = destDir.resolve(name);
                    try (var triples = new Triples(path)) {
                        triples.validate();
                        return true;
                    } catch (Throwable t) {
                        log.error("{}: {}", path, t.toString());
                        return false;
                    }
                }).reduce(true, Boolean::logicalAnd);
                if (!valid)
                    throw new IOException("Some Triple index files are not valid");
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
        private final Hdt2StoreIdCache idCache = new Hdt2StoreIdCache();

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

        private static final class Hdt2StoreIdCache {
            private static final VarHandle LOCK;
            static {
                try {
                    LOCK = MethodHandles.lookup().findVarHandle(Hdt2StoreIdCache.class, "plainLock", int.class);
                } catch (NoSuchFieldException | IllegalAccessException e) {
                    throw new ExceptionInInitializerError(e);
                }
            }
            @SuppressWarnings("unused") private int plainLock;
            private static final int CAPACITY = 1024*1024>>4;
            private static final int MASK = CAPACITY-1;
            private final long[] table = new long[CAPACITY<<1];

            private void lock() {
                while ((int)LOCK.compareAndExchangeAcquire(this, 0, 1) == 1) Thread.onSpinWait();
            }
            private void unlock() { LOCK.setRelease(this, 0); }

            public long get(long plainHdtId, TripleComponentRole role) {
                long key = plainHdtId << 2 | role.ordinal(), value = 0;
                int slot = ((int)key & MASK) << 1;
                lock();
                try {
                    if (table[slot] == key)
                        value = table[slot+1];
                } finally { unlock(); }
                return value;
            }
            public void set(long plainHdtId, TripleComponentRole role, long storeId) {
                long key = plainHdtId << 2 | role.ordinal();
                int slot = ((int)key & MASK) << 1;
                lock();
                try {
                    table[slot] = key;
                    table[slot+1] = storeId;
                } finally { unlock(); }
            }
        }

        private final class Translator extends RecursiveAction {
            private final Dict.AbstractLookup lookup = strings.polymorphicLookup();

            private long translate(long hdtId, TripleComponentRole role) {
                long storeId = idCache.get(hdtId, role);
                if (storeId == 0) {
                    SegmentRope nt = toNT(encode(hdtId, dictId, role));
                    if (nt == null)
                        throw new RuntimeException("string for " + hdtId + " not found in HDT dict");
                    storeId = lookup.find(nt);
                    if (storeId == Dict.NOT_FOUND)
                        throw new RuntimeException("String not found in strings dict: "+nt);
                    idCache.set(hdtId, role, storeId);
                }
                return storeId;
            }
            @Override protected void compute() {
                try {
                    for (T t; (t = uninterruptibleTake(raw)) != T_DIE; ) {
                        T translated = recycled.poll();
                        if (translated == null) translated = new T();
                        translated.set(translate(t.s, SUBJECT),
                                       translate(t.p, PREDICATE),
                                       translate(t.o, OBJECT));
                        uninterruptiblePut(ready, translated);
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
                            log.info("Visited {}/{} triples ({}%)",
                                     i, triples, String.format("%.3f", 100.0*i/triples));
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
                            log.info("Visited {}/{} strings ({}%)",
                                     i, strings, String.format("%.3f", 100.0*i/strings));
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
