package com.github.alexishuf.fastersparql.sparql.results;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.base.SPSCBIt;
import com.github.alexishuf.fastersparql.batch.type.TermBatch;
import com.github.alexishuf.fastersparql.model.rope.BufferRope;
import com.github.alexishuf.fastersparql.model.rope.ByteRope;
import com.github.alexishuf.fastersparql.model.rope.Rope;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import com.github.alexishuf.fastersparql.util.AutoCloseableSet;
import com.github.alexishuf.fastersparql.util.Results;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingDeque;

import static com.github.alexishuf.fastersparql.FSProperties.queueMaxBatches;
import static com.github.alexishuf.fastersparql.batch.type.Batch.TERM;
import static com.github.alexishuf.fastersparql.model.BindType.*;
import static com.github.alexishuf.fastersparql.sparql.expr.Term.termList;
import static com.github.alexishuf.fastersparql.util.Results.*;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.synchronizedList;
import static org.junit.jupiter.api.Assertions.*;

public class WsBindingParsersTest {
    private static final Rope DIE = Rope.of("!!!DIE!!!");

    private static final List<Throwable> threadErrors = synchronizedList(new ArrayList<>());
    private int byteOrBuf = 0;

    private final class Mailbox {
        private final String name;
        private final BlockingQueue<Rope> queue = new LinkedBlockingDeque<>();

        public Mailbox(String name) {
            this.name = name;
        }

        public void send(CharSequence frame) {
            Rope rope;
            if      (frame == DIE)           rope = DIE;
            else if ((byteOrBuf++ & 1) == 0) rope = new ByteRope(frame);
            else                             rope = new BufferRope(ByteBuffer.wrap(frame.toString().getBytes(UTF_8)));
            try {
                queue.put(rope);
            } catch (InterruptedException e) { throw new RuntimeException(e); }
        }

        public Rope recv() {
            try {
                return queue.take();
            } catch (InterruptedException e) { throw new RuntimeException(e); }
        }

        @Override public String toString() { return name; }
    }

    private Thread startThread(String name, Callable<?> callable) {
        Thread thread = new Thread(() -> {
            Thread.currentThread().setName(name);
            try {
                callable.call();
            } catch (Throwable t) {
                threadErrors.add(t);
                if      (t instanceof RuntimeException e) throw e;
                else if (t instanceof Error            e) throw e;
                else                                      throw new RuntimeException(t);
            }
        }, name);
        thread.start();
        return thread;
    }

    private Map<List<Term>, Results> parseBindingRow2Results(Results ex, Object ... args) {
        Map<List<Term>, Results> map = new HashMap<>();
        for (int i = 0; i < args.length; i += 2) { //noinspection unchecked
            List<Term> row = (List<Term>) args[i];
            assertEquals(ex.bindingsVars().size(), row.size());
            map.put(row, (Results)args[i+1]);
            assertTrue(ex.bindingsList().contains(row), row+" is not a binding row");
        }
        for (List<Term> row : ex.bindingsList())
            assertTrue(map.containsKey(row), "no results defined for " +row);
        return map;
    }

    private static Object feed(ResultsParserBIt<?> parser, Mailbox mailbox) {
        try {
            while (true) {
                Rope frame = mailbox.recv();
                if (frame == DIE) break;
                parser.feedShared(frame);
                if (frame instanceof ByteRope b) b.fill(0);
                else if (frame instanceof BufferRope b) b.buffer.clear().limit(0);
            }
        } catch (Throwable t) {
            parser.complete(t);
        } finally {
            if (!parser.isCompleted())
                parser.complete(null);
        }
        return null;
    }

    private @Nullable Object server(Results ex, Map<List<Term>, Results> bRow2Res,
                                    BIt<TermBatch> serverIt, Mailbox clientMBox) {
        assertTrue(bRow2Res.values().stream().map(Results::vars).distinct().count() <= 1);
        var serverVars = bRow2Res.values().stream().map(Results::vars).distinct().findFirst()
                                 .orElse(ex.vars().minus(ex.bindingsVars()));

        var serializer = new WsSerializer<TermBatch>(serverVars);
//        int requested = ex.bindingsList().size() < 4 ? 23 : 2;
        int activeBinding = 0;
//        clientMBox.send("!bind-request "+requested+"\n");
        for (TermBatch b = null; (b = serverIt.nextBatch(b)) != null;) {
            for (int r = 0; r < b.rows; r++, ++activeBinding) {
//                if (--requested == 0)
//                    clientMBox.send("!bind-request "+(++requested)+"\n");
                var bResults = bRow2Res.get(b.asList(r));
                assertNotNull(bResults, "no results defined for row " + b.asList(r));
                if (!bResults.isEmpty())
                    clientMBox.send("!active-binding "+activeBinding+"\n");
                try (var bIt = bResults.asPlan().execute(TERM)) {
                    assertEquals(serverVars, bIt.vars());
                    for (TermBatch bb = null; (bb = bIt.nextBatch(bb)) != null; )
                        clientMBox.send(serializer.serialize(bb));
                }
            }
        }
        // if the last bindings yielded no results, the cline needs this to issue rows required
        // by LEFT_JOIN/NOT_EXISTS/MINUS. For JOIN/EXISTS, this is unnecessary, but harmless.
        // However, if the last binding yielded non-empty results, this will be an echo of the last
        // !active-binding frame, which clients must tolerate.
        clientMBox.send("!active-binding "+(activeBinding-1)+"\n");
        //client must ignore !bind-request after it finished sending bindings

//        clientMBox.send("!bind-request 997\n");
        clientMBox.send("!end\n");
        return null;
    }


    private void test(Results ex, Object... bindingRow2results) {
        assertTrue(ex.hasBindings());
        switch (ex.bindType()) {
            case JOIN,LEFT_JOIN          -> assertTrue(ex.vars().containsAll(ex.bindingsVars()));
            case EXISTS,NOT_EXISTS,MINUS -> assertEquals(ex.bindingsVars(), ex.vars());
        }
        var bRow2Res = parseBindingRow2Results(ex, bindingRow2results);

        Mailbox serverMB = new Mailbox("server"), clientMB = new Mailbox("client");
        Thread serverFeeder = null, clientFeeder = null, server = null;
        try (var stuff = new AutoCloseableSet<>();
             var clientCb = stuff.put(new SPSCBIt<>(TERM, ex.vars(), queueMaxBatches()));
             var serverCb = stuff.put(new SPSCBIt<>(TERM, ex.bindingsVars(), queueMaxBatches()));
             var clientParser = new WsClientParserBIt<>(serverMB::send, TERM,
                                                        clientCb, ex.bindType(),
                                                        ex.bindingsBIt(), null, null);
             var serverParser = new WsServerParserBIt<>(clientMB::send, TERM, serverCb)) {
            serverFeeder = startThread("server-feeder", () -> feed(serverParser, serverMB));
            clientFeeder = startThread("client-feeder", () -> feed(clientParser, clientMB));
            server = startThread("server", () -> server(ex, bRow2Res, serverCb, clientMB));
            ex.check(clientCb);
        } finally {
            serverMB.send(DIE);
            clientMB.send(DIE);
            if (server != null) {
                try { server.join(1_000); } catch (InterruptedException ignored) {}
            }
            if (serverFeeder != null) {
                try { serverFeeder.join(1_000); } catch (InterruptedException ignored) {}
            }
            if (clientFeeder != null) {
                try { clientFeeder.join(1_000); } catch (InterruptedException ignored) {}
            }
            if (!threadErrors.isEmpty())
                fail("Server or feeder threads threw up", threadErrors.get(0));
        }
    }

    @BeforeEach
    void setUp() {
        threadErrors.clear();
    }

    @Test public void testSingleBindingSingleResult() {
        test(results("?x ?y", ":x1", ":y1").bindings("?x", ":x1"),
             termList(":x1"), results("?y", ":y1"));
    }

    @Test public void testEmptyBindings() {
        test(results("?x ?y").bindings("?x"));
    }

    @Test public void testIncreasingMatchesPerBinding() {
        test(results("?y     ?x",
                     ":y21", ":x2",
                     "31",   ":x3",
                     "32",   ":x3",
                     "_:41", ":x4",
                     "_:42", ":x4",
                     "_:43", ":x4"
                ).bindings("?x", ":x1", ":x2", ":x3", ":x4"),
             termList(":x1"), results("?y"),
             termList(":x2"), results("?y", ":y21"),
             termList(":x3"), results("?y", "31", "32"),
             termList(":x4"), results("?y", "_:41", "_:42", "_:43")
        );
    }

    @Test public void testDecreasingMatchesPerBinding() {
        test(results("?x ?y",
                     1,  11,
                     1,  12,
                     1,  13,
                     2,  21,
                     2,  22,
                     3,  31
                ).bindings("?x", 1, 2, 3, 4),
             termList(1), results("?y", 11, 12, 13),
             termList(2), results("?y", 21, 22),
             termList(3), results("?y", 31),
             termList(4), results("?y")
        );
    }

    @Test public void testLeftJoin() {
        test(results("?x  ?y",
                     1, 12,
                     2, null,
                     3, 31,
                     3, 32,
                     4, null).bindType(LEFT_JOIN).bindings("?x", 1, 2, 3, 4),
             termList(1), results("?y", 12),
             termList(2), results("?y"),
             termList(3), results("?y", 31, 32),
             termList(4), results("?y")
        );
    }

    @Test public void testBindToAsk() {
        test(results("?x", 1, 3).bindings("?x", 1, 2, 3, 4),
             termList(1), positiveResult(),
             termList(2), negativeResult(),
             termList(3), positiveResult(),
             termList(4), negativeResult()
        );
    }


    @Test public void testLeftJoinBindToAsk() {
        test(results("?x", 1, 2, 3, 4).bindType(LEFT_JOIN).bindings("?x", 1, 2, 3, 4),
             termList(1), positiveResult(),
             termList(2), negativeResult(),
             termList(3), positiveResult(),
             termList(4), negativeResult()
        );
    }

    @Test public void testExistsAnsweredWithAsk() {
        test(results("?x", 1, 3).bindType(EXISTS).bindings("?x", 1, 2, 3, 4),
             termList(1), positiveResult(),
             termList(2), negativeResult(),
             termList(3), positiveResult(),
             termList(4), negativeResult()
        );
    }

    @Test public void testExistsAnsweredWithValues() {
        test(results("?x", 1, 3).bindType(EXISTS).bindings("?x", 1, 2, 3, 4),
                termList(1), results("?y", 11),
                termList(2), results("?y"),
                termList(3), results("?y", 31, 32, 33, 34),
                termList(4), results("?y")
        );
    }

    @Test public void testNotExists() {
        test(results("?x", 2, 4).bindType(NOT_EXISTS).bindings("?x", 1, 2, 3, 4),
             termList(1), positiveResult(),
             termList(2), negativeResult(),
             termList(3), positiveResult(),
             termList(4), negativeResult()
        );
    }

    @Test public void testMinusAnswerWithValues() {
        test(results("?x", 2, 4).bindType(MINUS).bindings("?x", 1, 2, 3, 4),
                termList(1), results("?y", 11, 12),
                termList(2), results("?y"),
                termList(3), results("?y", 31),
                termList(4), results("?y")
        );
    }
}
