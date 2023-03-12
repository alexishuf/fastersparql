package com.github.alexishuf.fastersparql.fed.selectors;

import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import com.github.alexishuf.fastersparql.exceptions.BadSerializationException;
import com.github.alexishuf.fastersparql.fed.Selector;
import com.github.alexishuf.fastersparql.fed.Spec;
import com.github.alexishuf.fastersparql.model.rope.ByteRope;
import com.github.alexishuf.fastersparql.operators.plan.TriplePattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import static java.nio.charset.StandardCharsets.UTF_8;

public final class TrivialSelector extends Selector {
    private static final Logger log = LoggerFactory.getLogger(TrivialSelector.class);
    public static final String NAME = "trivial";
    public static final String RESULT = "result";
    private final boolean result;

    private static final byte[] TRUE_U8 = "true".getBytes(UTF_8);
    private static final byte[] FALSE_U8 = "false".getBytes(UTF_8);
    private static final byte[] TRUE_BYTES = (NAME+"\ntrue\n").getBytes(UTF_8);
    private static final byte[] FALSE_BYTES = (NAME+"\nfalse\n").getBytes(UTF_8);

    public static final class TrivialLoader implements Loader {
        @Override public String name() { return NAME; }

        @Override
        public Selector load(SparqlClient client, Spec spec, InputStream in) throws IOException {
            ByteRope r = new ByteRope();
            Boolean state;
            if (!r.readLine(in))                 state = null; // EOF
            else if (r.has(0,  TRUE_U8)) state = true;
            else if (r.has(0, FALSE_U8)) state = false;
            else                                 state = null; // invalid true|false line
            if (state == null)
                throw new BadSerializationException("Expected true|false, got \""+r+"\"");
            else if (state != spec.getBool(RESULT))
                log.warn("State result was {}. Will honor spec ({})", state, spec.getBool(RESULT));
            return new TrivialSelector(client.endpoint(), spec);
        }

        @Override public Selector create(SparqlClient client, Spec spec) {
            return new TrivialSelector(client.endpoint(), spec);
        }
    }

    public TrivialSelector(SparqlEndpoint endpoint, Spec spec) {
        super(endpoint, spec);
        result = spec.getOr(RESULT, true);
        notifyInit(InitOrigin.SPEC, null);
    }

    @Override public void save(OutputStream out) throws IOException {
        out.write(result ? TRUE_BYTES : FALSE_BYTES);
    }

    @Override public boolean has(TriplePattern tp) { return result; }

    @Override public void close() { }
}
