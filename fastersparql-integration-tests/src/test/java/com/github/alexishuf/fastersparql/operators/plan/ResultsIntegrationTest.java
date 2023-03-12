package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.client.util.VThreadTaskSet;
import com.github.alexishuf.fastersparql.util.AutoCloseableSet;
import com.github.alexishuf.fastersparql.util.Results;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.function.Consumer;

import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.fail;

public abstract class ResultsIntegrationTest {
    protected abstract AutoCloseableSet<SparqlClient> createClients();
    protected abstract List<Results> data();

    @Test void test() throws Exception{
        List<Results> data = data();
        int repetitions = Math.max(1, Runtime.getRuntime().availableProcessors() * 4 / data.size());
        try (var tasks = new VThreadTaskSet(getClass().getSimpleName());
             var clients = createClients()) {
            test(data, clients, Runnable::run);
            test(data, clients, runnable -> tasks.repeat(repetitions, runnable));
        }
    }

    private void test(List<Results> data, AutoCloseableSet<SparqlClient> clients,
                      Consumer<Runnable> executor) {
        for (int iResults = 0, dataSize = data.size(); iResults < dataSize; iResults++) {
            Results results = data.get(iResults);
            for (int iClient = 0, clientsSize = clients.size(); iClient < clientsSize; iClient++) {
                SparqlClient client = clients.get(iClient);
                try {
                    executor.accept(() -> results.check(client));
                } catch (Throwable t) {
                    String msg = format("For client[%d]=%s, results[%d]=%s",
                            iClient, client, iResults, results);
                    fail(msg, t);
                }
            }
        }
    }

}
