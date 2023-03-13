package com.github.alexishuf.fastersparql.batch.operators;

import com.github.alexishuf.fastersparql.batch.Batch;
import com.github.alexishuf.fastersparql.batch.adapters.AbstractBItTest;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.row.NotRowType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.fail;

class ShortQueueBItTest extends AbstractBItTest {

    private static class QueueScenario extends Scenario {
        private final boolean copy;
        public QueueScenario(Scenario other, boolean copy) {
            super(other);
            this.copy = copy;
        }
    }

    @Override protected List<? extends Scenario> scenarios() {
        List<QueueScenario> list = new ArrayList<>();
        for (Scenario b : baseScenarios()) {
            list.add(new QueueScenario(b, false));
            list.add(new QueueScenario(b, true));

            var single = new Scenario(b.size(), b.size(), b.size(), b.drainer(), b.error());
            list.add(new QueueScenario(single, false));
            list.add(new QueueScenario(single, true));
        }

        return list;
    }

    @Override protected void run(Scenario scenario) {
        QueueScenario s = (QueueScenario) scenario;
        try (var q = new ShortQueueBIt<>(NotRowType.INTEGER, Vars.EMPTY)) {
            List<Batch<Integer>> copied = new ArrayList<>();
            List<List<Integer>> copiedValues = new ArrayList<>();
            Thread producer = Thread.startVirtualThread(() -> {
                int chunk = s.minBatch(), i = 0;
                while (i < s.size()) {
                    Batch<Integer> b = new Batch<>(Integer.class, chunk);
                    int end = Math.min(s.size(), i + chunk);
                    while (i < end)
                        b.add(i++);
                    if (s.copy){
                        copied.add(b);
                        copiedValues.add(Arrays.asList(b.array).subList(0, b.size));
                        q.copy(b);
                    } else {
                        q.feed(b);
                    }
                }
                q.complete(s.error());
            });
            s.drainer().drainOrdered(q, s.expected(), s.error());
            producer.join();
            for (int i = 0; i < copied.size(); i++) {
                var expected = copiedValues.get(i).toArray(Integer[]::new);
                var actual = Arrays.copyOf(copied.get(i).array, copied.get(i).size);
                assertArrayEquals(expected, actual);
            }
        } catch (InterruptedException e) {
            fail(e);
        }
    }
}