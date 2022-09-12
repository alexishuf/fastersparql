package com.github.alexishuf.fastersparql.client.model.batch.adapters;

class BItPublisherTest extends AbstractBItPublisherTest {

    @Override protected void run(Scenario s) {
        drain(new BItPublisher<>(s.createIt()), 1).assertExpected(s);
        drain(new BItPublisher<>(s.createIt()), 4).assertExpected(s);
        drain(new BItPublisher<>(s.createIt()), Long.MAX_VALUE).assertExpected(s);
        fluxDrain(new BItPublisher<>(s.createIt()), s).assertExpected(s);
    }
}