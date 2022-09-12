package com.github.alexishuf.fastersparql.client.model.batch.adapters;

class BItFSPublisherTest extends AbstractBItPublisherTest {

    @Override protected void run(Scenario s) {
        drain(new BItFSPublisher<>(s.createIt()), 1).assertExpected(s);
//        drain(new BItFSPublisher<>(s.createIt()), 2).assertExpected(s);
//        drain(new BItFSPublisher<>(s.createIt()), Long.MAX_VALUE).assertExpected(s);
//        fluxDrain(new BItFSPublisher<>(s.createIt()), s).assertExpected(s);
    }
}