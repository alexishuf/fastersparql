package com.github.alexishuf.fastersparql.emit;

import com.github.alexishuf.fastersparql.batch.type.Batch;

public interface RebindableReceiver<B extends Batch<B>> extends Rebindable, Receiver<B> {
}
