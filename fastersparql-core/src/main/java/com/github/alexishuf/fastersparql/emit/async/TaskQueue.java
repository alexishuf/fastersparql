package com.github.alexishuf.fastersparql.emit.async;

import com.github.alexishuf.fastersparql.emit.async.EmitterService.LowPriorityTask;
import com.github.alexishuf.fastersparql.emit.async.EmitterService.Task;
import com.github.alexishuf.fastersparql.emit.async.EmitterService.Worker;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.jctools.queues.atomic.MpscUnboundedAtomicArrayQueue;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.locks.LockSupport;

import static java.lang.Thread.onSpinWait;

class TaskQueue {
    private static final int LOCAL_CAPACITY  = 128;
    private static final int SHARED_CAPACITY = 16_384;
    private static final int     PADDING     = 128/4;
    static {
        assert Integer.bitCount(LOCAL_CAPACITY) == 1;
        assert Integer.bitCount(SHARED_CAPACITY) == 1;
    }

    private static abstract class QueueHandle_0 {
        protected final Task<?>[] data;
        protected final int base, mask;
        protected final Worker worker, friend0, friend1;

        protected QueueHandle_0(Task<?>[] data, int base, int capacity,
                                Worker[] workers, int workerIdx) {
            this.data        = data;
            this.base        = base;
            this.mask        = capacity-1;
            this.worker      = workers[workerIdx];
            this.friend0 = workers[workerIdx == 0 ? workers.length-1 : workerIdx-1];
            this.friend1 = workers[(workerIdx+1) % workers.length];
            if (Integer.bitCount(capacity) != 1)
                throw new IllegalArgumentException("capacity is not a power of 2");
            if (base+capacity > data.length)
                throw new IllegalArgumentException("capacity is out of bounds");
            if (base < PADDING)
                throw new IllegalArgumentException("queue begins inside padding region");
            if (base+capacity > data.length-PADDING)
                throw new IllegalArgumentException("queue ends inside padding region");
        }
    }

    @SuppressWarnings("unused")
    private static abstract class QueueHandle_1 extends QueueHandle_0 {
        private volatile long l0_0, l0_1, l0_2, l0_3, l0_4, l0_5, l0_6, l0_7;
        private volatile long l1_0, l1_1, l1_2, l1_3, l1_4, l1_5, l1_6, l1_7;

        public QueueHandle_1(Task<?>[] data, int base, int capacity,
                             Worker[] workers, int workerIdx) {
            super(data, base, capacity, workers, workerIdx);
        }
    }
    private static abstract class QueueHandle_2 extends QueueHandle_1 {
        private static final int LOCKED = -1;
        private static final VarHandle S;
        static {
            try {
                S = MethodHandles.lookup().findVarHandle(QueueHandle_2.class, "plainSize", int.class);
            } catch (NoSuchFieldException|IllegalAccessException e) {
                throw new ExceptionInInitializerError(e);
            }
        }
        @SuppressWarnings("unused") private int plainSize;
        private int head;

        public QueueHandle_2(Task<?>[] data, int base, int capacity,
                             Worker[] workers, int workerIdx) {
            super(data, base, capacity, workers, workerIdx);
        }

        @Override public String toString() {
            return String.format("QueueHandle{base=%d, size=%d, cap=%d, head=%d}",
                                 base, plainSize, mask+1, head);
        }

        public int size() {
            int size;
            while ((size=(int)S.getOpaque(this)) == LOCKED) onSpinWait();
            return size;
        }

        final @Nullable Task<?> spinPoll() {
            int size;
            if ((int)S.getOpaque(this) == 0)
                return null;
            while ((size=(int)S.getAndSetAcquire(this, LOCKED)) == LOCKED)
                onSpinWait();
            Task<?> task;
            if (size != 0) {
                int head = this.head;
                task = data[base+head];
                this.head = size-- == 1 ? 0 : (head+1)&mask;
            } else {
                task = null;
            }
            S.setRelease(this, size);
            return task;
        }

        final boolean spinOffer(@NonNull Task<?> task) {
            int size;
            while ((size=(int)S.getAndSetAcquire(this, LOCKED)) == LOCKED)
                onSpinWait();
            boolean add = size <= mask;
            if (add)
                data[base+((head+size++)&mask)] = task;
            S.setRelease(this, size);
            switch (size) {
                case 1 -> { if (!worker.unparkNow()) friend0.unpark(); }
                case 2 -> { if (!friend0.unpark())   friend1.unpark(); }
                case 3 ->                            friend1.unparkNow();
            }
            return add;
        }
    }
    @SuppressWarnings("unused")
    private static final class QueueHandle extends QueueHandle_2 {
        private volatile long l0_0, l0_1, l0_2, l0_3, l0_4, l0_5, l0_6, l0_7;
        private volatile long l1_0, l1_1, l1_2, l1_3, l1_4, l1_5, l1_6, l1_7;

        public QueueHandle(Task<?>[] data, int base, int capacity,
                           Worker[] workers, int workerIdx) {
            super(data, base, capacity, workers, workerIdx);
        }
    }

    private final Task<?>[] data;
    private final QueueHandle[] handles;
    private final QueueHandle[][] pollSeq;
    private final int workerMask;
    private final int shHandleIdx;
    private final MpscUnboundedAtomicArrayQueue<Task<?>> overflowQueue;

    public TaskQueue(Worker[] workers) {
        int po2WorkerCount = 1 << (32-Integer.numberOfLeadingZeros(workers.length-1));
        int queueTypes     = 2;
        shHandleIdx = po2WorkerCount*2;
        workerMask  = po2WorkerCount-1;
        handles     = new QueueHandle[queueTypes + po2WorkerCount*queueTypes];
        data        = new Task[PADDING
                             + po2WorkerCount*(queueTypes*LOCAL_CAPACITY + PADDING)
                             + queueTypes* SHARED_CAPACITY
                             + PADDING];
        int base = PADDING, handleIdx = 0;
        for (int workerId = 0; workerId < po2WorkerCount; workerId++) {
            handles[handleIdx++] = new QueueHandle(data, base, LOCAL_CAPACITY, workers, workerId);
            base += LOCAL_CAPACITY;
            handles[handleIdx++] = new QueueHandle(data, base, LOCAL_CAPACITY, workers, workerId);
            base += LOCAL_CAPACITY+PADDING;
        }
        assert handleIdx == shHandleIdx;
        handles[handleIdx++] = new QueueHandle(data, base, SHARED_CAPACITY, workers, 0);
        base += SHARED_CAPACITY;
        handles[handleIdx  ] = new QueueHandle(data, base, SHARED_CAPACITY, workers, 0);
        assert nonOverlappingQueues();
        pollSeq = new QueueHandle[po2WorkerCount][];
        assert Integer.bitCount(po2WorkerCount) == 1;
        for (int workerId = 0; workerId < po2WorkerCount; workerId++) {
            var seq = pollSeq[workerId] = new QueueHandle[handles.length];
            seq[0] = handles[ workerId<<1   ];
            seq[1] = handles[(workerId<<1)+1];
            int i = 2;
            // poll all normal priority worker-local queues, sorted by shorter distance
            for (int step = 1; step < po2WorkerCount; step++) {
                int neighborId;
                if ((step&1) == 1)
                    neighborId = (workerId + (1+(step>>1)))&workerMask;
                else
                    neighborId = (workerId - (step>>1))&workerMask;
                seq[i++] = handles[neighborId<<1];
            }
            seq[i++] = handles[shHandleIdx];
            // poll all low-priority worker-local queues, also sorted by distance
            for (int step = 1; step < po2WorkerCount; step++) {
                int neighborId;
                if ((step&1) == 1)
                    neighborId = (workerId + (1+(step>>1)))&workerMask;
                else
                    neighborId = (workerId - (step>>1))&workerMask;
                seq[i++] = handles[(neighborId<<1)+1];
            }
            seq[i++] = handles[shHandleIdx+1];
            assert i == seq.length;
        }
        overflowQueue = new MpscUnboundedAtomicArrayQueue<>(1024);
        Thread.startVirtualThread(this::addOverflown);
    }

    private boolean nonOverlappingQueues() {
        int used = PADDING;
        for (var h : handles) {
            if (h.base < used)
                return false;
            used = h.base + h.mask+1;
        }
        return used == data.length-PADDING;
    }

    private void addOverflown() {
        long nanos = 1_000_000_000L;
        //noinspection InfiniteLoopStatement
        while (true) {
            Task<?> task = overflowQueue.poll();
            if (task == null) {
                nanos = Math.min(1_000_000_000L, nanos*10);
                LockSupport.parkNanos(overflowQueue, nanos);
            } else {
                nanos = 100_000L;
                var shared = handles[task instanceof LowPriorityTask<?> ? 1 : 0];
                while (!shared.spinOffer(task)) {
                    nanos = Math.min(100_000_000L, nanos*10);
                    LockSupport.parkNanos(this, nanos);
                }
            }
        }
    }

    public StringBuilder dump(StringBuilder sb) {
        sb.append("shared:    ").append(handles[shHandleIdx  ].size()).append('\n');
        sb.append("shared LP: ").append(handles[shHandleIdx+1].size()).append('\n');
        for (int workerId = 0; workerId <= workerMask; workerId++) {
            var prefix = String.format("worker %2d", workerId);
            sb.append(prefix).append(":    ").append(handles[ workerId<<1   ]).append('\n');
            sb.append(prefix).append(" LP: ").append(handles[(workerId<<1)+1]).append('\n');
        }
        return sb;
    }

    private void putOverflow(Task<?> task) {
        while (!overflowQueue.offer(task))
            Thread.yield();
    }

    public void put(Task<?> task, int workerId) {
        int type = task instanceof LowPriorityTask<?> ? 1 : 0;
        if (handles[((workerId&workerMask)<<1) + type].spinOffer(task))
            return;
       if (!handles[shHandleIdx + type].spinOffer(task))
            putOverflow(task);
    }

    public int sharedQueueSize() {
        return handles[shHandleIdx].size() + handles[shHandleIdx+1].size();
    }

    public Task<?> take(Worker workerThread) {
        int workerId = workerThread.workerId;
        Task<?> task;
        var pollSeq = this.pollSeq[workerId];
        while (true) {
            for (var h : pollSeq)
                if ((task=h.spinPoll()) != null) return task;
            workerThread.park();
        }
    }
}
