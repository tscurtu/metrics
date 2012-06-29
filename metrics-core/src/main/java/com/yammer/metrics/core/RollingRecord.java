package com.yammer.metrics.core;

import java.util.LinkedList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A counter that records the absolute number of events that have occurred in the last
 * specified time buckets.
 */
public class RollingRecord extends Counter {
    private LinkedList<Long> eventBuckets;
    private AtomicLong total;
    private long buckets;

    /**
     * Creates a new {@link RollingRecord}.
     *
     * @param buckets    the buckets for which this counter will record events
     * @param timeUnit    the time unit of the forementioned buckets
     * @param secondsTick the update buckets in seconds
     */
    RollingRecord(long buckets, TimeUnit timeUnit, long secondsTick) {
        this.eventBuckets = new LinkedList<Long>();
        this.total = new AtomicLong(0L);
        this.buckets = timeUnit.toMillis(buckets) / secondsTick / 1000L;

        if (this.buckets <= 0) {
            throw new IllegalArgumentException("Invalid buckets-tick combination");
        }
    }

    public synchronized void tick() {
        if (eventBuckets.size() + 1 >= buckets) {
            total.addAndGet(-eventBuckets.removeLast());
        }
        eventBuckets.addFirst(count.getAndSet(0L));
    }

    @Override
    public void inc(long n) {
        count.addAndGet(n);
        total.addAndGet(n);
    }

    @Override
    public void dec(long n) {
        count.addAndGet(-n);
        total.addAndGet(-n);
    }

    @Override
    public long getCount() {
        return total.get();
    }

    @Override
    public synchronized void clear() {
        count.set(0L);
        total.set(0L);
        eventBuckets.clear();
    }
}
