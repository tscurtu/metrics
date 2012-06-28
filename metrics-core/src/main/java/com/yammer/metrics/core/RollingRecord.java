package com.yammer.metrics.core;

import java.util.LinkedList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A counter that records the absolute number of events that have occurred in the last
 * specified time interval.
 */
public class RollingRecord extends Counter {
    private LinkedList<Long> eventBuckets;
    private AtomicLong total;
    private long interval;

    /**
     * Creates a new {@link RollingRecord}.
     *
     * @param interval    the interval for which this counter will record events
     * @param timeUnit    the time unit of the forementioned interval
     * @param secondsTick the update interval in seconds
     */
    RollingRecord(long interval, TimeUnit timeUnit, long secondsTick) {
        this.eventBuckets = new LinkedList<Long>();
        this.total = new AtomicLong(0L);
        this.interval = timeUnit.toMillis(interval) / secondsTick / 1000L;

        if (this.interval <= 0) {
            throw new IllegalArgumentException("Invalid interval-tick combination");
        }
    }

    public synchronized void tick() {
        if (eventBuckets.size() + 1 >= interval) {
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
