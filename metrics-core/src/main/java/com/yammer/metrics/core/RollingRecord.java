package com.yammer.metrics.core;

import java.util.LinkedList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A counter that records the absolute number of events that have occurred in the last specified time buckets.
 * <p>
 * Can only be used with update granularities as small as seconds.
 */
public class RollingRecord extends Counter {
    private LinkedList<Long> totalBuckets;
    private LinkedList<Long> maxBuckets;
    private AtomicLong total;
    private AtomicLong latestTotal;
    private AtomicLong max;
    private AtomicLong latestMax;
    private long recordHistory;

    /**
     * Creates a new {@link RollingRecord}.
     *
     * @param recordHistory the number of time units in the past to store counted events
     * @param timeUnit      the record history time unit
     */
    RollingRecord(long recordHistory, TimeUnit timeUnit) {
        if (timeUnit.toSeconds(1L) <= 0L) {
            throw new IllegalArgumentException("This record can't be used with history time units finer than seconds");
        }

        this.totalBuckets = new LinkedList<Long>();
        this.maxBuckets = new LinkedList<Long>();
        this.total = new AtomicLong(0L);
        this.latestTotal = new AtomicLong(0L);
        this.max = new AtomicLong(0L);
        this.latestMax = new AtomicLong(0L);
        this.recordHistory = timeUnit.toSeconds(recordHistory);
    }

    public synchronized void tick() {
        // remove oldest bucket
        if (totalBuckets.size() + 1L >= recordHistory) {
            total.addAndGet(-totalBuckets.removeLast());
            long lastBucket = maxBuckets.removeLast();
            // the max value might change during the following operations
            // that's why we have to recheck
            long oldMax = max.get();
            if (lastBucket >= oldMax) {
                long newMax = latestMax.get();
                for (long l : maxBuckets) {
                    if (newMax < l) newMax = l;
                }
                max.compareAndSet(oldMax, newMax);
            }
        }
        // push latest buckets into the lists and reset them
        totalBuckets.addFirst(latestTotal.getAndSet(0L));
        maxBuckets.addFirst(latestMax.getAndSet(0L));
        total.addAndGet(totalBuckets.getFirst());
    }

    @Override
    public void inc(long n) {
        latestTotal.addAndGet(n);
        while (max.get() < n) max.set(n);
        while (latestMax.get() < n) latestMax.set(n);
    }

    @Override
    public void dec(long n) {
        latestTotal.addAndGet(-n);
        // foolproof! or is it?
        while (max.get() < -n) max.set(-n);
        while (latestMax.get() < -n) latestMax.set(-n);
    }

    @Override
    public long getCount() {
        return total.get();
    }

    public long getMax() {
        return max.get();
    }

    @Override
    public synchronized void clear() {
        total.set(0L);
        latestTotal.set(0L);
        max.set(0L);
        latestMax.set(0L);
        totalBuckets.clear();
        maxBuckets.clear();
    }
}
