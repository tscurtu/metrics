package com.yammer.metrics.core;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A meter that provides the absolute number of events that have occurred in the last 1, 5 and 15 minutes.
 */
public class AbsoluteMeter extends Meter {
    private final RollingRecord am1Record;
    private final RollingRecord am5Record;
    private final RollingRecord am15Record;

    AbsoluteMeter(ScheduledExecutorService tickThread, String eventType, Clock clock) {
        super(tickThread, eventType, TimeUnit.MINUTES,  clock);
        this.am1Record = new RollingRecord(1, TimeUnit.MINUTES, INTERVAL);
        this.am5Record = new RollingRecord(5, TimeUnit.MINUTES, INTERVAL);
        this.am15Record = new RollingRecord(15, TimeUnit.MINUTES, INTERVAL);
    }

    @Override
    void tick() {
        am1Record.tick();
        am5Record.tick();
        am15Record.tick();
    }

    @Override
    public void mark(long n) {
        count.addAndGet(n);
        am1Record.inc(n);
        am5Record.inc(n);
        am15Record.inc(n);
    }

    @Override
    public double getFifteenMinuteRate() {
        return am15Record.getCount();
    }

    @Override
    public double getFiveMinuteRate() {
        return am5Record.getCount();
    }

    @Override
    public double getOneMinuteRate() {
        return am1Record.getCount();
    }
}
