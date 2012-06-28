package com.yammer.metrics.core;

import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A timer that records the absolute number of events that have occurred in the recent past.
 */
public class RollingTimer extends Timer {
    private AtomicLong concurrentCalls;

    /**
     * Creates a new {@link RollingTimer}.
     *
     * @param tickThread   background thread for updating the rates
     * @param durationUnit the scale unit for this timer's duration metrics
     */
    RollingTimer(ScheduledExecutorService tickThread, TimeUnit durationUnit) {
        this(tickThread, durationUnit, Clock.defaultClock());
    }

    /**
     * Creates a new {@link RollingTimer}.
     *
     * @param tickThread   background thread for updating the rates
     * @param durationUnit the scale unit for this timer's duration metrics
     * @param clock        the clock used to calculate duration
     */
    RollingTimer(ScheduledExecutorService tickThread, TimeUnit durationUnit, Clock clock) {
        super(new AbsoluteMeter(tickThread, "calls", clock), durationUnit, clock);
        this.concurrentCalls = new AtomicLong(0L);
    }

    @Override
    public void clear() {
        super.clear();
        concurrentCalls.set(0L);
    }

    @Override
    public TimerContext time() {
        concurrentCalls.incrementAndGet();
        return super.time();
    }

    @Override
    protected void update(long duration) {
        duration = (long) convertFromNS(duration);
        if (duration >= 0) {
            histogram.update(duration);
        }
        meter.mark(duration);
    }

    @Override
    public <T> T time(Callable<T> event) throws Exception {
        concurrentCalls.incrementAndGet();
        final long startTime = clock.getTick();
        try {
            return event.call();
        } finally {
            update(clock.getTick() - startTime);
        }
    }

    @Override
    public double getFifteenMinuteRate() {
        return meter.getFifteenMinuteRate();
    }

    @Override
    public double getFiveMinuteRate() {
        return meter.getFiveMinuteRate();
    }

    @Override
    public double getOneMinuteRate() {
        return meter.getOneMinuteRate();
    }
}
