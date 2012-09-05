package com.yammer.metrics.core;

import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A timer that records the absolute number of events that have occurred in the recent past.
 */
public class RollingTimer extends Timer {
    protected AbsoluteMeter meter; // keep a casted copy here

    /**
     * Creates a new {@link RollingTimer}.
     *
     * @param eventType    the event type name
     * @param durationUnit the scale unit for this timer's duration metrics
     * @param clock        the clock used to calculate duration
     */
    RollingTimer(String eventType, TimeUnit durationUnit, Clock clock) {
        super(new AbsoluteMeter(eventType, clock), durationUnit, clock);
        meter = (AbsoluteMeter) super.meter;
    }

    /**
     * Creates a new {@link RollingTimer}.
     *
     * @param tickThread   background thread for updating the rates
     * @param eventType    the event type name
     * @param durationUnit the scale unit for this timer's duration metrics
     * @param clock        the clock used to calculate duration
     */
    RollingTimer(ScheduledExecutorService tickThread, String eventType, TimeUnit durationUnit, Clock clock) {
        super(new AbsoluteMeter(tickThread, eventType, clock), durationUnit, clock);
        meter = (AbsoluteMeter) super.meter;
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

    public double getFifteenMinuteMax() {
        return meter.getOneMinuteMax();
    }

    @Override
    public double getFiveMinuteRate() {
        return meter.getFiveMinuteRate();
    }

    public double getFiveMinuteMax() {
        return meter.getOneMinuteMax();
    }

    @Override
    public double getOneMinuteRate() {
        return meter.getOneMinuteRate();
    }

    public double getOneMinuteMax() {
        return meter.getOneMinuteMax();
    }

    void tick() {
        meter.tick();
    }
}
