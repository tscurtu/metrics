package com.yammer.metrics.core;

import java.util.concurrent.TimeUnit;

/**
 * A timing context.
 *
 * @see Timer#time()
 */
public class TimerContext {
    protected final Timer timer;
    protected final Clock clock;
    protected long startTime;

    /**
     * Creates a new {@link TimerContext} with the current time as its starting value and with the
     * given {@link Timer}.
     *
     * @param timer the {@link Timer} to report the elapsed time to
     */
    TimerContext(Timer timer, Clock clock) {
        this.timer = timer;
        this.clock = clock;
        this.startTime = clock.getTick();
    }

    /**
     * Stops recording the elapsed time and updates the timer.
     *
     * @return the number of nanoseconds that have elapsed from the start time
     */
    public long stop() {
        long duration = clock.getTick() - startTime;
        timer.update(duration, TimeUnit.NANOSECONDS);
        return duration;
    }
}
