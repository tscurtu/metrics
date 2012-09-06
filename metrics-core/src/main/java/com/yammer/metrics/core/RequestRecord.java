package com.yammer.metrics.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A record of requests. Counts the failed and successful requests that have occurred during the last minute and times
 * them.
 * <p>
 * Logs individual request metrics.
 */
public class RequestRecord implements Metric {
    private static final long SECONDS_IN_MINUTE = TimeUnit.MINUTES.toSeconds(1L);
    private static final long MILLIS_IN_SECOND = TimeUnit.SECONDS.toMillis(1L);
    private static final long TICK_INTERVAL = 1L; // 1 second
    private static final String TOTAL_REQUESTS = "total_requests";
    private static final String SERVED_REQUESTS = "served_requests";
    private static final String FAILED_REQUESTS = "failed_requests";
    private static final String CONCURRENT_TIMER = "concurrent_timer";

    private final Logger LOG = LoggerFactory.getLogger(this.getClass().getName());
    private final Clock clock;
    private final RollingRecord servedRequests;
    private final RollingRecord failedRequests;
    private final RollingTimer concurrentTimer;
    private final ThreadLocal<ReusableTimerContext> timerContext;

    private long lastMinute;

    /**
     * Creates a nre request record object.
     *
     * @param tickThread background thread for updating the rates
     * @param name       the record name
     * @param clock      the clock used to calculate duration
     */
    RequestRecord(ScheduledExecutorService tickThread, String name, Clock clock) {
        this.clock = Clock.defaultClock();
        this.servedRequests = new RollingRecord(1L, TimeUnit.MINUTES);
        this.failedRequests = new RollingRecord(1L, TimeUnit.MINUTES);
        this.concurrentTimer = new RollingTimer(CONCURRENT_TIMER, TimeUnit.MILLISECONDS, clock);
        this.timerContext = new ThreadLocal<ReusableTimerContext>() {
                @Override
                protected ReusableTimerContext initialValue() {
                    return new ReusableTimerContext(concurrentTimer, RequestRecord.this.clock);
                }
            };
        this.lastMinute = 0L;

        // schedule timer tick
        tickThread.scheduleAtFixedRate(new Runnable() {
              @Override
              public void run() {
                  servedRequests.tick();
                  failedRequests.tick();
                  concurrentTimer.tick();

                  // log the metrics
                  long ts = RequestRecord.this.clock.getTime() / MILLIS_IN_SECOND;
                  long thisMinute = ts / SECONDS_IN_MINUTE;
                  if (thisMinute > lastMinute) {
                      lastMinute = thisMinute;
                      if (LOG.isInfoEnabled()) {
                          LOG.info("{}.1m {} {}", new Object[] {TOTAL_REQUESTS, ts, getLastMinuteTotalCount()});
                          LOG.info("{}.1m {} {}", new Object[] {SERVED_REQUESTS, ts, getLastMinuteServedCount()});
                          LOG.info("{}.1m {} {}", new Object[] {FAILED_REQUESTS, ts, getLastMinuteFailedCount()});
                          LOG.info("{}.total.1m {} {}", new Object[] {CONCURRENT_TIMER, ts, getLastMinuteTotalTime()});
                          LOG.info("{}.max.1m {} {}", new Object[] {CONCURRENT_TIMER, ts, getLastMinuteMaxTime()});
                          LOG.info("{}.avg.1m {} {}", new Object[] {CONCURRENT_TIMER, ts, getLastMinuteAvgTime()});
                      }
                  }
              }
          }, TICK_INTERVAL, TICK_INTERVAL, TimeUnit.SECONDS);
    }

    /**
     * Returns the internal timer's duration unit.
     *
     * @return this request record's duration unit
     */
    public TimeUnit getDurationUnit() {
        return concurrentTimer.getDurationUnit();
    }

    /**
     * Starts a new request timer.
     *
     * @return the cock tick in nanoseconds
     */
    public long start() {
        return timerContext.get().start(); // updates concurrentTimer
    }

    /**
     * Marks the request as successfully served and stops the timer.
     *
     * @return the request duration in nanoseconds
     */
    public long markServed() {
        servedRequests.inc();
        return timerContext.get().stop(); // updates concurrentTimer
    }

    /**
     * Marks the request as successfully served and stops the timer.
     *
     * @return the request duration in nanoseconds
     */
    public long markFailed() {
        failedRequests.inc();
        return timerContext.get().stop(); // updates concurrentTimer
    }

    /**
     * Returns the number of served requests in the last minute.
     *
     * @return the number of served requests in the last minute
     */
    public long getLastMinuteServedCount() {
        return servedRequests.getCount();
    }

    /**
     * Returns the number of failed requests in the last minute.
     *
     * @return the number of failed requests in the last minute
     */
    public long getLastMinuteFailedCount() {
        return failedRequests.getCount();
    }

    /**
     * Returns the number of total requests in the last minute.
     *
     * @return the number of total requests in the last minute
     */
    public long getLastMinuteTotalCount() {
        return getLastMinuteFailedCount() + getLastMinuteServedCount();
    }

    /**
     * Returns the total handling time in the last minute.
     *
     * @return the total handling time in the last minute
     */
    public double getLastMinuteTotalTime() {
        return concurrentTimer.getOneMinuteRate();
    }

    /**
     * Returns the maximum handling time in the last minute.
     *
     * @return the maximum handling time in the last minute
     */
    public double getLastMinuteMaxTime() {
        return concurrentTimer.getOneMinuteMax();
    }

    /**
     * Returns the average handling time in the last minute.
     *
     * @return the average handling time in the last minute
     */
    public double getLastMinuteAvgTime() {
        long totalCount = getLastMinuteTotalCount();
        return (totalCount == 0L) ? 0D : concurrentTimer.getOneMinuteRate() / totalCount;
    }

    @Override
    public <T> void processWith(MetricProcessor<T> processor, MetricName name, T context) throws Exception {
        processor.processRequestRecord(name, this, context);
    }


    /**
     * A reusable {@link TimerContext}.
     */
    protected class ReusableTimerContext extends TimerContext {
        public ReusableTimerContext(Timer timer, Clock clock) {
            super(timer, clock);
        }

        /**
         * Starts the timer.
         *
         * @return the clock tick in nanoseconds
         */
        public long start() {
            return startTime = clock.getTick();
        }
    }
}
