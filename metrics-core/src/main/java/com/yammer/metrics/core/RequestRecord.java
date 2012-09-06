package com.yammer.metrics.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * A record of requests. Counts failed and successful requests and times them.
 * <p>
 * Logs individual request metrics.
 */
public class RequestRecord {
    private static final long SECONDS_IN_MINUTE = TimeUnit.MINUTES.toSeconds(1L);
    private static final long MILLIS_IN_SECOND = TimeUnit.SECONDS.toMillis(1L);
    private static final long TICK_INTERVAL = 1L; // 1 second
    private static final String TOTAL_REQUESTS = "total_requests";
    private static final String SERVED_REQUESTS = "served_requests";
    private static final String FAILED_REQUESTS = "failed_requests";
    private static final String CONCURRENT_TIMER = "concurrent_timer";

    private final Logger LOG = LoggerFactory.getLogger(this.getClass().getName());

    private Clock clock;
    private MetricsRegistry metricsRegistry;
    private RollingRecord totalRequests;
    private RollingRecord servedRequests;
    private RollingRecord failedRequests;
    private RollingTimer concurrentTimer;
    private ThreadLocal<ReusableTimerContext> timerContext;
    private long lastMinute;

    public RequestRecord() {
        clock = Clock.defaultClock();
        metricsRegistry = new MetricsRegistry();
        totalRequests = new RollingRecord(1L, TimeUnit.MINUTES);
        servedRequests = new RollingRecord(1L, TimeUnit.MINUTES);
        failedRequests = new RollingRecord(1L, TimeUnit.MINUTES);
        concurrentTimer = metricsRegistry.getOrAdd(new MetricName(RollingTimer.class, CONCURRENT_TIMER),
            new RollingTimer(CONCURRENT_TIMER, TimeUnit.MILLISECONDS, clock));
        timerContext = new ThreadLocal<ReusableTimerContext>() {
                @Override
                protected ReusableTimerContext initialValue() {
                    return new ReusableTimerContext(concurrentTimer, clock);
                }
            };
        lastMinute = 0L;

        // schedule timer tick
        metricsRegistry.newMeterTickThreadPool().scheduleAtFixedRate(new Runnable() {
              @Override
              public void run() {
                  totalRequests.tick();
                  servedRequests.tick();
                  failedRequests.tick();
                  concurrentTimer.tick();

                  // log the metrics
                  long ts = clock.getTime() / MILLIS_IN_SECOND;
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
     * Starts a new request timer.
     *
     * @return the cock tick in nanoseconds
     */
    public long start() {
        return timerContext.get().start();
    }

    /**
     * Marks the request as successfully served and stops the timer.
     *
     * @return the request duration in nanoseconds
     */
    public long markServed() {
        servedRequests.inc();
        totalRequests.inc();
        return timerContext.get().stop(); // updates concurrentTimer
    }

    /**
     * Marks the request as successfully served and stops the timer.
     *
     * @return the request duration in nanoseconds
     */
    public long markFailed() {
        failedRequests.inc();
        totalRequests.inc();
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
        return totalRequests.getCount();
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
        return (totalRequests.getCount() == 0L) ? 0D : concurrentTimer.getOneMinuteRate() / totalRequests.getCount();
    }

    /**
     * A reusable {@link TimerContext}.
     */
    protected class ReusableTimerContext extends TimerContext {
        public ReusableTimerContext(Timer timer, Clock clock) {
            super(timer, clock);
        }

        /**
         * Starts a the timer.
         *
         * @return the cock tick in nanoseconds
         */
        public long start() {
            return startTime = clock.getTick();
        }
    }
}
