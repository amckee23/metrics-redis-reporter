package com.amckee.metrics.redis;

import com.codahale.metrics.*;
import redis.clients.jedis.Jedis;

import java.util.Locale;
import java.util.Map;
import java.util.SortedMap;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import static com.amckee.metrics.redis.constants.RedisReporterConstants.*;

/**
 * A reporter which outputs measurements to a redis cache.
 */
public class RedisReporter extends ScheduledReporter {
    /**
     * Returns a new {@link Builder} for {@link RedisReporter}.
     *
     * @param registry the registry to report
     * @return a {@link Builder} instance for a {@link RedisReporter}
     */
    public static Builder forRegistry(MetricRegistry registry) {
        return new Builder(registry);
    }

    /**
     * A builder for {@link RedisReporter} instances. Defaults to using the default locale and
     * time zone, writing to a redis cache, converting rates to events/second, converting
     * durations to milliseconds, and not filtering metrics.
     */
    public static class Builder {
        private final MetricRegistry registry;
        private Jedis output;
        private Locale locale;
        private Clock clock;
        private TimeZone timeZone;
        private TimeUnit rateUnit;
        private TimeUnit durationUnit;
        private MetricFilter filter;
        private String host;
        private int port;

        private Builder(MetricRegistry registry) {
            this.registry = registry;
            this.host = "localhost";
            this.port = 6379;
            this.output = new Jedis(host, port);
            this.locale = Locale.getDefault();
            this.clock = Clock.defaultClock();
            this.timeZone = TimeZone.getDefault();
            this.rateUnit = TimeUnit.SECONDS;
            this.durationUnit = TimeUnit.MILLISECONDS;
            this.filter = MetricFilter.ALL;
        }

        /**
         * Format numbers for the given {@link Locale}.
         *
         * @param locale a {@link Locale}
         * @return {@code this}
         */
        public Builder formattedFor(Locale locale) {
            this.locale = locale;
            return this;
        }

        /**
         * Use the given {@link Clock} instance for the time.
         *
         * @param clock a {@link Clock} instance
         * @return {@code this}
         */
        public Builder withClock(Clock clock) {
            this.clock = clock;
            return this;
        }

        /**
         * Use the given {@link TimeZone} for the time.
         *
         * @param timeZone a {@link TimeZone}
         * @return {@code this}
         */
        public Builder formattedFor(TimeZone timeZone) {
            this.timeZone = timeZone;
            return this;
        }

        /**
         * Convert rates to the given time unit.
         *
         * @param rateUnit a unit of time
         * @return {@code this}
         */
        public Builder convertRatesTo(TimeUnit rateUnit) {
            this.rateUnit = rateUnit;
            return this;
        }

        /**
         * Convert durations to the given time unit.
         *
         * @param durationUnit a unit of time
         * @return {@code this}
         */
        public Builder convertDurationsTo(TimeUnit durationUnit) {
            this.durationUnit = durationUnit;
            return this;
        }


        /**
         * Only report metrics which match the given filter.
         *
         * @param filter a {@link MetricFilter}
         * @return {@code this}
         */
        public Builder filter(MetricFilter filter) {
            this.filter = filter;
            return this;
        }

        public Builder redisServer(String host, int port) {
            this.host = host;
            this.port = port;
            return this;
        }

        /**
         * Builds a {@link RedisReporter} with the given properties.
         *
         * @return a {@link RedisReporter}
         */
        public RedisReporter build() {
            return new RedisReporter(registry,
                    locale,
                    clock,
                    timeZone,
                    rateUnit,
                    durationUnit,
                    filter, host, port);
        }
    }


    private final Jedis output;


    private RedisReporter(MetricRegistry registry,
                          Locale locale,
                          Clock clock,
                          TimeZone timeZone,
                          TimeUnit rateUnit,
                          TimeUnit durationUnit,
                          MetricFilter filter,
                          String host,
                          int port) {
        super(registry, "redis-reporter", filter, rateUnit, durationUnit);
        this.output = new Jedis(host, port);

    }

    @Override
    public void report(SortedMap<String, Gauge> gauges,
                       SortedMap<String, Counter> counters,
                       SortedMap<String, Histogram> histograms,
                       SortedMap<String, Meter> meters,
                       SortedMap<String, Timer> timers) {



        if (!gauges.isEmpty()) {
            for (Map.Entry<String, Gauge> entry : gauges.entrySet()) {
                printGauge(entry.getKey(), entry);
            }
        }

        if (!counters.isEmpty()) {
            for (Map.Entry<String, Counter> entry : counters.entrySet()) {
                printCounter(entry.getKey(), entry);
            }
        }

        if (!histograms.isEmpty()) {
            for (Map.Entry<String, Histogram> entry : histograms.entrySet()) {
                printHistogram(entry.getKey(), entry.getValue());
            }
        }

        if (!meters.isEmpty()) {
            for (Map.Entry<String, Meter> entry : meters.entrySet()) {
                printMeter(entry.getKey(), entry.getValue());
            }
        }

        if (!timers.isEmpty()) {
            for (Map.Entry<String, Timer> entry : timers.entrySet()) {
                printTimer(entry.getKey(), entry.getValue());
            }
        }
    }

    private void printMeter(String key, Meter meter) {
        output.set(key + COUNT_KEY, String.valueOf(meter.getCount()));
        output.set(key + MEAN_KEY, String.valueOf(convertRate(meter.getMeanRate())));
        output.set(key + ONE_MINUTE_KEY, String.valueOf(convertRate(meter.getOneMinuteRate())));
        output.set(key + FIVE_MINUTE_KEY, String.valueOf(convertRate(meter.getFiveMinuteRate())));
        output.set(key + FIFTEEN_MINUTE_KEY, String.valueOf(convertRate(meter.getFifteenMinuteRate())));
    }

    private void printCounter(String key, Map.Entry<String, Counter> entry) {
        output.set(key + COUNT_KEY, String.valueOf(entry.getValue().getCount()));
    }

    private void printGauge(String key, Map.Entry<String, Gauge> entry) {
        output.set(key + VALUE_KEY, String.valueOf(entry.getValue().getValue()));
    }

    private void printHistogram(String key, Histogram histogram) {
        output.set(key + COUNT_KEY, String.valueOf(histogram.getCount()));
        addSnapShotToCache(histogram.getSnapshot(), key);

    }

    private void printTimer(String key, Timer timer) {

        output.set(key + COUNT_KEY, String.valueOf(timer.getCount()));
        output.set(key + MEAN_KEY, String.valueOf(timer.getMeanRate()));
        output.set(key + ONE_MINUTE_KEY, String.valueOf(timer.getOneMinuteRate()));
        output.set(key + FIVE_MINUTE_KEY, String.valueOf(timer.getFiveMinuteRate()));
        output.set(key + FIFTEEN_MINUTE_KEY, String.valueOf(timer.getFifteenMinuteRate()));

        addSnapShotToCache(timer.getSnapshot(), key);

    }

    private void addSnapShotToCache(Snapshot snapshot, String key) {
        output.set(key + MIN_KEY, String.valueOf(snapshot.getMin()));
        output.set(key + MAX_KEY, String.valueOf(snapshot.getMax()));
        output.set(key + MEAN_KEY, String.valueOf(snapshot.getMean()));
        output.set(key + STDEV_KEY, String.valueOf(snapshot.getStdDev()));
        output.set(key + MEDIAN_KEY, String.valueOf(snapshot.getMedian()));
        output.set(key + PERCENTILE_75, String.valueOf(snapshot.get75thPercentile()));
        output.set(key + PERCENTILE_95, String.valueOf(snapshot.get95thPercentile()));
        output.set(key + PERCENTILE_98, String.valueOf(snapshot.get98thPercentile()));
        output.set(key + PERCENTILE_99, String.valueOf(snapshot.get99thPercentile()));
        output.set(key + PERCENTILE_999, String.valueOf(snapshot.get999thPercentile()));
    }
}