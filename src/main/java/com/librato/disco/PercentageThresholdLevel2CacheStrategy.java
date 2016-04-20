package com.librato.disco;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * A level 2 cache strategy that promotes based on the percentage of l1 nodes
 * versus l2 nodes dropping below a threshold.
 */
public class PercentageThresholdLevel2CacheStrategy implements ILevel2CacheStrategy {
    private static final Logger log = LoggerFactory.getLogger(PercentageThresholdLevel2CacheStrategy.class);
    private final ThrottledRunner printPercentageRunner = new ThrottledRunner(5, TimeUnit.SECONDS);
    private final double threshold;
    private final long ttl;
    private final TimeUnit ttlUnit;

    public PercentageThresholdLevel2CacheStrategy(double threshold,
                                                  long l2CacheTtl,
                                                  TimeUnit l2CacheTtlUnit) {
        this.threshold = threshold;
        this.ttl = l2CacheTtl;
        this.ttlUnit = l2CacheTtlUnit;
    }

    @Override
    public boolean promote(final String serviceName, int l1CacheSize, int l2CacheSize, boolean isPromoted) {
        if (l1CacheSize == l2CacheSize && l1CacheSize > 0 && l2CacheSize > 0) {
            // we're fine.
            return false;
        }
        if (l2CacheSize == 0) {
            // all of the l2 cache entries have ttl'd out. keep the promoted state.
            return isPromoted;
        }
        // here we are guaranteed to not have a divide by zero error
        final double percentageOfL2CacheSize = (double) l1CacheSize / (double) l2CacheSize;
        printPercentageRunner.run(new Runnable() {
            @Override
            public void run() {
                printPercentage(serviceName, percentageOfL2CacheSize);
            }
        });
        if (l2CacheSize <= 2) {
            // special case. only promote when l1 cache size is zero
            return l1CacheSize == 0;
        }
        return percentageOfL2CacheSize <= threshold;
    }

    private synchronized void printPercentage(String serviceName, double percentage) {
        String formattedPercentage = String.format("%.2f", percentage);
        if (percentage < 1.0) {
            log.warn("Current percentage for {} is {} [threshold is {}]", serviceName, formattedPercentage, threshold);
        }
    }

    @Override
    public long getTtl() {
        return ttl;
    }

    @Override
    public TimeUnit getTtlTimeUnit() {
        return ttlUnit;
    }
}
