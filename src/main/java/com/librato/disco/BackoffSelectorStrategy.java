package com.librato.disco;

import org.apache.curator.framework.recipes.cache.ChildData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Random;

/**
 * This selector strategy attempts to backoff a node given its creation timestamp for a specified period, allowing
 * a percentage through to the node (low effort on enforcing percentage)
 */
public class BackoffSelectorStrategy implements SelectorStrategy {
    private static final Logger log = LoggerFactory.getLogger(BackoffSelectorStrategy.class);
    private final Random rand = new Random();
    private final SelectorStrategy base;
    private final int percentage;
    private final long period; // millis

    public BackoffSelectorStrategy(long thresholdMillis, int percentage) {
        // NOTE: RandomSelectorStrategy is recommended as a base because otherwise the spillover to the next node
        // would cause a substantial imbalance to that node relative to the others
        this(new RandomSelectorStrategy(), thresholdMillis, percentage);
    }

    public BackoffSelectorStrategy(SelectorStrategy base, long periodMillis, int percentage) {
        this.base = base;
        this.period = periodMillis;
        this.percentage = percentage;
    }

    @Override
    public ChildData choose(List<ChildData> children) {
        ChildData cd = null;
        final int maxIters = children.size();
        for (int i = 0; i < maxIters; i++) {
            cd = base.choose(children);
            final long now = System.currentTimeMillis();
            final long cdTime = cd.getStat().getCtime();
            if (now - cdTime > period || rand.nextInt(100) <= percentage) {
                break;
            }
        }
        return cd;
    }
}
