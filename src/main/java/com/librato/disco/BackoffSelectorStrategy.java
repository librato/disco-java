package com.librato.disco;

import org.apache.curator.framework.recipes.cache.ChildData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Random;

/**
 * TODO: Document
 */
public class BackoffSelectorStrategy implements SelectorStrategy {
    private static final Logger log = LoggerFactory.getLogger(BackoffSelectorStrategy.class);
    private final Random rand = new Random();
    private final SelectorStrategy base;
    private final int percentage;

    private long threshold = 10000; // 10 seconds

    public BackoffSelectorStrategy(long thresholdMillis, int percentage) {
        this.threshold = thresholdMillis;
        this.percentage = percentage;
        this.base = new RoundRobinSelectorStrategy();
    }

    @Override
    public ChildData choose(List<ChildData> children) {
        while (true) {
            ChildData cd = base.choose(children);
            final long now = System.currentTimeMillis();
            final long cdTime = cd.getStat().getCtime();
            if (now - cdTime < threshold && rand.nextInt(100) > percentage) {
                continue;
            }
            return cd;
        }
    }
}
