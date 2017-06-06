package com.librato.disco;

import org.apache.curator.framework.recipes.cache.ChildData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.stream.Collectors;

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
        this(new ThreadLocalRoundRobinSelectorStrategy(), thresholdMillis, percentage);
    }

    public BackoffSelectorStrategy(SelectorStrategy base, long periodMillis, int percentage) {
        this.base = base;
        this.period = periodMillis;
        this.percentage = percentage;
    }

    private boolean allow(ChildData cd) {
        // if this ChildData is old enough or falls into the requested percentile, allow to pass
        final long elapsed = System.currentTimeMillis() - cd.getStat().getCtime();
        return elapsed > period || rand.nextInt(100) <= percentage;
    }

    private ChildData fallbackChoose(List<ChildData> children, ChildData current) {
        // Create a randomly-ordered list of the remaining choices
        List<ChildData> otherChildren = children.stream()
                .filter(c -> c != current)
                .collect(Collectors.toList());
        Collections.shuffle(otherChildren);

        // From the remaining ChildData return the first allowed node, if found,
        // otherwise return the originally selected node
        Optional<ChildData> chosen = otherChildren.stream()
                .filter(this::allow)
                .findFirst();

        return chosen.orElse(current);
    }

    @Override
    public ChildData choose(List<ChildData> children) {
        ChildData cd = base.choose(children);
        if (allow(cd)) {
            return cd;
        }
        return fallbackChoose(children, cd);
    }
}
