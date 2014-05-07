package com.librato.disco;


import org.apache.curator.framework.recipes.cache.ChildData;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Selects a node in round-robin fashion
 */
public class RoundRobinSelectorStrategy implements SelectorStrategy {
    private final AtomicLong idx = new AtomicLong(0);

    @Override
    public ChildData choose(List<ChildData> children) {
        return children.get((int) (idx.getAndIncrement() % children.size()));
    }
}
