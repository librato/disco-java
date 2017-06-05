package com.librato.disco;


import org.apache.curator.framework.recipes.cache.ChildData;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Selects a node in round-robin fashion per thread
 */
public class ThreadLocalRoundRobinSelectorStrategy implements SelectorStrategy {
    private final ThreadLocal<Long> idx = ThreadLocal.withInitial(() -> 0L);

    @Override
    public ChildData choose(List<ChildData> children) {
        Long currentIdx = idx.get();
        idx.set(currentIdx+1);
        return children.get((int) (currentIdx % children.size()));
    }
}
