package com.librato.disco;

import org.apache.curator.framework.recipes.cache.ChildData;

import java.util.List;
import java.util.Random;

/**
 * Selects a random node
 */
public class RandomSelectorStrategy implements SelectorStrategy {
    private final Random random = new Random();

    @Override
    public ChildData choose(List<ChildData> children) {
        return children.get(random.nextInt(children.size()));
    }
}
