package com.librato.disco;


import org.apache.curator.framework.recipes.cache.ChildData;

import java.util.List;

/**
 * Interface to select a node from set of nodes
 */
public interface SelectorStrategy {
    ChildData choose(List<ChildData> children);
}
