package com.librato.disco;

import org.apache.curator.framework.recipes.cache.ChildData;

import java.util.List;

/**
 * The basic interface around being able to query for nodes. Note that
 * implementations of this interface should not typically block for data
 * as the methods that may call them could be synchronized.
 */
public interface IStateCache {
    List<ChildData> getCurrentData();

    void start() throws Exception;

    void stop() throws Exception;
}
