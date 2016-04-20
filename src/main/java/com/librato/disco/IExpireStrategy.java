package com.librato.disco;

import org.apache.curator.framework.recipes.cache.ChildData;

public interface IExpireStrategy {
    /**
     * Whether or not a certain child data should be expired
     * based on it's expire time.
     */
    boolean shouldExpire(ChildData data, long expireAtMillis);
}
