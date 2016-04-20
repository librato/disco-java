package com.librato.disco;

import org.apache.curator.framework.recipes.cache.ChildData;

public class DefaultExpireStrategy implements IExpireStrategy {
    @Override
    public boolean shouldExpire(ChildData data, long expireAtMillis) {
        return expireAtMillis <= System.currentTimeMillis();
    }
}
