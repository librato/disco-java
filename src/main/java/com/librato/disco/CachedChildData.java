package com.librato.disco;

import org.apache.curator.framework.recipes.cache.ChildData;

/**
 * Composes a child data and an expire-at ttl. This is used in a
 * map of path -> cached child data in order to keep a relatively
 * up to date view of the world in the event of an outage.
 */
class CachedChildData {
    private ChildData data;
    private long expireAt;

    public CachedChildData(ChildData data, long expireAt) {
        this.data = data;
        this.expireAt = expireAt;
    }

    public synchronized ChildData getData() {
        return data;
    }

    public synchronized  CachedChildData setData(ChildData data) {
        this.data = data;
        return this;
    }

    public synchronized long getExpireAt() {
        return expireAt;
    }

    public synchronized CachedChildData setExpireAt(long expireAt) {
        this.expireAt = expireAt;
        return this;
    }
}
