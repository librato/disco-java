package com.librato.disco;

import java.util.concurrent.TimeUnit;

class FakeLevel2CacheStrategy implements ILevel2CacheStrategy {
    long ttl;
    TimeUnit ttlUnit;
    boolean promote;

    public FakeLevel2CacheStrategy(long ttl, TimeUnit ttlUnit) {
        this.ttl = ttl;
        this.ttlUnit = ttlUnit;
    }

    @Override
    public boolean promote(String serviceName, int l1CacheSize, int l2CacheSize, boolean isPromoted) {
        return promote;
    }

    @Override
    public long getTtl() {
        return ttl;
    }

    @Override
    public TimeUnit getTtlTimeUnit() {
        return ttlUnit;
    }

    public void setPromote(boolean promote) {
        this.promote = promote;
    }

    public void setTtl(long ttl, TimeUnit unit) {
        this.ttl = ttl;
        this.ttlUnit = unit;
    }
}
