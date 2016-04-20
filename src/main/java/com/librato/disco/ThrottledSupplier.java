package com.librato.disco;

import java.util.concurrent.TimeUnit;

public class ThrottledSupplier<T> {
    interface Supplier<T> {
        T get();
    }

    final long callEvery;
    final TimeUnit callEveryUnit;
    final Supplier<T> delegate;
    long lastGetAt;
    T lastValue;

    public ThrottledSupplier(long callEvery, TimeUnit callEveryUnit, Supplier<T> delegate) {
        this.callEvery = callEvery;
        this.callEveryUnit = callEveryUnit;
        this.delegate = delegate;
    }

    public synchronized T get() {
        long now = System.currentTimeMillis();
        if (now - callEveryUnit.toMillis(callEvery) >= lastGetAt) {
            lastValue = delegate.get();
            lastGetAt = now;
        }
        return lastValue;
    }
}
