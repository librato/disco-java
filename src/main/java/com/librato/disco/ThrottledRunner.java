package com.librato.disco;

import java.util.concurrent.TimeUnit;

public class ThrottledRunner {
    private final long runOnlyEvery;
    private final TimeUnit runOnlyEveryUnit;
    private long lastRunAt;

    public ThrottledRunner(long runOnlyEvery, TimeUnit runOnlyEveryUnit) {
        this.runOnlyEvery = runOnlyEvery;
        this.runOnlyEveryUnit = runOnlyEveryUnit;
    }

    public synchronized void run(Runnable delegate) {
        long now = System.currentTimeMillis();
        if (now - runOnlyEveryUnit.toMillis(runOnlyEvery) >= lastRunAt) {
            delegate.run();
            lastRunAt = now;
        }
    }
}
