package com.librato.disco;

import com.google.common.base.Preconditions;

import java.util.concurrent.atomic.AtomicBoolean;

public class StarterStopper {
    private final AtomicBoolean started = new AtomicBoolean();

    public void start() {
        Preconditions.checkArgument(started.compareAndSet(false, true));
    }

    public void stop() {
        Preconditions.checkArgument(started.compareAndSet(true, false));
    }

    public boolean isStarted() {
        return started.get();
    }
}
