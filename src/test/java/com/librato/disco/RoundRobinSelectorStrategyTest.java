package com.librato.disco;

import com.google.common.collect.Lists;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static java.time.temporal.ChronoUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;
import static org.mockito.internal.verification.VerificationModeFactory.atLeast;

public class RoundRobinSelectorStrategyTest {
    @Test
    public void testChoose() throws Exception {
        ChildData childA = mock(ChildData.class);
        ChildData childB = mock(ChildData.class);
        ChildData childC = mock(ChildData.class);
        List<ChildData> children = Lists.newArrayList(childA, childB, childC);

        final int numIterations = 1000;
        SelectorStrategy strategy = new RoundRobinSelectorStrategy();
        for (int i = 0; i < numIterations; i++) {
            ChildData child = strategy.choose(children);
            int mod = i % children.size();
            if (mod == 0) {
                assertEquals(childA, child);
            } else if (mod == 1) {
                assertEquals(childB, child);
            } else if (mod == 2) {
                assertEquals(childC, child);
            }
        }
    }

    @Test
    public void testChooseBaselineFixedPeriod() throws Exception {
        testThreadedExecution(new RoundRobinSelectorStrategy(),10, 3, () -> { return 100L; });
    }

    @Test
    public void testChooseThreadLocalFixedPeriod() throws Exception {
        testThreadedExecution(new ThreadLocalRoundRobinSelectorStrategy(),10, 3, () -> { return 100L; });
    }

    @Test
    public void testChooseThreadLocalVariablePeriod() throws Exception {
        testThreadedExecution(new ThreadLocalRoundRobinSelectorStrategy(),100, 30, new VariablePeriodSupplier(100, 100));
    }

    @Test
    public void testChooseThreadLocalRandomPeriod() throws Exception {
        testThreadedExecution(new ThreadLocalRoundRobinSelectorStrategy(),100, 30, new RandomPeriodSupplier(100, 10000));
    }

    public class VariablePeriodSupplier implements Supplier<Long> {
        private long period;
        private long increment;

        VariablePeriodSupplier(long startingValue, long increment) {
            this.period = startingValue;
            this.increment = increment;
        }
        @Override
        public Long get() {
            long lastPeriod = period;
            period += increment;
            return lastPeriod;
        }
    }

    public class RandomPeriodSupplier implements Supplier<Long> {
        private final PrimitiveIterator.OfLong random;

        RandomPeriodSupplier(long lowerBound, long upperBound) {
            random = new Random().longs(lowerBound, upperBound).iterator();
        }

        @Override
        public Long get() {
            return random.nextLong();
        }
    }

    private void testThreadedExecution(SelectorStrategy strategy, int numThreads, int numChildren, Supplier<Long> periodChooser) throws Exception {
        List<ChildData> children = new ArrayList<>(numChildren);
        final List<AtomicInteger> counts = new ArrayList<>(numChildren);
        final List<ScheduledFuture> futureList = new LinkedList<>();

        for (int i=0; i<numChildren; i++) {
            ChildData childData = mock(ChildData.class);
            children.add(childData);
            final AtomicInteger ai = new AtomicInteger(0);
            final String returnValue = "";
            counts.add(ai);
            when(childData.getPath()).then(invocation -> {
                ai.incrementAndGet();
                return returnValue;
            });
        }

        Runnable runnable = () -> {
            ChildData child = strategy.choose(children);
            child.getPath();
        };

        Instant now = Instant.now();
        Instant future = now.plus(1, SECONDS);
        for (int i=0; i<numThreads; i++) {
            long initialDelay = Duration.between(future, now).toNanos();
            if (initialDelay < 0) {
                initialDelay = 0;
            }
            long period = periodChooser.get();
            futureList.add(createThread(runnable, initialDelay, period));
        }

        try {
            Thread.sleep(1000);
        } catch (InterruptedException ignored) {}

        futureList.forEach(f -> f.cancel(true));

        int totalInvocations = counts.stream().mapToInt(AtomicInteger::get).sum();
        int expected = (int) (totalInvocations / children.size() * .9);

        children.forEach(mocked -> {
            verify(mocked, atLeast(expected)).getPath();
        });
    }

    private ScheduledFuture<?> createThread(Runnable command, long initialDelay, long period) {
        final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        return scheduler.scheduleAtFixedRate(command, initialDelay, period, TimeUnit.NANOSECONDS);
    }
}
