package com.librato.disco;

import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class PercentageThresholdLevel2CacheStrategyTest {
    String serviceName = "foo";

    @Test
    public void basicOperation() throws Exception {
        double threshold = 0.5;
        PercentageThresholdLevel2CacheStrategy strategy = new PercentageThresholdLevel2CacheStrategy(
                threshold,
                5, TimeUnit.MINUTES);

        // l1 size of 4 and l2 size of 4
        assertThat(strategy.promote(serviceName, 4, 4, false), equalTo(false));

        // remove one of the delegate datas, keep the l2 cache size of 4
        assertThat(strategy.promote(serviceName, 3, 4, false), equalTo(false));

        // remove one more, that should be 50%
        assertThat(strategy.promote(serviceName, 2, 4, false), equalTo(true));

        // verify that the strategy still thinks it should be promoted
        assertThat(strategy.promote(serviceName, 2, 4, false), equalTo(true));

        // edge case, still remove yet another one
        assertThat(strategy.promote(serviceName, 1, 4, true), equalTo(true));

        // restore to > 50 % verify it should not promote now
        assertThat(strategy.promote(serviceName, 3, 4, true), equalTo(false));

        // if both are zero, edge case, keep existing promotion status
        assertThat(strategy.promote(serviceName, 0, 0, true), equalTo(true));
        assertThat(strategy.promote(serviceName, 0, 0, false), equalTo(false));
    }
}
