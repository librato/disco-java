package com.librato.disco;

import com.google.common.collect.Lists;
import junit.framework.TestCase;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.zookeeper.data.Stat;

import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BackoffSelectorStrategyTest extends TestCase {

    public void testChoose() throws Exception {
        ChildData childA = mock(ChildData.class);
        ChildData childC = mock(ChildData.class);
        List<ChildData> children = Lists.newArrayList(childA, childC);

        SelectorStrategy backoff = new BackoffSelectorStrategy(10000, 10);

        Stat oldStat = mock(Stat.class);
        when(oldStat.getCtime()).thenReturn(System.currentTimeMillis() - 10001);
        when(childA.getStat()).thenReturn(oldStat);

        Stat newStat = mock(Stat.class);
        when(newStat.getCtime()).thenReturn(System.currentTimeMillis());
        when(childC.getStat()).thenReturn(newStat);

        int olderChosen = 0;
        int newerChosen = 0;

        Integer total = 100000;
        for (int i = 0; i < total; i++) {
            ChildData child = backoff.choose(children);
            if (child == childA) {
                olderChosen++;
            } else if (child == childC) {
                newerChosen++;
            } else {
                throw new IllegalStateException();
            }
        }

        /*
        given the backing round-robin strategy, childC should be chosen 50%
        of all invocations but should only be returned in approximately 10%
        of those cases. Expected percentage is 5, but given the use of random
        numbers, assert less than 6.
        */
        double pct = (newerChosen / total.doubleValue());
        assertTrue(pct < .06);
    }
}