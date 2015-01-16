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

        // We specify round robin here because it's easier to reason about in tests
        SelectorStrategy base = new RoundRobinSelectorStrategy();
        SelectorStrategy backoff = new BackoffSelectorStrategy(base, 10000, 10);

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

        long pct = Math.round(((newerChosen / total.doubleValue()) * 100));
        assertEquals(10, pct);
    }
}