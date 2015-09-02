package com.librato.disco;

import com.google.common.collect.Lists;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class RoundRobinSelectorStrategyTest {
    @Test
    public void testChoose() throws Exception {
        ChildData childA = mock(ChildData.class);
        ChildData childB = mock(ChildData.class);
        ChildData childC = mock(ChildData.class);
        List<ChildData> children = Lists.newArrayList(childA, childB, childC);

        final int numIterations = 1000;
        RoundRobinSelectorStrategy strategy = new RoundRobinSelectorStrategy();
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
}
