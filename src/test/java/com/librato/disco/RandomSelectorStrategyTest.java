package com.librato.disco;

import com.google.common.collect.Lists;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.junit.Test;

import java.util.List;

import static junit.framework.Assert.assertNotNull;
import static org.mockito.Mockito.mock;

public class RandomSelectorStrategyTest {
    @Test
    public void testChoose() throws Exception {
        ChildData childA = mock(ChildData.class);
        ChildData childB = mock(ChildData.class);
        ChildData childC = mock(ChildData.class);
        List<ChildData> children = Lists.newArrayList(childA, childB, childC);

        final int numIterations = 1000;
        RandomSelectorStrategy strategy = new RandomSelectorStrategy();
        for (int i = 0; i < numIterations; i++) {
            assertNotNull(strategy.choose(children));
        }
    }
}
