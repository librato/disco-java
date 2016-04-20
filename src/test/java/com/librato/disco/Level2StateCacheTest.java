package com.librato.disco;

import org.apache.curator.framework.recipes.cache.ChildData;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static com.librato.disco.FakeChildData.newData;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class Level2StateCacheTest {
    static final List<ChildData> noData = Collections.emptyList();
    String serviceName = "foo";

    @Test
    public void testDelegatesToL1Cache() throws Exception {
        FakeStateCache l1Cache = new FakeStateCache();
        FakeLevel2CacheStrategy strategy = new FakeLevel2CacheStrategy(10, SECONDS);
        Level2StateCache l2Cache = new Level2StateCache(serviceName, l1Cache, strategy);
        assertThat(l2Cache.getCurrentData(), equalTo(noData));

        ChildData c1 = newData("c1");

        l1Cache.add(c1);
        assertThat(l2Cache.getCurrentData(), equalTo(singletonList(c1)));

        l1Cache.remove(c1);
        assertThat(l2Cache.getCurrentData(), equalTo(noData));
    }

    @Test
    public void testPromotes() throws Exception {
        FakeStateCache l1Cache = new FakeStateCache();
        FakeLevel2CacheStrategy strategy = new FakeLevel2CacheStrategy(10, SECONDS);
        Level2StateCache l2Cache = new Level2StateCache(serviceName, l1Cache, strategy);
        assertThat(l2Cache.getCurrentData(), equalTo(noData));

        ChildData c1 = newData("c1");
        ChildData c2 = newData("c2");

        l1Cache.add(c1, c2);
        assertThat(l2Cache.getCurrentData(), equalTo(asList(c1, c2)));

        // verify that during promotion no changes to the l1 cache propagate
        strategy.setPromote(true);
        l1Cache.clear();
        assertThat(l1Cache.getCurrentData(), equalTo(noData));
        assertThat(l2Cache.getCurrentData(), equalTo(asList(c1, c2)));

        ChildData c3 = newData("c3");
        ChildData c4 = newData("c4");
        l1Cache.add(c2, c3, c4);
        assertThat(l1Cache.getCurrentData(), equalTo(asList(c2, c3, c4)));
        assertThat(l2Cache.getCurrentData(), equalTo(asList(c1, c2)));

        strategy.setPromote(false);
        assertThat(l1Cache.getCurrentData(), equalTo(asList(c2, c3, c4)));
        assertThat(l2Cache.getCurrentData(), equalTo(asList(c2, c3, c4)));

        l1Cache.remove(c4);
        assertThat(l1Cache.getCurrentData(), equalTo(asList(c2, c3)));
        assertThat(l2Cache.getCurrentData(), equalTo(asList(c2, c3)));
    }

    @Test
    public void expiresEntries() throws Exception {
        FakeStateCache l1Cache = new FakeStateCache();
        FakeLevel2CacheStrategy strategy = new FakeLevel2CacheStrategy(10, SECONDS);
        IExpireStrategy expireStrategy = mock(IExpireStrategy.class);
        Level2StateCache l2Cache = new Level2StateCache(serviceName, l1Cache, strategy, expireStrategy);
        assertThat(l2Cache.getCurrentData(), equalTo(noData));

        ChildData c1 = newData("c1");
        ChildData c2 = newData("c2");
        ChildData c3 = newData("c3");
        l1Cache.add(c1, c2, c3);
        assertThat(l2Cache.getCurrentData(), equalTo(asList(c1, c2, c3)));

        // remove c2 from the l1 cache, but at this point c2 should not yet be expired in the l2 cache
        l1Cache.remove(c2);
        strategy.setPromote(true);
        // therefore it should be in the returned results
        assertThat(l2Cache.getCurrentData(), equalTo(asList(c1, c2, c3)));

        // demote the l2 cache, and now we should have c1 and c3 returned
        strategy.setPromote(false);
        assertThat(l2Cache.getCurrentData(), equalTo(asList(c1, c3)));

        // if we promote again we should get all three returned
        strategy.setPromote(true);
        assertThat(l2Cache.getCurrentData(), equalTo(asList(c1, c2, c3)));

        // if we expire c2 from the l2 cache it should still be returned as it
        // is in the promoted state
        when(expireStrategy.shouldExpire(eq(c2), anyLong())).thenReturn(true);
        assertThat(l2Cache.getCurrentData(), equalTo(asList(c1, c2, c3)));

        // because it is expired, and also not in the l1 cache, when we demote
        // the l2 cache it should be pruned
        strategy.setPromote(false);
        assertThat(l2Cache.getCurrentData(), equalTo(asList(c1, c3)));
        // and then once we promote again, because it has been expired, it won't
        // be in the frozen list
        strategy.setPromote(true);
        assertThat(l2Cache.getCurrentData(), equalTo(asList(c1, c3)));
    }

}
