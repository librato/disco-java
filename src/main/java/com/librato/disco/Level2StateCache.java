package com.librato.disco;

import org.apache.curator.framework.recipes.cache.ChildData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A state cache that wraps another state cache.  In normal operation it will
 * delegate to the other state cache and update an internal map of nodes and
 * ttls as nodes are returned from the delegate.
 * <p>
 * A supplied strategy will determine whether or not the ttl cached nodes
 * should ever be promoted. If they are promoted, during the time of promotion
 * the ttl cached nodes will be frozen and served instead of what the delegate
 * cache returns.  Once the strategy determines it is time to demote the l2
 * cache, operation will return to normal, the nodes will be unfrozen, and
 * the delegate data will be returned to the caller.
 */
public class Level2StateCache implements IStateCache {
    private static final Logger log = LoggerFactory.getLogger(Level2StateCache.class);
    private final String serviceName;
    private final IStateCache delegate;
    private final ILevel2CacheStrategy strat;
    private final AtomicBoolean promoted = new AtomicBoolean();
    private final AtomicReference<Long> promotedAt = new AtomicReference<>();
    // path -> cached data
    private final ConcurrentMap<String, CachedChildData> cache = new ConcurrentHashMap<>();
    private final AtomicReference<List<ChildData>> promotedData = new AtomicReference<>();
    private final IExpireStrategy expireStrategy;
    private final ScheduledExecutorService monitorExecutor = Executors.newSingleThreadScheduledExecutor();
    private volatile ScheduledFuture<?> monitorFuture;
    private final StarterStopper starterStopper = new StarterStopper();

    /**
     * Constructor.
     *
     * @param delegate the cache that will populate the l2 cache
     * @param strat    determines when to promote demote
     */
    public Level2StateCache(String serviceName, IStateCache delegate, ILevel2CacheStrategy strat) {
        this(serviceName, delegate, strat, new DefaultExpireStrategy());
    }

    Level2StateCache(String serviceName, IStateCache delegate, ILevel2CacheStrategy strat, IExpireStrategy expireStrategy) {
        this.serviceName = serviceName;
        this.delegate = delegate;
        this.strat = strat;
        this.expireStrategy = expireStrategy;
    }

    @Override
    public List<ChildData> getCurrentData() {
        List<ChildData> data = delegate.getCurrentData();
        if (strat == null) {
            // cache is disbled if strat is null
            return data;
        }

        updateCache(data);
        prune();

        // signal to the strat that we have delegate data
        boolean shouldPromote = strat.promote(serviceName, data.size(), cache.size(), promoted.get());

        List<ChildData> promotedData = getPromotedData(shouldPromote);
        if (promotedData != null && !promotedData.isEmpty()) {
            return promotedData;
        }

        // by default return the delegate data
        return data;
    }

    /**
     * A synchronized method which sets the l2 state to a promoted or demoted
     * state based on what the strategy dictated.
     * <p>
     * If the strategy recommends promotion then it will attempt to set the
     * state of the cache to promoted and will set a reference to the
     * data frozen at the time of promotion. If the strategy recommends promotion
     * it will return the current frozen data.
     * <p>
     * If the strategy does not recommend promotion then it will return null
     * to signify that it should use the upstream delegate cached data.
     *
     * @param shouldPromote whether or not the strat recommended promotion
     */
    private synchronized List<ChildData> getPromotedData(boolean shouldPromote) {
        if (shouldPromote) {
            if (this.promoted.compareAndSet(false, true)) {
                List<ChildData> newPromotion = new ArrayList<>();
                for (CachedChildData data : cache.values()) {
                    newPromotion.add(data.getData());
                }
                Collections.sort(newPromotion);
                log.error("Promoting L2 cache for {} using {} promoted child data nodes", serviceName, newPromotion.size());
                promotedData.set(newPromotion);
                promotedAt.set(System.currentTimeMillis());
            }
            return promotedData.get();
        } else {
            if (this.promoted.compareAndSet(true, false)) {
                log.info("Demoting L2 cache for {}", serviceName);
                promotedData.set(null);
                promotedAt.set(null);
            }
        }
        return null;
    }

    /**
     * Updates the cache with the specified data.
     */
    private void updateCache(List<ChildData> data) {
        for (ChildData childData : data) {
            long stratTtl = strat.getTtlTimeUnit().toMillis(strat.getTtl());
            long expireAtMillis = System.currentTimeMillis() + stratTtl;
            CachedChildData cachedData = cache.get(childData.getPath());
            if (cachedData == null) {
                cachedData = new CachedChildData(childData, expireAtMillis);
                cache.put(childData.getPath(), cachedData);
            } else {
                cachedData.setData(childData);
                cachedData.setExpireAt(expireAtMillis);
            }
        }
    }

    /**
     * Removes any entries from the cache which are too old
     */
    private void prune() {
        Iterator<Map.Entry<String, CachedChildData>> iterator = cache.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, CachedChildData> entry = iterator.next();
            CachedChildData cached = entry.getValue();
            if (expireStrategy.shouldExpire(cached.getData(), cached.getExpireAt())) {
                log.info("Expiring {} due to TTL", cached.getData().getPath());
                iterator.remove();
            }
        }
    }

    @Override
    public void start() throws Exception {
        starterStopper.start();
        delegate.start();
        monitorFuture = monitorExecutor.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                Long promotedAtMillis = promotedAt.get();
                if (promotedAtMillis != null) {
                    long promotionDurationMillis = System.currentTimeMillis() - promotedAtMillis;
                    // if longer than a minute of promotion
                    if (promotionDurationMillis > 1000 * 60) {
                        log.error("L2 cache for {} has been promoted for {}s", serviceName, promotionDurationMillis / 1000);
                    }
                }
            }
        }, 1, 1, TimeUnit.MINUTES);
    }

    @Override
    public void stop() throws Exception {
        starterStopper.stop();
        delegate.stop();
        if (monitorFuture != null) {
            monitorFuture.cancel(false);
        }
    }
}
