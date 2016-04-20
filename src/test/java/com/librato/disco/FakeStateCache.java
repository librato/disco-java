package com.librato.disco;

import org.apache.curator.framework.recipes.cache.ChildData;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

class FakeStateCache extends AbstractStateCache {
    List<ChildData> currentData = new CopyOnWriteArrayList<>();

    @Override
    public List<ChildData> getCurrentData() {
        return currentData;
    }

    public void add(ChildData... data) {
        for (ChildData childData : data) {
            if (!this.currentData.contains(childData)) {
                this.currentData.add(childData);
            }
        }
    }

    public void remove(ChildData data) {
        this.currentData.remove(data);
    }

    public void clear() {
        this.currentData.clear();
    }
}
