package com.librato.disco;

import org.apache.curator.framework.recipes.cache.ChildData;

public class FakeChildData extends ChildData {

    public static ChildData newData(String data) {
        return new FakeChildData(data);
    }

    public FakeChildData(String data) {
        super("/path/data-" + data, null, data.getBytes());
    }
}
