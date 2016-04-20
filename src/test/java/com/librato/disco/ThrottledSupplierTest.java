package com.librato.disco;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class ThrottledSupplierTest {
    class FakeDoubleSupplier implements ThrottledSupplier.Supplier<Double> {
        volatile double value = 0;

        @Override
        public Double get() {
            return value;
        }
    }

    @Test
    public void basicOperation() throws Exception {
        FakeDoubleSupplier delegate = new FakeDoubleSupplier();
        ThrottledSupplier<Double> supplier = new ThrottledSupplier<>(200, TimeUnit.MILLISECONDS, delegate);
        Assert.assertThat(supplier.get(), CoreMatchers.equalTo(0D));

        delegate.value = 1;
        Assert.assertThat(supplier.get(), CoreMatchers.equalTo(0D));

        Thread.sleep(200);
        Assert.assertThat(supplier.get(), CoreMatchers.equalTo(1D));
    }
}
