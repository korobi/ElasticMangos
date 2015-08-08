package io.korobi;

import io.korobi.utils.NumberUtil;
import org.junit.Assert;
import org.junit.Test;

public class NumberUtilTest {

    @Test
    public void testFractionalValue() throws Exception {
        Assert.assertTrue(NumberUtil.isValueFractional(1.5d));
    }

    @Test
    public void testIntegerValue() throws Exception {
        Assert.assertFalse(NumberUtil.isValueFractional(1d));
    }
}