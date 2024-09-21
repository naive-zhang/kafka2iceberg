package com.fishsun.bigdata.utils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class StringUtilsTest {
    @Test
    public void testNumberValid() {
        Assertions.assertTrue(StringUtils.isValidIntegerWithRegex("435435353"));
        Assertions.assertFalse(StringUtils.isValidIntegerWithRegex("435433vd432432"));
    }
}
