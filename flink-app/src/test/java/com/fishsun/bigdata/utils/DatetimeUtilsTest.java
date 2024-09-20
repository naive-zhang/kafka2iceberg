package com.fishsun.bigdata.utils;

import org.junit.jupiter.api.Test;

import java.time.ZoneOffset;
import java.util.logging.Logger;

public class DatetimeUtilsTest {
    private static final Logger logger = Logger.getLogger(DatetimeUtilsTest.class.getName());

    @Test
    public void testBeijingTime() {
        logger.info(DateTimeUtils.getBeijingTimeNow());
    }

    @Test
    public void testBeijingInstant() {
        logger.info(DateTimeUtils.parseStringToLocalDateTime(
                DateTimeUtils.getBeijingTimeNow()
        ).toInstant(ZoneOffset.UTC).toString());
    }
}
