package com.fishsun.bigdata.utils;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Created by IntelliJ IDEA.
 *
 * @Author : zhangxinsen
 * @create : 2024/9/16 15:44
 * @Desc :
 */
public class DateTimeUtils {
  private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
  private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");

  public static LocalDateTime parseStringToLocalDateTime(String dateTimeString) {
    return LocalDateTime.parse(dateTimeString, FORMATTER);
  }

  public static LocalDate parseString2localDate(String dateString) {
    String date = dateString;
    if (!dateString.contains(":")) {
      date = dateString.trim() + " 00:00:00";
    }
    LocalDateTime localDateTime = parseStringToLocalDateTime(date);
    return localDateTime.toLocalDate();
  }

  public static String getBeijingTimeNow() {
    // 指定东八区时区（例如：亚洲/上海）
    ZoneId zoneId = ZoneId.of("Asia/Shanghai");
    // 获取当前时间
    ZonedDateTime zonedDateTime = ZonedDateTime.now(zoneId);
    // 格式化日期时间
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
      // 输出东八区时间
    return zonedDateTime.format(formatter);
  }
}
