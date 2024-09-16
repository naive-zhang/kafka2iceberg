package com.fishsun.bigdata.utils;

import java.time.LocalDate;
import java.time.LocalDateTime;
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

  public static void main(String[] args) {
    String datetime = "2024-09-10 01:00:02";
    System.out.println(parseStringToLocalDateTime(datetime));
    System.out.println(parseString2localDate(datetime));
    datetime = "2024-09-12";
    System.out.println(parseString2localDate(datetime));
  }
}
