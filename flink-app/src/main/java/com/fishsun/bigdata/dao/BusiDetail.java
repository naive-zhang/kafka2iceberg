package com.fishsun.bigdata.dao;

import lombok.Builder;
import lombok.Data;
import lombok.ToString;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by IntelliJ IDEA.
 *
 * @Author : zhangxinsen
 * @create : 2024/9/7 23:33
 * @Desc :
 */
@Data
@Builder
@ToString
public class BusiDetail {
  public Integer user_id;
  public Integer goods_id;
  public Integer goods_cnt;
  public BigDecimal fee;
  public Timestamp collection_time;
  public Timestamp order_time;
  public Integer is_valid;
  public Timestamp create_time;
  public Timestamp sign_time;


  public static int randomInt(int lower, int upper) {
    double random = Math.random();
    return (int) Math.floor(random * (upper - lower) + lower);
  }

  public static double randomNum(double low, double high) {
    double random = Math.random();
    return random * (high - low) + low;
  }

  public static LocalDateTime getRandomTimeBetween(LocalDateTime start, LocalDateTime end) {
    // 将 LocalDateTime 转换为秒数 (Epoch Second)
    long startSeconds = start.toEpochSecond(ZoneOffset.UTC);
    long endSeconds = end.toEpochSecond(ZoneOffset.UTC);

    // 在指定的时间范围内生成随机秒数
    long randomSeconds = ThreadLocalRandom.current().nextLong(startSeconds, endSeconds);

    // 将随机秒数转换回 LocalDateTime
    return LocalDateTime.ofEpochSecond(randomSeconds, 0, ZoneOffset.UTC);
  }

  /**
   * 随机生成一条数据
   *
   * @return
   */
  public static BusiDetail randomGen() {
    // 定义开始时间和结束时间
    LocalDateTime startTime = LocalDateTime.of(2024, 9, 1, 0, 0, 0);
    LocalDateTime endTime = LocalDateTime.of(2024, 9, 5, 23, 59, 59);

    // 生成随机时间
    LocalDateTime collectionTime = getRandomTimeBetween(startTime, endTime),
            orderTime = getRandomTimeBetween(startTime, endTime),
            signTime = getRandomTimeBetween(startTime, endTime),
            createTime = getRandomTimeBetween(startTime, endTime);

    return BusiDetail.builder()
            .user_id(randomInt(1, 100))
            .goods_id(randomInt(1, 10000))
            .goods_cnt(randomInt(1, 15))
            .fee(BigDecimal.valueOf(randomNum(0, 10000)))
            .collection_time(Timestamp.valueOf(collectionTime))
            .create_time(Timestamp.valueOf(createTime))
            .order_time(Timestamp.valueOf(orderTime))
            .sign_time(Timestamp.valueOf(signTime))
            .is_valid(1)
            .build();
  }

  public static void main(String[] args) {
    System.out.println(randomGen());
  }
}