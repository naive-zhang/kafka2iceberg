package com.fishsun.bigdata.task;

/**
 * Created by IntelliJ IDEA.
 *
 * @Author : zhangxinsen
 * @create : 2024/9/10 23:30
 * @Desc :
 */
public enum DataSourceType {

  KAFKA_CANAL_JSON("kafka-canal-json"),
  KAFKA_DEBEZIUM_JSON("kafka-debezium-json"),
  KAFKA_JSON("kafka-json"),
  ROCKET_MQ_JSON("rocket-mq-json"),
  UNKNOWN_SOURCE("unknown-source");

  private final String description;

  // 构造方法
  DataSourceType(String description) {
    this.description = description;
  }

  // 获取枚举的描述信息
  public String getDescription() {
    return description;
  }

  // 通过描述信息获取对应的枚举类型
  public static DataSourceType fromDescription(String description) {
    for (DataSourceType sourceType : DataSourceType.values()) {
      if (sourceType.getDescription().equalsIgnoreCase(description)) {
        if (sourceType.equals(UNKNOWN_SOURCE)) {
          throw new IllegalArgumentException("got an unknown source type");
        }
        return sourceType;
      }
    }
    throw new IllegalArgumentException("Unknown description: " + description);
  }

  // 覆盖toString方法，返回枚举的描述信息
  @Override
  public String toString() {
    return this.description;
  }
}
