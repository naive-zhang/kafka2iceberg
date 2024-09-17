package com.fishsun.bigdata.dao;

import lombok.Builder;
import lombok.Data;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 *
 * @Author : zhangxinsen
 * @create : 2024/9/14 23:15
 * @Desc :
 */


@Data
@Builder
public class Task {
  private String source;
  private String kafkaBootstrapServers;
  private String kafkaTopics;
  private String kafkaGroupId;
  private String catalogType;
  private String namespace;
  private String tableName;
  private String offset;
  private List<Field> taskFields;
}
