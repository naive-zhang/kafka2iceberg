package com.fishsun.bigdata.utils;

import com.fishsun.bigdata.DeserializedSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.types.Row;
import org.apache.thrift.TException;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 *
 * @Author : zhangxinsen
 * @create : 2024/9/10 10:47
 * @Desc :
 */
public class KafkaUtils {
  public static KafkaSource<Row> getKafkaSource(Map<String, String> paramMap) throws TException {
    String bootstrapServers = paramMap.getOrDefault(
            "bootstrap.servers",
            "kafka:9092"
    );
    List<String> topicList =
            Arrays.asList(paramMap.getOrDefault(
                    "topics",
                    "example"
            ));
    String group = paramMap.getOrDefault(
            "group.id",
            "flink-group"
    );
    return KafkaSource.<Row>builder()
            .setBootstrapServers(bootstrapServers)
            .setTopics(topicList)
            .setGroupId(group)
            .setStartingOffsets(org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer.earliest())
            .setDeserializer(new DeserializedSchema(paramMap))
            .build();
  }

  public static void main(String[] args) {
    String a = "hello";
    String[] split = a.split(",");
    System.out.println(split.length);
    System.out.println(split[0]);
    ;
  }
}
