package com.fishsun.bigdata.utils;

import com.fishsun.bigdata.DeserializedSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.Row;
import org.apache.thrift.TException;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by IntelliJ IDEA.
 *
 * @Author : zhangxinsen
 * @create : 2024/9/10 10:47
 * @Desc :
 */
public class KafkaUtils {
    public static KafkaSource<RowData> getKafkaSource(Map<String, String> paramMap) throws TException {
        String bootstrapServers = paramMap.getOrDefault("bootstrap.servers", "kafka:9092");
        List<String> topicList = Arrays.stream(paramMap
                        .getOrDefault("topics", "example")
                        .split(","))
                .map(String::trim)
                .collect(Collectors.toList());
        String group = paramMap.getOrDefault("group.id", "flink-group");
        return KafkaSource.<RowData>builder().setBootstrapServers(bootstrapServers).setTopics(topicList).setGroupId(group).setStartingOffsets(org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer.earliest()).setDeserializer(new DeserializedSchema(paramMap)).build();
    }
}
