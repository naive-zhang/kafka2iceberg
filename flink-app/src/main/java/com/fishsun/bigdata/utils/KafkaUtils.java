package com.fishsun.bigdata.utils;

import com.fishsun.bigdata.DeserializedSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.table.data.RowData;
import org.apache.kafka.common.TopicPartition;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private static final Logger logger = LoggerFactory.getLogger(KafkaUtils.class);
    public static KafkaSource<RowData> getKafkaSource(Map<String, String> paramMap) throws TException {
        String bootstrapServers = paramMap.getOrDefault("bootstrap.servers", "kafka:9092");
        List<String> topicList = Arrays.stream(paramMap
                        .getOrDefault("topics", "example")
                        .split(","))
                .map(String::trim)
                .collect(Collectors.toList());
        String group = paramMap.getOrDefault("group.id", "flink-group");
        Map<TopicPartition, Long> topicPartitionStringMap = ParamUtils.getTopicPartitions(paramMap);
        String offset = paramMap.getOrDefault("offset", "earliest").trim().toLowerCase();
        if (topicPartitionStringMap == null || topicPartitionStringMap.isEmpty()) {
            if (!offset.equals("earliest") && !offset.equals("latest")) {
                if (offset.endsWith("l")) {
                    offset = offset.substring(0, offset.length() - 1);
                }
                if (!StringUtils.isValidIntegerWithRegex(offset)) {
                    throw new IllegalArgumentException("Invalid offset: " + offset);
                }
            }
        } else {
            logger.info("from a specified offset");
            for (Map.Entry<TopicPartition, Long> topicPartitionLongEntry : topicPartitionStringMap.entrySet()) {
                logger.info("{} with offset {}", topicPartitionLongEntry.getKey(), topicPartitionLongEntry.getValue());
            }
            return KafkaSource.<RowData>builder().setBootstrapServers(bootstrapServers)
                    .setTopics(topicList)
                    .setGroupId(group)
                    .setStartingOffsets(org.apache.flink.connector.kafka.source.enumerator
                            .initializer.OffsetsInitializer
                            .offsets(
                                    topicPartitionStringMap
                            )
                    )
                    .setDeserializer(new DeserializedSchema(paramMap)).build();
        }
        switch (offset) {
            case "earliest":
                return KafkaSource.<RowData>builder().setBootstrapServers(bootstrapServers)
                        .setTopics(topicList)
                        .setGroupId(group)
                        .setStartingOffsets(org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer.earliest())
                        .setDeserializer(new DeserializedSchema(paramMap)).build();
            case "latest":
                return KafkaSource.<RowData>builder().setBootstrapServers(bootstrapServers)
                        .setTopics(topicList)
                        .setGroupId(group)
                        .setStartingOffsets(org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer.latest())
                        .setDeserializer(new DeserializedSchema(paramMap)).build();
            default:
                return KafkaSource.<RowData>builder().setBootstrapServers(bootstrapServers)
                        .setTopics(topicList)
                        .setGroupId(group)
                        .setStartingOffsets(org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer.timestamp(
                                Long.valueOf(offset)
                        ))
                        .setDeserializer(new DeserializedSchema(paramMap)).build();
        }
    }
}
