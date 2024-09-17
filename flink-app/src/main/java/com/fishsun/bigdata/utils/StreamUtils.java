package com.fishsun.bigdata.utils;

import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 *
 * @Author : zhangxinsen
 * @create : 2024/9/10 10:38
 * @Desc :
 */
public class StreamUtils {
  public static StreamExecutionEnvironment getStreamEnv(Map<String, String> paramMap) {
    if (paramMap.getOrDefault("local", "false")
            .toLowerCase()
            .trim()
            .equals("false")) {
      return StreamExecutionEnvironment.getExecutionEnvironment();
    }
    org.apache.flink.configuration.Configuration conf = new org.apache.flink.configuration.Configuration();
    conf.set(RestOptions.PORT, Integer.valueOf(paramMap.getOrDefault("rest.port",
            "9999")));
    return StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(
            conf
    );
  }
}
