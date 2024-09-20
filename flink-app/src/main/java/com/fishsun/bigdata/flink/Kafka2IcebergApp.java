package com.fishsun.bigdata.flink;

import com.fishsun.bigdata.utils.ApplicationUtils;
import com.fishsun.bigdata.utils.StreamUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.iceberg.flink.util.FlinkPackage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static com.fishsun.bigdata.utils.ParamUtils.enhanceConfig;
import static com.fishsun.bigdata.utils.ParamUtils.parseConfig;

/**
 * Created by IntelliJ IDEA.
 *
 * @Author : zhangxinsen
 * @create : 2024/9/8 20:41
 * @Desc :
 */
public class Kafka2IcebergApp {

  private static final Logger logger = LoggerFactory.getLogger(Kafka2IcebergApp.class);

  public static void main(String[] args) throws Exception {
    logger.info("flink package version: {}", FlinkPackage.version());
    Map<String, String> paramMap = parseConfig(args);
    enhanceConfig(paramMap);
    // 创建 Flink 执行环境
    // local=false(默认) --> 集群环境
    // local=true(本地webUI) --> 本地环境 rest.port配置端口
    StreamExecutionEnvironment env =
            StreamUtils.getStreamEnv(paramMap);

    ApplicationUtils.setupPipelines(
            env,
            paramMap
    );


    // 启动 Flink 作业
    env.execute("Flink Kafka to Iceberg with Additional Fields");
  }

}
