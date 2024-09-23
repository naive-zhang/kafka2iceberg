package com.fishsun.bigdata.flink;

import com.fishsun.bigdata.dao.FlinkTask;
import com.fishsun.bigdata.utils.ApplicationUtils;
import com.fishsun.bigdata.utils.FlinkTaskUtils;
import com.fishsun.bigdata.utils.StreamUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.iceberg.flink.util.FlinkPackage;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import static com.fishsun.bigdata.utils.ParamUtils.enhanceConfig;

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
        ParameterTool pt = ParameterTool.fromArgs(args);
        int taskGroupId = pt.getInt("task.group.id", 1);
        boolean isLocalMode = pt.getBoolean("local.enable", false);
        int restPort = pt.getInt("rest.port", 9999);

        String host = pt.get("host", "db");
        int port = pt.getInt("port", 13306);
        String dbName = pt.get("db", "flink_tasks");
        String userName = pt.get("user", "root");
        String password = pt.get("password", "123456");

        FlinkTaskUtils.JDBC_URL = String.format("jdbc:mysql://%s:%d/%s?useSSL=false&characterEncoding=utf8",
                host, port, dbName);
        FlinkTaskUtils.JDBC_USER = userName;
        FlinkTaskUtils.JDBC_PASSWORD = password;
        logger.info("jdbc url: {}", FlinkTaskUtils.JDBC_URL);

        logger.info("flink package version: {}", FlinkPackage.version());
        List<FlinkTask> flinkTasksByGroupId = FlinkTaskUtils.getInstance().getFlinkTasksByGroupId(taskGroupId);
        // 创建 Flink 执行环境
        // local=false(默认) --> 集群环境
        // local=true(本地webUI) --> 本地环境 rest.port配置端口
        StreamExecutionEnvironment env =
                StreamUtils.getStreamEnv(isLocalMode, restPort);
        flinkTasksByGroupId.forEach(flinkTask -> {
            Map<String, String> paramMap = flinkTask.getParamMap();
            enhanceConfig(paramMap);
            try {
                ApplicationUtils.setupPipelines(
                        env,
                        paramMap
                );
                logger.info("{} set up successfully", flinkTask.getTaskName());
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (TException e) {
                throw new RuntimeException(e);
            }
        });
        // 启动 Flink 作业
        JobClient jobClient = env.executeAsync("Flink Kafka to Iceberg with Additional Fields");
        ApplicationUtils.jobClient = jobClient;
        Thread.sleep(Long.MAX_VALUE);
    }

}
