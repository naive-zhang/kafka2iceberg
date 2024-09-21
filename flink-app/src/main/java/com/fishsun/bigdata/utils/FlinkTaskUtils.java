package com.fishsun.bigdata.utils;

import com.fishsun.bigdata.dao.FlinkTask;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.sql.*;
import java.util.*;


public class FlinkTaskUtils {

    private static final Logger logger = LoggerFactory.getLogger(FlinkTaskUtils.class);
    private static FlinkTaskUtils instance;

    public static String JDBC_URL = "jdbc:mysql://db:13306/flink_tasks?useSSL=false&characterEncoding=utf8";
    public static String JDBC_USER = "root";
    public static String JDBC_PASSWORD = "123456";

    /**
     * Thread-safe singleton instance creator.
     *
     * @return A single instance of HiveSchemaUtils.
     * @throws MetaException Thrown if an error occurs while creating the MetaStoreClient.
     */
    public static FlinkTaskUtils getInstance() {

        // Double-checked locking to ensure thread-safe singleton initialization.
        if (instance == null) {
            synchronized (FlinkTaskUtils.class) {
                if (instance == null) {
                    instance = new FlinkTaskUtils();
                }
            }
        }
        return instance;
    }

    /**
     * 根据给定的 task_group_id，获取对应的 List<FlinkTask>。
     *
     * @param taskGroupId 要查询的 task_group_id
     * @return 包含 FlinkTask 的列表
     * @throws SQLException 数据库访问异常
     */
    public List<FlinkTask> getFlinkTasksByGroupId(int taskGroupId) throws SQLException {
        Connection connection = null;
        PreparedStatement psTaskGroup = null;
        PreparedStatement psTaskGroupParams = null;
        PreparedStatement psTaskParams = null;
        ResultSet rsTaskGroup = null;
        ResultSet rsTaskGroupParams = null;
        ResultSet rsTaskParams = null;
        List<FlinkTask> taskList = new ArrayList<>();

        try {
            // 1. 获取数据库连接
            connection = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASSWORD);

            // 2. get params with group-level
            String sqlGroupTaskParams = "SELECT param_key, param_value FROM t_task_group_params WHERE task_group_id = ?";
            psTaskGroupParams = connection.prepareStatement(sqlGroupTaskParams);

            // 设置参数
            psTaskGroupParams.setInt(1, taskGroupId);
            rsTaskGroupParams = psTaskGroupParams.executeQuery();

            Map<String, String> taskGroupParameters = new HashMap<>();
            while (rsTaskGroupParams.next()) {
                String paramKey = rsTaskGroupParams.getString("param_key").trim();
                String paramValue = rsTaskGroupParams.getString("param_value").trim();
                taskGroupParameters.put(paramKey, paramValue);
            }
            // 清理结果集
            rsTaskGroupParams.close();

            // 3. 查询 t_task_group 表，获取 task_id 列表
            String sqlTaskGroup = "SELECT task_id, task_name FROM t_task_group WHERE task_group_id = ?";
            psTaskGroup = connection.prepareStatement(sqlTaskGroup);
            psTaskGroup.setInt(1, taskGroupId);
            rsTaskGroup = psTaskGroup.executeQuery();

            while (rsTaskGroup.next()) {
                int taskId = rsTaskGroup.getInt("task_id");
                taskList.add(
                        FlinkTask.builder()
                                .taskId(taskId)
                                .taskName(rsTaskGroup.getString("task_name"))
                                .taskGroupId(taskGroupId)
                                .build()
                );
            }


            // 4. 对于每个 task_id，查询 t_task 表，获取参数并构建 FlinkTask 对象
            String sqlTaskParams = "SELECT param_key, param_value FROM t_task WHERE task_id = ?";
            psTaskParams = connection.prepareStatement(sqlTaskParams);

            for (FlinkTask task : taskList) {
                // 设置参数
                psTaskParams.setInt(1, task.getTaskId());
                rsTaskParams = psTaskParams.executeQuery();

                Map<String, String> paramMap = new HashMap<>(taskGroupParameters);
                while (rsTaskParams.next()) {
                    String paramKey = rsTaskParams.getString("param_key").trim();
                    String paramValue = rsTaskParams.getString("param_value").trim();
                    paramMap.put(paramKey, paramValue);
                }
                task.setParamMap(paramMap);
                // 清理结果集
                rsTaskParams.close();
            }

        } finally {
            // 关闭资源
            if (rsTaskParams != null) rsTaskParams.close();
            if (rsTaskGroup != null) rsTaskGroup.close();
            if (psTaskParams != null) psTaskParams.close();
            if (psTaskGroup != null) psTaskGroup.close();
            if (connection != null) connection.close();
        }
        logger.info("got {} tasks with task group id {}", taskList.size(), taskGroupId);
        for (FlinkTask flinkTask : taskList) {
            logger.info("task info {}", flinkTask);
        }
        return taskList;
    }
}
