package com.fishsun.bigdata.utils;

import com.fishsun.bigdata.dao.FlinkTask;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.List;

public class FlinkTaskUtilsTest {
    @BeforeAll
    public static void setUpBeforeClass() throws Exception {
        FlinkTaskUtils.JDBC_URL = "jdbc:mysql://db:13306/flink_tasks?useSSL=false&characterEncoding=utf8";
        FlinkTaskUtils.JDBC_USER = "root";
        FlinkTaskUtils.JDBC_PASSWORD = "123456";
    }
    @Test
    public void testGetFlinkTasks() throws SQLException {
        int taskGroupId = 1;
        FlinkTaskUtils instance = FlinkTaskUtils.getInstance();
        List<FlinkTask> flinkTasksByGroupId = instance.getFlinkTasksByGroupId(taskGroupId);
        Assertions.assertNotNull(flinkTasksByGroupId);
    }
}
