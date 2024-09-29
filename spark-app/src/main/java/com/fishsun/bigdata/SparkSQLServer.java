package com.fishsun.bigdata;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.sparkproject.jetty.server.Server;
import org.sparkproject.jetty.servlet.DefaultServlet;
import org.sparkproject.jetty.servlet.ServletContextHandler;
import org.sparkproject.jetty.servlet.ServletHolder;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class SparkSQLServer {

    private static SparkSession spark;

    public static void main(String[] args) throws Exception {
        SparkConf sparkConf = ConfigLoader.loadConfFromClassloader();

        if (Arrays.asList(args).stream().anyMatch(x -> x.trim().equalsIgnoreCase("local"))) {
            spark = SparkSession.builder()
                    .appName("Spark SQL Web Executor")
                    .master("local[2]")
                    .config(sparkConf)
                    .enableHiveSupport()
                    .getOrCreate();
        } else {
            spark = SparkSession.builder()
                    .appName("Spark SQL Web Executor")
                    .config(sparkConf)
                    .enableHiveSupport()
                    .getOrCreate();
        }

        // 设置 Jetty 服务器
        int serverPort = sparkConf.getInt("server.port", 8080);
        Server server = new Server(serverPort);

        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");

        // 配置静态资源的路径（resourceBase）
        context.setResourceBase(SparkSQLServer.class.getClassLoader().getResource("").toExternalForm());

        // 增加一个 DefaultServlet 处理静态资源
        context.addServlet(new ServletHolder("default", new DefaultServlet()), "/static/*");

        // 映射路径到相应的Servlet
        context.addServlet(new ServletHolder(new SQLInputServlet()), "/");
        context.addServlet(new ServletHolder(new SQLExecuteServlet(spark)), "/execute");
        context.addServlet(new ServletHolder(new SQLCancelServlet(spark)), "/cancel");
        server.setHandler(context);

        System.out.println("启动 Spark SQL Web 服务器, 请访问: http://localhost:" + serverPort);
        server.start();
        server.join();
    }

    // 显示SQL输入页面的Servlet
    public static class SQLInputServlet extends HttpServlet {
        @Override
        protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
            resp.setContentType("text/html");
            resp.setStatus(HttpServletResponse.SC_OK);
            resp.getWriter().println(HTMLPage.getIndexPage());
        }
    }

    // 取消SQL执行的Servlet
    public static class SQLCancelServlet extends HttpServlet {
        private SparkSession spark;

        public SQLCancelServlet(SparkSession spark) {
            this.spark = spark;
        }

        @Override
        protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
            String jobGroup = req.getParameter("jobGroup");
            if (jobGroup != null && !jobGroup.isEmpty()) {
                spark.sparkContext().cancelJobGroup(jobGroup);
                resp.setStatus(HttpServletResponse.SC_OK);
                resp.getWriter().println("SQL 执行已取消.");
            } else {
                resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                resp.getWriter().println("未提供有效的 JobGroup ID.");
            }
        }
    }

    // 执行SQL并返回结果的Servlet
    public static class SQLExecuteServlet extends HttpServlet {
        private SparkSession spark;

        public SQLExecuteServlet(SparkSession spark) {
            this.spark = spark;
        }

        @Override
        protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
            String sql = req.getParameter("sql");
            String jobGroup = req.getParameter("jobGroup");

            resp.setContentType("application/json");
            resp.setStatus(HttpServletResponse.SC_OK);

            if (sql == null || sql.trim().isEmpty()) {
                resp.getWriter().println("{\"error\":\"未提供SQL语句！请返回并输入有效的SQL语句。\"}");
                return;
            }

            try {
                // 自动判断并加上 LIMIT 200
                sql = SQLUtils.addLimitIfNecessary(spark, sql);

                // 设置 Job Group
                String groupId = (jobGroup != null && !jobGroup.isEmpty()) ? jobGroup : UUID.randomUUID().toString();
                spark.sparkContext().setJobGroup(groupId, "User Submitted SQL Query", true);

                // 执行SQL语句
                Dataset<Row> result = spark.sql(sql);

                // 收集结果，限制最多返回100行
                List<Row> rows = result.limit(100).collectAsList();

                // 构建 JSON 格式的结果
                Map<String, Object> resultMap = new HashMap<>();
                resultMap.put("columns", Arrays.asList(result.columns()));
                List<List<String>> data = new ArrayList<>();
                for (Row row : rows) {
                    List<String> rowData = new ArrayList<>();
                    for (int i = 0; i < row.size(); i++) {
                        Object value = row.get(i);
                        rowData.add(value != null ? value.toString() : "null");
                    }
                    data.add(rowData);
                }
                resultMap.put("data", data);

                // 输出结果
                resp.getWriter().println(JSONUtils.toJson(resultMap));
            } catch (Exception e) {
                e.printStackTrace();
                resp.getWriter().println("{\"error\":\"SQL 执行错误：" + e.getMessage() + "\"}");
            } finally {
                // 清除 Job Group
                spark.sparkContext().clearJobGroup();
            }
        }
    }
}
