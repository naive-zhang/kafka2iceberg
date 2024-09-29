package com.fishsun.bigdata;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.*;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.execution.LogicalRDD;
import scala.runtime.AbstractPartialFunction;
import org.sparkproject.jetty.server.Server;
import org.sparkproject.jetty.servlet.ServletContextHandler;
import org.sparkproject.jetty.servlet.ServletHolder;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Base64;
import java.util.List;
import java.util.UUID;

public class SparkSQLServer {

    private static SparkSession spark;

    public static void main(String[] args) throws Exception {
        // 创建 SparkSession，使用本地模式
        spark = SparkSession.builder()
                .appName("Spark SQL Web Executor")
                .master("local[*]")
                .config("hive.metastore.uris", "thrift://localhost:9083")
                .config("hive.metastore.warehouse.dir", "hdfs://master:9000/user/hive/warehouse")
                .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
                .config("spark.sql.catalog.spark_catalog.type", "hive")
                .config("spark.sql.catalog.spark_catalog.uri", "thrift://localhost:9083")
                .config("spark.sql.catalog.spark_catalog.warehouse", "hdfs://master:9000/user/hive/warehouse")
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .enableHiveSupport()
                .getOrCreate();

        // 设置 Jetty 服务器
        Server server = new Server(8080);
        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");
        server.setHandler(context);

        // 映射路径到相应的Servlet
        context.addServlet(new ServletHolder(new SQLInputServlet()), "/");
        context.addServlet(new ServletHolder(new SQLExecuteServlet()), "/execute");
        context.addServlet(new ServletHolder(new SQLCancelServlet()), "/cancel");

        System.out.println("启动 Spark SQL Web 服务器, 请访问: http://localhost:8080");
        server.start();
        server.join();
    }

    // 显示SQL输入页面的Servlet
    public static class SQLInputServlet extends HttpServlet {
        @Override
        protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
            resp.setContentType("text/html");
            resp.setStatus(HttpServletResponse.SC_OK);
            resp.getWriter().println(
                    "<html>" +
                            "<head>" +
                            "<title>Spark SQL Executor</title>" +
                            "<link href='https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css' rel='stylesheet'>" +
                            "<script src='https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/js/bootstrap.bundle.min.js'></script>" +
                            "<style>" +
                            "body, html { height: 100%; margin: 0; }" +
                            ".container { height: 100vh; display: flex; flex-direction: column; padding: 20px; }" +
                            "#sql-input { flex: 1; height: 200px; resize: none; }" +
                            "#execute-btn, #cancel-btn { margin-top: 10px; }" +
                            "#loading-spinner { display: none; text-align: center; }" +
                            "#loading-spinner .spinner-border { width: 3rem; height: 3rem; }" +
                            "#result { flex: 2; overflow: auto; margin-top: 10px; background: #f8f9fa; padding: 15px; border: 1px solid #ddd; }" +
                            "table { margin-top: 10px; }" +
                            "th { position: sticky; top: 0; background: #f8f9fa; }" +
                            "th, td { white-space: nowrap; }" +
                            "</style>" +
                            "<script>" +
                            "var controller;" +
                            "var currentJobGroup = '';" +
                            "function showLoadingSpinner() {" +
                            "    document.getElementById('loading-spinner').style.display = 'block';" +
                            "}" +
                            "function hideLoadingSpinner() {" +
                            "    document.getElementById('loading-spinner').style.display = 'none';" +
                            "}" +
                            "function decodeBase64(encodedStr) {" +
                            "    return decodeURIComponent(atob(encodedStr).split('').map(function(c) {" +
                            "        return '%' + ('00' + c.charCodeAt(0).toString(16)).slice(-2);" +
                            "    }).join(''));" +
                            "}" +
                            "function executeSQL() {" +
                            "    var sql = document.getElementById('sql-input').value.trim();" +
                            "    if (!sql) {" +
                            "        alert('请输入有效的 SQL 语句');" +
                            "        return;" +
                            "    }" +
                            "    currentJobGroup = 'job-' + Math.random().toString(36).substring(2, 15);" +  // 生成唯一 jobId
                            "    controller = new AbortController();" + // 初始化新的 AbortController
                            "    showLoadingSpinner();" + // 显示加载动画
                            "    fetch('/execute', {" +
                            "        method: 'POST'," +
                            "        headers: { 'Content-Type': 'application/x-www-form-urlencoded' }," +
                            "        body: 'sql=' + encodeURIComponent(sql) + '&jobGroup=' + currentJobGroup," +
                            "        signal: controller.signal" + // 绑定 AbortController 信号
                            "    }).then(response => {" +
                            "        if (!response.ok) throw new Error('网络错误: ' + response.status);" +
                            "        return response.text();" +
                            "    }).then(resultHtml => {" +
                            "        document.getElementById('result').innerHTML = resultHtml;" +
                            "        var cells = document.querySelectorAll('.encrypted-cell');" +
                            "        cells.forEach(cell => {" +
                            "            cell.textContent = decodeBase64(cell.textContent);" +
                            "        });" +
                            "    }).catch(error => {" +
                            "        if (error.name === 'AbortError') {" +
                            "            document.getElementById('result').innerHTML = '<div class=\"alert alert-warning\">SQL 执行已取消。</div>';" +
                            "        } else {" +
                            "            console.error('SQL 执行失败:', error);" +
                            "            alert('SQL 执行失败: ' + error.message);" +
                            "        }" +
                            "    }).finally(() => {" +
                            "        hideLoadingSpinner();" + // 隐藏加载动画
                            "    });" +
                            "}" +
                            "function cancelSQL() {" +
                            "    if (controller) {" +
                            "        controller.abort();" +  // 取消前端请求
                            "        fetch('/cancel', {" +  // 后端取消 Spark Job
                            "            method: 'POST'," +
                            "            headers: { 'Content-Type': 'application/x-www-form-urlencoded' }," +
                            "            body: 'jobGroup=' + encodeURIComponent(currentJobGroup)" +
                            "        });" +
                            "    }" +
                            "}" +
                            "</script>" +
                            "</head>" +
                            "<body>" +
                            "<div class='container'>" +
                            "<h1 class='text-center mb-4'>Spark SQL Executor</h1>" +
                            "<textarea id='sql-input' class='form-control' placeholder='输入您的 SQL 查询语句...'></textarea>" +
                            "<button id='execute-btn' class='btn btn-primary w-100' onclick='executeSQL()'>执行 SQL</button>" +
                            "<button id='cancel-btn' class='btn btn-danger w-100' onclick='cancelSQL()'>取消执行</button>" +  // 添加取消执行按钮
                            "<div id='loading-spinner' class='mt-4'>" +
                            "  <div class='spinner-border text-primary' role='status'>" +
                            "    <span class='visually-hidden'>Loading...</span>" +
                            "  </div>" +
                            "</div>" +
                            "<div id='result' class='card mt-4'>" +
                            "<h4 class='text-center'>SQL 执行结果将在此显示...</h4>" +
                            "</div>" +
                            "</div>" +
                            "</body>" +
                            "</html>"
            );
        }
    }


    // 取消SQL执行的Servlet
    public static class SQLCancelServlet extends HttpServlet {
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
        @Override
        protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
            String sql = req.getParameter("sql");

            resp.setContentType("text/html");
            resp.setStatus(HttpServletResponse.SC_OK);

            if (sql == null || sql.trim().isEmpty()) {
                resp.getWriter().println("<div class='alert alert-danger' role='alert'>未提供SQL语句！请返回并输入有效的SQL语句。</div>");
                return;
            }

            try {
                // 自动判断并加上 LIMIT 200
                sql = addLimitIfNecessary(sql);

                // 执行SQL语句
                Dataset<Row> result = spark.sql(sql);
                List<Row> rows = result.collectAsList();

                // 构建 SQL 执行结果的 HTML，并添加行号和加密内容
                StringBuilder resultHtml = new StringBuilder();
                resultHtml.append("<h3 class='text-center'>SQL 执行结果</h3>");
                resultHtml.append("<p><strong>执行的 SQL:</strong> " + sql + "</p>");
                resultHtml.append("<div class='table-responsive'><table class='table table-bordered table-striped'><thead><tr>");

                // 显示列名（加上行号）
                resultHtml.append("<th>#</th>");
                for (String colName : result.columns()) {
                    resultHtml.append("<th>" + colName + "</th>");
                }
                resultHtml.append("</tr></thead><tbody>");

                // 显示数据行并进行加密
                int rowIndex = 1;
                for (Row row : rows) {
                    resultHtml.append("<tr>");
                    resultHtml.append("<td>" + rowIndex++ + "</td>"); // 添加行号
                    for (int i = 0; i < row.size(); i++) {
                        String encryptedValue = Base64.getEncoder().encodeToString(row.get(i).toString().getBytes());
                        resultHtml.append("<td class='encrypted-cell'>" + encryptedValue + "</td>");
                    }
                    resultHtml.append("</tr>");
                }

                resultHtml.append("</tbody></table></div>");

                resp.getWriter().println(resultHtml.toString());
            } catch (Exception e) {
                resp.getWriter().println("<div class='alert alert-danger' role='alert'>SQL 执行错误！</div>");
                resp.getWriter().println("<pre>" + e.getMessage() + "</pre>");
            }
        }

        // 从逻辑计划层面判断是否需要加 LIMIT 200
        private String addLimitIfNecessary(String sql) {
            try {
                LogicalPlan logicalPlan = spark.sessionState().sqlParser().parsePlan(sql);
                boolean hasLimit = logicalPlan.collect(new AbstractPartialFunction<LogicalPlan, Boolean>() {
                    @Override
                    public Boolean apply(LogicalPlan plan) {
                        return Limit.class.isAssignableFrom(plan.getClass());
                    }

                    @Override
                    public boolean isDefinedAt(LogicalPlan plan) {
                        return Limit.class.isAssignableFrom(plan.getClass());
                    }
                }).nonEmpty();

                if (isQueryPlan(logicalPlan) && !hasLimit) {
                    return sql + " LIMIT 200";
                }
            } catch (Exception e) {
                System.err.println("SQL 解析失败: " + e.getMessage());
            }
            return sql;
        }

        // 判断逻辑计划是否是查询类计划
        private boolean isQueryPlan(LogicalPlan plan) {
            return plan instanceof Project ||
                    plan instanceof Aggregate ||
                    plan instanceof Filter ||
                    plan instanceof Sort ||
                    plan instanceof Union ||
                    plan instanceof Join ||
                    plan instanceof LogicalRelation ||
                    plan instanceof SubqueryAlias ||
                    plan instanceof LogicalRDD;
        }
    }
}
