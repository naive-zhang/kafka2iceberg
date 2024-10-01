package com.fishsun.bigdata.servlet;

import com.fishsun.bigdata.JSONUtils;
import com.fishsun.bigdata.SQLUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.*;

/**
 * 执行多个SQL语句和Shell命令的Servlet
 *
 * @Author : zhangxinsen
 * @create : 2024/9/30 22:20
 * @Desc :
 */
public class SQLExecuteAllServlet extends HttpServlet {
  private SparkSession spark;

  public SQLExecuteAllServlet(SparkSession spark) {
    this.spark = spark;
  }

  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    String sqlInput = req.getParameter("sql");
    String jobGroup = req.getParameter("jobGroup");

    resp.setContentType("application/json");
    resp.setCharacterEncoding("UTF-8");
    resp.setStatus(HttpServletResponse.SC_OK);

    if (sqlInput == null || sqlInput.trim().isEmpty()) {
      resp.getWriter().println("{\"error\":\"未提供SQL语句！请返回并输入有效的SQL语句。\"}");
      return;
    }

    // Split the input by ';' to get individual statements
    String[] statements = sqlInput.split("(?<!\\\\);"); // Handle escaped semicolons if necessary

    List<Map<String, Object>> results = new ArrayList<>();

    try {
      // 设置 Job Group
      String groupId = (jobGroup != null && !jobGroup.isEmpty()) ? jobGroup : UUID.randomUUID().toString();
      spark.sparkContext().setJobGroup(groupId, "Execute All Statements", true);

      for (String stmt : statements) {
        stmt = stmt.trim();
        if (stmt.isEmpty()) {
          continue;
        }

        if (stmt.startsWith("!")) {
          // Shell command
          String command = stmt.substring(1).trim();
          Map<String, Object> cmdResult = new HashMap<>();
          cmdResult.put("type", "shell");
          cmdResult.put("command", command);

          try {
            ProcessBuilder pb = new ProcessBuilder();
            // 如果需要在shell中执行命令，可以使用 bash -c "command"
            pb.command("bash", "-c", command);
            pb.redirectErrorStream(true);
            Process process = pb.start();

            // 读取输出
            Scanner scanner = new Scanner(process.getInputStream()).useDelimiter("\\A");
            String output = scanner.hasNext() ? scanner.next() : "";
            scanner.close();

            int exitCode = process.waitFor();
            cmdResult.put("exitCode", exitCode);
            cmdResult.put("output", output);

            if (exitCode != 0) {
              cmdResult.put("error", "Shell命令以退出码 " + exitCode + " 失败。");
            }

          } catch (Exception e) {
            cmdResult.put("error", "Shell命令执行失败: " + e.getMessage());
          }

          results.add(cmdResult);
        } else {
          // SQL statement
          Map<String, Object> sqlResult = new HashMap<>();
          sqlResult.put("type", "sql");
          sqlResult.put("statement", stmt);

          try {
            // 自动判断并加上 LIMIT 200
            String sql = SQLUtils.addLimitIfNecessary(spark, stmt);

            // 执行SQL语句
            Dataset<Row> result = spark.sql(sql);

            // 收集结果，限制最多返回100行
            List<Row> rows = result.limit(100).collectAsList();

            // 构建 JSON 格式的结果
            sqlResult.put("columns", Arrays.asList(result.columns()));
            List<List<String>> data = new ArrayList<>();
            for (Row row : rows) {
              List<String> rowData = new ArrayList<>();
              for (int i = 0; i < row.size(); i++) {
                Object value = row.get(i);
                rowData.add(value != null ? value.toString() : "null");
              }
              data.add(rowData);
            }
            sqlResult.put("data", data);
          } catch (Exception e) {
            sqlResult.put("error", "SQL执行错误: " + e.getMessage());
          }

          results.add(sqlResult);
        }
      }

      // 输出结果
      Map<String, Object> responseMap = new HashMap<>();
      responseMap.put("results", results);
      resp.getWriter().println(JSONUtils.toJson(responseMap));

    } catch (Exception e) {
      e.printStackTrace();
      resp.getWriter().println("{\"error\":\"执行所有SQL语句时发生错误: " + e.getMessage() + "\"}");
    } finally {
      // 清除 Job Group
      spark.sparkContext().clearJobGroup();
    }
  }
}
