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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * 执行SQL并返回结果的Servlet
 *
 * @Author : zhangxinsen
 * @create : 2024/9/30 22:20
 * @Desc :
 */
// 执行SQL并返回结果的Servlet
public class SQLExecuteServlet extends HttpServlet {
  private SparkSession spark;

  public SQLExecuteServlet(SparkSession spark) {
    this.spark = spark;
  }

  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    String sql = req.getParameter("sql");
    String jobGroup = req.getParameter("jobGroup");

    resp.setContentType("application/json");
    resp.setCharacterEncoding("UTF-8");
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
      spark.sparkContext().setJobGroup(groupId, sql, true);

      // 执行SQL语句
      Dataset<Row> result = spark.sql(sql);

      // 收集结果，限制最多返回100行
      List<Row> rows = result.collectAsList();

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