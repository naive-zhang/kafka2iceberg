package com.fishsun.bigdata.servlet;

import org.apache.spark.sql.SparkSession;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * 取消SQL执行的Servlet
 *
 * @Author : zhangxinsen
 * @create : 2024/9/30 22:21
 * @Desc :
 */
// 取消SQL执行的Servlet
public class SQLCancelServlet extends HttpServlet {
  private SparkSession spark;

  public SQLCancelServlet(SparkSession spark) {
    this.spark = spark;
  }

  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    resp.setCharacterEncoding("UTF-8");
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