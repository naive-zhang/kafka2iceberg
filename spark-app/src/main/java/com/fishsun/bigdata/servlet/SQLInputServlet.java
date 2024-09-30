package com.fishsun.bigdata.servlet;

import com.fishsun.bigdata.HTMLPage;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * 显示SQL输入页面的Servlet
 *
 * @Author : zhangxinsen
 * @create : 2024/9/30 22:19
 * @Desc :
 */
public class SQLInputServlet extends HttpServlet {
  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    resp.setContentType("text/html; charset=UTF-8");
    resp.setCharacterEncoding("UTF-8");
    resp.setStatus(HttpServletResponse.SC_OK);
    resp.getWriter().println(HTMLPage.getIndexPage());
  }
}