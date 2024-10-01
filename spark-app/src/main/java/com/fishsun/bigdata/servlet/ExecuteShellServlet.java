package com.fishsun.bigdata.servlet;

import com.fishsun.bigdata.JSONUtils;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

/**
 * 执行Shell命令的Servlet
 */
public class ExecuteShellServlet extends HttpServlet {

  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    String command = req.getParameter("command");

    resp.setContentType("application/json");
    resp.setCharacterEncoding("UTF-8");
    resp.setStatus(HttpServletResponse.SC_OK);

    if (command == null || command.trim().isEmpty()) {
      resp.getWriter().println("{\"error\":\"未提供Shell命令！请返回并输入有效的Shell命令。\"}");
      return;
    }

    try {
      ProcessBuilder builder = new ProcessBuilder();
      // 使用系统的Shell执行命令
      builder.command("sh", "-c", command);
      builder.redirectErrorStream(true);
      Process process = builder.start();

      // 读取命令的输出
      BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
      StringBuilder output = new StringBuilder();
      String line;
      while ((line = reader.readLine()) != null) {
        output.append(line).append("\n");
      }

      int exitCode = process.waitFor();
      Map<String, String> respMap = new HashMap<>();
      if (exitCode == 0) {
        // 成功执行
        respMap.put("output", output.toString());
        resp.getWriter().println(JSONUtils.toJson(respMap));
      } else {
        // 执行失败
        respMap.put("error", "Shell命令执行失败，退出码: " + exitCode);
        resp.getWriter().println(JSONUtils.toJson(respMap));
      }
    } catch (Exception e) {
      e.printStackTrace();
      resp.getWriter().println("{\"error\":\"Shell 命令执行错误：" + e.getMessage() + "\"}");
    }
  }
}
