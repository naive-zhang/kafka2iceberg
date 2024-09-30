package com.fishsun.bigdata;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import scala.Option;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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
    URL resource = SparkSQLServer.class.getClassLoader().getResource("");
    if (resource != null) {
      context.setResourceBase(resource.toExternalForm());
    } else {
      System.err.println("无法找到资源路径。");
      System.exit(1);
    }

    // 增加一个 DefaultServlet 处理静态资源
    context.addServlet(new ServletHolder("default", new DefaultServlet()), "/static/*");

    // 映射路径到相应的Servlet
    context.addServlet(new ServletHolder(new SQLInputServlet()), "/");
    context.addServlet(new ServletHolder(new SQLExecuteServlet(spark)), "/execute");
    context.addServlet(new ServletHolder(new SQLCancelServlet(spark)), "/cancel");

    // 获取 Spark Web UI 的地址
    Option<String> sparkWebUrlOption = spark.sparkContext().uiWebUrl();
    if (sparkWebUrlOption.isDefined()) {
      String sparkWebUrl = sparkWebUrlOption.get();
      System.out.println("Spark Web UI 地址: " + sparkWebUrl);

      // 添加自定义的反向代理 Servlet，将 /proxy/* 映射到 Spark Web UI
      ServletHolder proxyServletHolder = new ServletHolder(new ReverseProxyServlet(sparkWebUrl, "/proxy"));
      context.addServlet(proxyServletHolder, "/proxy/*");
    } else {
      System.err.println("无法获取 Spark Web UI 地址。");
    }

    server.setHandler(context);
    System.out.println("启动 Spark SQL Web 服务器, 请访问: http://localhost:" + serverPort);
    server.start();
    server.join();
  }

  // 自定义的反向代理 Servlet
  public static class ReverseProxyServlet extends HttpServlet {
    private final String targetBase;
    private final String proxyBase;
    private final CloseableHttpClient httpClient;

    public ReverseProxyServlet(String targetBase, String proxyBase) {
      this.targetBase = targetBase.endsWith("/") ? targetBase.substring(0, targetBase.length() - 1) : targetBase;
      this.proxyBase = proxyBase.endsWith("/") ? proxyBase.substring(0, proxyBase.length() - 1) : proxyBase;
      this.httpClient = HttpClients.createDefault();
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
      proxyRequest(req, resp, "GET");
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
      proxyRequest(req, resp, "POST");
    }

    @Override
    protected void doPut(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
      proxyRequest(req, resp, "PUT");
    }

    @Override
    protected void doDelete(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
      proxyRequest(req, resp, "DELETE");
    }

    private void proxyRequest(HttpServletRequest req, HttpServletResponse resp, String method) throws IOException {
      String targetPath = req.getPathInfo();
      if (targetPath == null) targetPath = "";
      String query = req.getQueryString();
      String targetUrl = targetBase + targetPath + (query != null ? "?" + query : "");

      System.out.println("代理请求: " + method + " " + targetUrl);

      // 创建相应的 HttpClient 请求
      HttpRequestBase proxyRequest;
      switch (method.toUpperCase()) {
        case "POST":
          HttpPost post = new HttpPost(targetUrl);
          String postBody = readRequestBody(req);
          if (postBody != null && !postBody.isEmpty()) {
            post.setEntity(new StringEntity(postBody, StandardCharsets.UTF_8));
          }
          proxyRequest = post;
          break;
        case "PUT":
          HttpPut put = new HttpPut(targetUrl);
          String putBody = readRequestBody(req);
          if (putBody != null && !putBody.isEmpty()) {
            put.setEntity(new StringEntity(putBody, StandardCharsets.UTF_8));
          }
          proxyRequest = put;
          break;
        case "DELETE":
          proxyRequest = new HttpDelete(targetUrl);
          break;
        case "GET":
        default:
          proxyRequest = new HttpGet(targetUrl);
          break;
      }

      // 复制请求头
      Collections.list(req.getHeaderNames()).forEach(headerName -> {
        if (!headerName.equalsIgnoreCase("Host")) { // Host 由 HttpClient 自动处理
          Collections.list(req.getHeaders(headerName)).forEach(headerValue -> {
            proxyRequest.addHeader(headerName, headerValue);
          });
        }
      });

      try (CloseableHttpResponse proxyResponse = httpClient.execute(proxyRequest)) {
        // 设置响应状态码
        resp.setStatus(proxyResponse.getStatusLine().getStatusCode());

        // 复制响应头
        for (Header header : proxyResponse.getAllHeaders()) {
          if (!header.getName().equalsIgnoreCase("Content-Length")) { // Content-Length 自动处理
            if (header.getName().equalsIgnoreCase("Location")) {
              // 重写重定向的 Location 头部
              String location = header.getValue();
              if (location.startsWith(targetBase)) {
                location = proxyBase + location.substring(targetBase.length());
                resp.setHeader("Location", location);
                System.out.println("重写 Location 头部为: " + location);
                continue;
              }
            }
            resp.setHeader(header.getName(), header.getValue());
          }
        }

        // 处理响应内容
        HttpEntity entity = proxyResponse.getEntity();
        if (entity != null) {
          String contentType = entity.getContentType() != null ? entity.getContentType().getValue() : "";
          InputStream inputStream = entity.getContent();
          String responseBody = convertStreamToString(inputStream, getCharsetFromContentType(contentType));

          if (contentType.contains("text/html")) {
            // 重写响应内容中的绝对路径
            responseBody = responseBody.replaceAll("href=\"/", "href=\"" + proxyBase + "/");
            responseBody = responseBody.replaceAll("src=\"/", "src=\"" + proxyBase + "/");
            responseBody = responseBody.replaceAll("action=\"/", "action=\"" + proxyBase + "/");
            responseBody = responseBody.replaceAll("url\\(/", "url\\(" + proxyBase + "/");
            // 还可以根据需要添加更多的替换规则
            System.out.println("重写响应内容中的绝对路径。");
          }

          // 设置响应内容类型和编码
          resp.setContentType(contentType);
          resp.setCharacterEncoding("UTF-8"); // 确保使用 UTF-8 编码

          // 写入响应内容
          resp.getWriter().write(responseBody);
        }
      } catch (Exception e) {
        e.printStackTrace();
        resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "代理请求失败: " + e.getMessage());
      }
    }

    private String readRequestBody(HttpServletRequest req) throws IOException {
      StringBuilder sb = new StringBuilder();
      BufferedReader reader = req.getReader();
      String line;
      while ((line = reader.readLine()) != null) {
        sb.append(line).append("\n");
      }
      return sb.toString();
    }

    private String convertStreamToString(InputStream is, String charset) throws IOException {
      if (is == null) return "";
      StringBuilder sb = new StringBuilder();
      try (BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
        String line;
        while ((line = reader.readLine()) != null) {
          sb.append(line).append("\n");
        }
      }
      return sb.toString();
    }

    private String getCharsetFromContentType(String contentType) {
      if (contentType == null) return null;
      String[] parts = contentType.split(";");
      for (String part : parts) {
        part = part.trim();
        if (part.startsWith("charset=")) {
          return part.substring("charset=".length());
        }
      }
      return null;
    }

    @Override
    public void destroy() {
      try {
        httpClient.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
      super.destroy();
    }
  }

  // 显示SQL输入页面的Servlet
  public static class SQLInputServlet extends HttpServlet {
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
      resp.setContentType("text/html; charset=UTF-8");
      resp.setCharacterEncoding("UTF-8");
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
