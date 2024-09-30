package com.fishsun.bigdata.servlet;

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

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

/**
 * 自定义的反向代理 Servlet
 *
 * @Author : zhangxinsen
 * @create : 2024/9/30 22:22
 * @Desc :
 */
// 自定义的反向代理 Servlet
public class ReverseProxyServlet extends HttpServlet {
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