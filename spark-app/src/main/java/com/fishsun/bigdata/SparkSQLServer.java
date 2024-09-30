package com.fishsun.bigdata;

import com.fishsun.bigdata.servlet.ReverseProxyServlet;
import com.fishsun.bigdata.servlet.SQLCancelServlet;
import com.fishsun.bigdata.servlet.SQLExecuteServlet;
import com.fishsun.bigdata.servlet.SQLInputServlet;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import scala.Option;

import java.net.URL;
import java.util.Arrays;

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
      sparkWebUrl = sparkWebUrl.replace("/redirect/", "/");
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

}
