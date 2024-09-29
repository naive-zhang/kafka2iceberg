package com.fishsun.bigdata;

import org.apache.spark.SparkConf;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import javax.xml.parsers.*;

public class ConfigLoader {

  // 读取 classpath 中的所有 XML 文件，并解析为配置项
  public static Map<String, String> loadConfigFromClasspath() {
    Map<String, String> configMap = new HashMap<>();
    try {
      // 获取 classpath 中所有的 XML 文件
      ClassLoader classLoader = ConfigLoader.class.getClassLoader();
      URL resource = classLoader.getResource("");
      if (resource != null) {
        File directory = new File(resource.toURI());
        if (directory.exists()) {
          System.out.println(directory.getAbsolutePath());
          // 递归读取所有 XML 文件
          loadAllXmlFiles(directory, configMap);
        }
      }
    } catch (URISyntaxException e) {
      System.err.println("Error loading XML files: " + e.getMessage());
    }
    return configMap;
  }

  // 递归加载目录中的所有 XML 文件
  private static void loadAllXmlFiles(File directory, Map<String, String> configMap) {
    if (directory == null || !directory.exists()) return;
    File[] files = directory.listFiles();
    if (files == null) return;

    for (File file : files) {
      if (file.isDirectory()) {
        // 如果是目录，递归处理
        loadAllXmlFiles(file, configMap);
      } else if (file.getName().endsWith(".xml")) {
        // 如果是 XML 文件，解析文件并提取配置项
        parseXmlFile(file, configMap);
      }
    }
  }

  // 解析单个 XML 文件，并提取 <property> 中的 name 和 value
  private static void parseXmlFile(File file, Map<String, String> configMap) {
    try {
      DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
      DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
      Document doc = dBuilder.parse(file);
      doc.getDocumentElement().normalize();

      // 读取所有的 <property> 节点
      NodeList propertyNodes = doc.getElementsByTagName("property");
      for (int i = 0; i < propertyNodes.getLength(); i++) {
        org.w3c.dom.Node node = propertyNodes.item(i);
        if (node.getNodeType() == org.w3c.dom.Node.ELEMENT_NODE) {
          org.w3c.dom.Element element = (org.w3c.dom.Element) node;

          // 提取 name 和 value 属性
          String name = element.getElementsByTagName("name").item(0).getTextContent();
          String value = element.getElementsByTagName("value").item(0).getTextContent();

          // 添加到配置项中
          configMap.put(name, value);
        }
      }
      System.out.println("Loaded config from: " + file.getAbsolutePath());
    } catch (Exception e) {
      System.err.println("Error parsing XML file " + file.getAbsolutePath() + ": " + e.getMessage());
    }
  }

  public static SparkConf loadConfFromClassloader() {
    Map<String, String> conf = loadConfigFromClasspath();
    SparkConf sparkConf = new SparkConf();
    for (Map.Entry<String, String> kvEntry : conf.entrySet()) {
      System.out.println("load key: " + kvEntry.getKey() + " value: " + kvEntry.getValue());
      sparkConf.set(kvEntry.getKey(), kvEntry.getValue());
    }
    return sparkConf;
  }
}
