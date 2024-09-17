package com.fishsun.bigdata.utils;

import com.fishsun.bigdata.dao.BusiDetail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;

/**
 * Created by IntelliJ IDEA.
 *
 * @Author : zhangxinsen
 * @create : 2024/9/7 23:12
 * @Desc :
 */
public class ConnectionUtils {
  public static Connection getConnection(Map<String, String> paramsMap) throws SQLException {
    String url = String.format("jdbc:mysql://%s:%s", paramsMap.get("host"), paramsMap.get("port"));
    Properties properties = new Properties();
    properties.put("user", paramsMap.get("user"));
    properties.put("password", paramsMap.get("password"));
    Connection connection = DriverManager.getConnection(url, properties);
    return connection;
  }

  public static void initTable(Map<String, String> paramsMap) throws SQLException {
    Connection connection = getConnection(paramsMap);
    Statement statement = connection.createStatement();
    statement.execute("drop table if exists test.t_busi_detail");
    statement.execute("CREATE TABLE test.t_busi_detail\n" +
            "(\n" +
            "    `bid`             bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',\n" +
            "    `user_id`         int(11)    NOT NULL COMMENT '用户id',\n" +
            "    `goods_id`        int(11)    NOT NULL COMMENT '商品ID',\n" +
            "    `goods_cnt`       int(11)    NOT NULL DEFAULT '0' COMMENT '商品数量',\n" +
            "    `fee`             decimal(16, 4)      DEFAULT '0.0000' COMMENT '费用',\n" +
            "    `collection_time` datetime            DEFAULT CURRENT_TIMESTAMP COMMENT '加购时间',\n" +
            "    `order_time`      datetime            DEFAULT CURRENT_TIMESTAMP COMMENT '下单时间',\n" +
            "    `is_valid`        int(11)             DEFAULT '1' COMMENT '交易是否有效. 1有效,2无效',\n" +
            "    `create_time`     datetime   NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',\n" +
            "    `update_time`     datetime            DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',\n" +
            "    PRIMARY KEY (`bid`)\n" +
            ") ENGINE = InnoDB\n" +
            "  DEFAULT CHARSET = utf8mb4\n" +
            "  COLLATE = utf8mb4_bin");
    statement.close();
    connection.close();
  }

  public static void addSignTime(Map<String, String> paramMap) throws SQLException {
    Connection connection = getConnection(paramMap);
    Statement statement = connection.createStatement();
    statement.execute("alter table test.t_busi_detail\n" +
            "    add sign_time datetime null comment '签收时间' after is_valid");
    statement.close();
    connection.close();
  }

  public static void update(Map<String, String> paramMap) throws SQLException {
    Connection connection = getConnection(paramMap);
    Statement statement = connection.createStatement();
    statement.execute("UPDATE test.t_busi_detail\n" +
            "SET fee = fee+rand()\n" +
            "ORDER BY RAND()\n" +
            "LIMIT 1");
    statement.close();
    connection.close();
  }

  public static void delete(Map<String, String> paramMap) throws SQLException {
    Connection connection = getConnection(paramMap);
    Statement statement = connection.createStatement();
    statement.execute("delete from test.t_busi_detail\n" +
            "ORDER BY RAND()\n" +
            "LIMIT 1");
    statement.close();
    connection.close();
  }

  public static void batchExecute(Map<String, String> paramMap, int executeNum, boolean isAddSignTime) throws SQLException {
    Connection connection = getConnection(paramMap);
    connection.setAutoCommit(true);
    double insertProb = 0.7, updateProb = 0.2, deleteProb = 0.05;
    PreparedStatement preparedStatement = null;
    if (isAddSignTime) {
      preparedStatement = connection.prepareStatement("insert into test.t_busi_detail (" +
              "user_id" +
              ", goods_id" +
              ", goods_cnt" +
              ", fee" +
              ", collection_time" +
              ", order_time" +
              ", is_valid" +
              ", create_time\n" +
              ", sign_time )\n" +
              "values (?,?,?,?,?, ?, ?, ?, ?)");
    } else {
      preparedStatement = connection.prepareStatement("insert into test.t_busi_detail (" +
              "user_id" +
              ", goods_id" +
              ", goods_cnt" +
              ", fee" +
              ", collection_time" +
              ", order_time" +
              ", is_valid" +
              ", create_time\n" +
              "                                )\n" +
              "values (?,?,?,?,?, ?, ?, ?)");
    }
    for (int i = 0; i < executeNum; i++) {
      double random = Math.random();
      if (random <= insertProb) {
        BusiDetail busiDetail = BusiDetail.randomGen();
        preparedStatement.setInt(1, busiDetail.getUser_id());
        preparedStatement.setInt(2, busiDetail.getGoods_id());
        preparedStatement.setInt(3, busiDetail.getGoods_cnt());
        preparedStatement.setBigDecimal(4, busiDetail.getFee());
        preparedStatement.setTimestamp(5, busiDetail.getCollection_time());
        preparedStatement.setTimestamp(6, busiDetail.getOrder_time());
        preparedStatement.setInt(7, busiDetail.getIs_valid());
        preparedStatement.setTimestamp(8, busiDetail.getCreate_time());
        if (isAddSignTime) {
          preparedStatement.setTimestamp(9, busiDetail.getSign_time());
        }
        preparedStatement.execute();
      } else if (random <= insertProb + updateProb) {
        update(paramMap);
      } else {
        delete(paramMap);
      }
    }
    connection.close();
  }
}
