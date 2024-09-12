package com.fishsun.bigdata.mock;

import com.fishsun.bigdata.utils.ConnectionUtils;

import java.sql.SQLException;
import java.util.Map;

import static com.fishsun.bigdata.utils.ParamUtils.parseConfig;

/**
 * Created by IntelliJ IDEA.
 *
 * @Author : zhangxinsen
 * @create : 2024/9/7 23:00
 * @Desc :
 */
public class CDCMockApp {


  public static void main(String[] args) throws SQLException, ClassNotFoundException, InterruptedException {
    Map<String, String> paramMap = parseConfig(args);
    Class.forName("com.mysql.cj.jdbc.Driver");
    if (paramMap.getOrDefault("init_table", "false").equals("true")) {
      ConnectionUtils.initTable(paramMap);
    }
    if (paramMap.getOrDefault("add_col", "false").equals("true")) {
      ConnectionUtils.addSignTime(paramMap);
    }
    if (paramMap.getOrDefault("mock_old", "false").equals("true")) {
      for (int i = 0; i < 100; i++) {
        int executeNum = (int) (Math.round(Math.random() * 100) + 100);
        ConnectionUtils.batchExecute(
                paramMap,
                executeNum,
                false
        );
        Thread.sleep((long) (Math.random() * 10000));
      }
    }
    if (paramMap.getOrDefault("mock_new", "false").equals("true")) {
      int executeNum = (int) (Math.round(Math.random() * 5) + 1);
      ConnectionUtils.batchExecute(
              paramMap,
              executeNum,
              true
      );
    }
  }
}
