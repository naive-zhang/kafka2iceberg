package com.fishsun.bigdata.utils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

/**
 * Created by IntelliJ IDEA.
 *
 * @Author : zhangxinsen
 * @create : 2024/9/11 15:51
 * @Desc :
 */
public class FieldUtils {
  public static TypeInformation fieldType2typeInformation(String dataType) {
    switch (dataType) {
      case "string":
        return Types.STRING;
      case "bigint":
      case "long":
        return Types.LONG;
      case "tinyint":
      case "int":
        return Types.INT;
      case "bool":
      case "boolean":
        return Types.BOOLEAN;
      case "tiemstamp_ntz":
        return Types.LOCAL_DATE_TIME;
      case "date":
        return Types.LOCAL_DATE;
      default:
        if (dataType.startsWith("decimal")) {
          return Types.BIG_DEC;
        } else {
          return Types.STRING;
        }
    }
  }

  public static DataType fieldType2dataType(String dataType) {
    switch (dataType) {
      case "string":
        return DataTypes.STRING();
      case "bigint":
      case "long":
        return DataTypes.BIGINT();
      case "tinyint":
      case "int":
        return DataTypes.INT();
      case "bool":
      case "boolean":
        return DataTypes.BOOLEAN();
      case "timestamp_ntz":
        return DataTypes.TIMESTAMP(6);
      case "date":
        return DataTypes.DATE();
      default:
        if (dataType.startsWith("decimal")) {
          String precisions = dataType.trim().replace("decimal(","").replace(")","");
          String[] split = precisions.split(",");
          return DataTypes.DECIMAL(Integer.valueOf(split[0]), Integer.valueOf(split[1]));
        } else {
          return DataTypes.STRING();
        }
    }
  }


  public static void main(String[] args) {
    String a = "1232";
    System.out.println(a.substring(2));
  }
}
