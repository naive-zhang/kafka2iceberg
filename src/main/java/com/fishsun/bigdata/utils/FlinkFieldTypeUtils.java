package com.fishsun.bigdata.utils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.TypeConversions;

/**
 * Created by IntelliJ IDEA.
 *
 * @Author : zhangxinsen
 * @create : 2024/9/10 21:41
 * @Desc :
 */
public class FlinkFieldTypeUtils {
  public static void main(String[] args) {
    // 定义 DataTypes 类型
    DataType dataType = DataTypes.TIMESTAMP(6);

    // 转换为 TypeInformation
    TypeInformation<?> typeInfo = TypeConversions.fromDataTypeToLegacyInfo(dataType);

    System.out.println("TypeInformation: " + typeInfo);
  }
}
