package com.fishsun.bigdata.field;

/**
 * Created by IntelliJ IDEA.
 *
 * @Author : zhangxinsen
 * @create : 2024/9/11 15:43
 * @Desc :
 */
public enum FieldType {
  BOOL(0),
  INT(10),
  BIGINT(20),
  DECIMAL(30),
  STRING(40),
  TIMESTAMP_NTZ(50),
  DATE(60);

  private final Integer typeVal;

  // 构造方法
  FieldType(Integer typeVal) {
    this.typeVal = typeVal;
  }

  // 获取枚举的描述信息
  public Integer getTypeVal() {
    return typeVal;
  }

  // 通过描述信息获取对应的枚举类型
  public static FieldType fromVal(Integer typeVal) {
    for (FieldType fieldType : FieldType.values()) {
      if (fieldType.getTypeVal().equals(typeVal)) {
        return fieldType;
      }
    }
    throw new IllegalArgumentException("Unknown type: " + typeVal);
  }


  public static void main(String[] args) {
    System.out.println(DECIMAL.toString());
  }
}
