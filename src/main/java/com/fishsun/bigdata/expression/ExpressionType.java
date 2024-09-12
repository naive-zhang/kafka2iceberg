package com.fishsun.bigdata.expression;

/**
 * Created by IntelliJ IDEA.
 *
 * @Author : zhangxinsen
 * @create : 2024/9/11 16:10
 * @Desc :
 */
public enum ExpressionType {
  EQUAL(10), // 比较表达式
  LATERAL(20); // 字面值表达式

  private final Integer typeVal;

  // 构造方法
  ExpressionType(Integer typeVal) {
    this.typeVal = typeVal;
  }

  // 获取枚举的描述信息
  public Integer getTypeVal() {
    return typeVal;
  }

  // 通过描述信息获取对应的枚举类型
  public static ExpressionType fromVal(Integer typeVal) {
    for (ExpressionType expressionType : ExpressionType.values()) {
      if (expressionType.getTypeVal().equals(typeVal)) {
        return expressionType;
      }
    }
    throw new IllegalArgumentException("Unknown expression type: " + typeVal);
  }
}
