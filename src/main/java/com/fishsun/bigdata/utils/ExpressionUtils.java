package com.fishsun.bigdata.utils;

import com.fishsun.bigdata.expression.ExpressionNode;
import com.fishsun.bigdata.expression.ExpressionType;

/**
 * Created by IntelliJ IDEA.
 *
 * @Author : zhangxinsen
 * @create : 2024/9/11 16:12
 * @Desc :
 */
public class ExpressionUtils {

  /**
   * 获得表达式的类型
   *
   * @param expression
   * @return
   */
  public static ExpressionType getType(String expression) {
    if (expression.contains("==")) return ExpressionType.EQUAL;
    return ExpressionType.LATERAL;
  }

  public static ExpressionNode parseTree(String expression) {
    if (null == expression || expression.trim().isEmpty()) {
      return null;
    }
    if (getType(expression) == ExpressionType.EQUAL) {
      ExpressionNode node = ExpressionNode.builder()
              .expressionType(ExpressionType.EQUAL)
              .build();
      String[] split = expression.split("==");
      String rightExpression = split[1];
      String leftExpression = split[0];
      ExpressionNode rightNode = ExpressionNode.builder()
              .operator(rightExpression.trim())
              .expressionType(ExpressionType.LATERAL)
              .leftExpressionNode(null)
              .rightExpressionNode(null)
              .build();
      node.setRightExpressionNode(rightNode);
      node.setLeftExpressionNode(parseTree(leftExpression.trim()));
      return node;
    }
    // 从$.开始依次往下解析
    if (!expression.startsWith("$.")) {
      throw new IllegalArgumentException("暂时只支持从JSON中解析数据的表达式");
    }
    String[] split = expression.replace("$.", "").split("\\.");
    String remainExpression = null;
    if (split.length > 1) {
      String[] tmp = new String[split.length - 1];
      for (int i = 0; i < tmp.length; i++) {
        tmp[i] = split[i + 1];
      }
      remainExpression = "$." + String.join(".", tmp);
    }
    return ExpressionNode.builder()
            .rightExpressionNode(null)
            .expressionType(ExpressionType.LATERAL)
            .operator("$." + split[0])
            .leftExpressionNode(parseTree(remainExpression))
            .build();
  }


  public static void main(String[] args) {
    ExpressionNode expressionNode = parseTree("$.person.id == 32");
    System.out.println(expressionNode);
  }
}
