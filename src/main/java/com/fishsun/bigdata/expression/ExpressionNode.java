package com.fishsun.bigdata.expression;

import lombok.Builder;
import lombok.Data;
import lombok.ToString;

/**
 * Created by IntelliJ IDEA.
 *
 * @Author : zhangxinsen
 * @create : 2024/9/11 16:14
 * @Desc :
 */

@Data
@Builder
@ToString
/**
 * 表达式节点
 * 字面值表达式的话 只有左节点
 * 等值表达式的话 有右节点
 */
public class ExpressionNode {
  private ExpressionType expressionType;
  private ExpressionNode leftExpressionNode;
  private ExpressionNode rightExpressionNode;
  private String operator;
}
