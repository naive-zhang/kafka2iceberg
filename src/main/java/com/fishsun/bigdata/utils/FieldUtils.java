package com.fishsun.bigdata.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fishsun.bigdata.dao.IcebergFieldParams;
import com.fishsun.bigdata.expression.ExpressionNode;
import com.fishsun.bigdata.expression.ExpressionType;
import com.fishsun.bigdata.field.FieldType;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.types.Row;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 *
 * @Author : zhangxinsen
 * @create : 2024/9/11 15:51
 * @Desc :
 */
public class FieldUtils {
  public static TypeInformation fieldType2typeInformation(FieldType fieldType) {
    switch (fieldType) {
      case BOOL:
        return Types.BOOLEAN;
      case INT:
        return Types.INT;
      case BIGINT:
        return Types.BIG_INT;
      case DECIMAL:
        return Types.BIG_DEC;
      case STRING:
        return Types.STRING;
      case TIMESTAMP_NTZ:
        return Types.LOCAL_DATE_TIME;
      case DATE:
        return Types.LOCAL_DATE;
    }
    throw new IllegalArgumentException("unknown field type");
  }

  /**
   * 对多棵树进行合并
   * 并且合并成一颗层次遍历的数据
   * @param expressionNodeList
   */
  public static void mergeTrees(List<ExpressionNode> expressionNodeList) {

  }

  /**
   * 组装行对象
   *
   * @param row
   * @param icebergFieldParams
   * @param jsonNode
   */
  public static void setupRow(Row row, IcebergFieldParams icebergFieldParams, JsonNode jsonNode) {
    ExpressionNode expressionNode = ExpressionUtils.parseTree(icebergFieldParams.getNodeExpression());
    JsonNode leftLeafNode = parseLeafVal(expressionNode, jsonNode);
    if (expressionNode.getExpressionType() == ExpressionType.EQUAL) {
      boolean isEquals = leftLeafNode.asText().trim().equalsIgnoreCase(expressionNode.getRightExpressionNode().getOperator().trim());
      if (icebergFieldParams.fieldType == FieldType.BOOL) {
        row.setField(icebergFieldParams.getFieldSeq(), isEquals);
      } else {
        // 0-1的int来代替bool
        row.setField(icebergFieldParams.getFieldSeq(), isEquals ? 1 : 0);
      }
      return;
    }
    switch (icebergFieldParams.fieldType) {
      case BOOL:
        row.setField(icebergFieldParams.getFieldSeq(), leftLeafNode.asBoolean());
      case INT:
        row.setField(icebergFieldParams.getFieldSeq(), leftLeafNode.asInt());
      case BIGINT:
        row.setField(icebergFieldParams.getFieldSeq(), leftLeafNode.asLong());
      case DECIMAL:
        row.setField(icebergFieldParams.getFieldSeq(), new BigDecimal(leftLeafNode.asText()));
      case STRING:
        row.setField(icebergFieldParams.getFieldSeq(), leftLeafNode.asText());
      case TIMESTAMP_NTZ:
        row.setField(icebergFieldParams.getFieldSeq(), Timestamp.valueOf(leftLeafNode.asText()).toLocalDateTime());
      case DATE:
        row.setField(icebergFieldParams.getFieldSeq(), new Date(Timestamp.valueOf(leftLeafNode.asText()).getTime()).toLocalDate());
      default:
        row.setField(icebergFieldParams.getFieldSeq(), leftLeafNode.asText());
    }
  }

  /**
   * 解析叶子节点的值
   *
   * @param expressionNode
   * @param jsonNode
   * @return
   */
  private static JsonNode parseLeafVal(ExpressionNode expressionNode, JsonNode jsonNode) {
    if (expressionNode == null) return null;
    String key = expressionNode.getOperator().substring(2);
    if (expressionNode.getLeftExpressionNode() == null) {
      return jsonNode.get(key);
    }
    return parseLeafVal(expressionNode.getLeftExpressionNode(), jsonNode.get(key));
  }

  public static void main(String[] args) {
    String a = "1232";
    System.out.println(a.substring(2));
  }
}
