package com.fishsun.bigdata.dao;

import com.fishsun.bigdata.field.FieldType;
import lombok.Builder;
import lombok.Data;
import lombok.ToString;

/**
 * Created by IntelliJ IDEA.
 *
 * @Author : zhangxinsen
 * @create : 2024/9/11 15:49
 * @Desc :
 */

@ToString
@Builder
@Data
public class IcebergFieldParams {
  public Integer fieldId;
  public Integer tblId;
  public String fieldName;
  public FieldType fieldType;
  public boolean isNullable;
  public boolean isPartitionKey;
  public boolean isPrimaryKey;
  public Integer fieldSeq;
  public String nodeExpression;
}
