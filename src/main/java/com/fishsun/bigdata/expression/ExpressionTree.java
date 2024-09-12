package com.fishsun.bigdata.expression;

import lombok.Builder;
import lombok.Data;

/**
 * Created by IntelliJ IDEA.
 *
 * @Author : zhangxinsen
 * @create : 2024/9/12 17:02
 * @Desc :
 */
@Data
@Builder
public class ExpressionTree {
  private Integer level;
  private Integer height; // 树的高度

}
