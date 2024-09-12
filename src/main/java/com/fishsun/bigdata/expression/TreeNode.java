package com.fishsun.bigdata.expression;

import lombok.Builder;
import lombok.Data;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 *
 * @Author : zhangxinsen
 * @create : 2024/9/12 17:04
 * @Desc :
 */

@Data
@Builder
public class TreeNode<E> {
  private Integer level;
  private List<E>
}
