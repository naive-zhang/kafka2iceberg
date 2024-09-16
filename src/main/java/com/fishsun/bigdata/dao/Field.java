package com.fishsun.bigdata.dao;

import lombok.Builder;
import lombok.Data;

/**
 * Created by IntelliJ IDEA.
 *
 * @Author : zhangxinsen
 * @create : 2024/9/14 23:14
 * @Desc :
 */


@Data
@Builder
public class Field {
  private String fieldName;
  private Integer seq;
  private String type;
  private String ref;
}
