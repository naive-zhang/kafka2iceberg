package com.fishsun.bigdata;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.*;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.execution.LogicalRDD;
import scala.runtime.AbstractPartialFunction;

public class SQLUtils {

  // 从逻辑计划层面判断是否需要加 LIMIT 200
  public static String addLimitIfNecessary(SparkSession spark, String sql) {
    try {
      LogicalPlan logicalPlan = spark.sessionState().sqlParser().parsePlan(sql);
      boolean hasLimit = logicalPlan.collect(new AbstractPartialFunction<LogicalPlan, Boolean>() {
        @Override
        public Boolean apply(LogicalPlan plan) {
          return Limit.class.isAssignableFrom(plan.getClass());
        }

        @Override
        public boolean isDefinedAt(LogicalPlan plan) {
          return Limit.class.isAssignableFrom(plan.getClass());
        }
      }).nonEmpty();

      if (isQueryPlan(logicalPlan) && !hasLimit) {
        return sql + " LIMIT 200";
      }
    } catch (Exception e) {
      System.err.println("SQL 解析失败: " + e.getMessage());
    }
    return sql;
  }

  // 判断逻辑计划是否是查询类计划
  private static boolean isQueryPlan(LogicalPlan plan) {
    return plan instanceof Project ||
            plan instanceof Aggregate ||
            plan instanceof Filter ||
            plan instanceof Sort ||
            plan instanceof Union ||
            plan instanceof Join ||
            plan instanceof LogicalRelation ||
            plan instanceof SubqueryAlias ||
            plan instanceof LogicalRDD;
  }
}
