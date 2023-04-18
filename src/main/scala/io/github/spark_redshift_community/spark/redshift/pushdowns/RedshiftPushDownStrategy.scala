/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off line.size.limit
// https://github.com/ActionIQ-OSS/spark-snowflake/blob/spark_2.4/src/main/scala/net/snowflake/spark/snowflake/pushdowns/SnowflakeStrategy.scala
// scalastyle:on line.size.limit
package io.github.spark_redshift_community.spark.redshift.pushdowns

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.SparkPlan
import io.github.spark_redshift_community.spark.redshift.pushdowns.querygeneration.QueryBuilder
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.InternalRow

// Entry point of the pushdowns, taking in a logical plan and returns a spark physical plan.
class RedshiftPushDownStrategy(conf: SparkConf) extends Strategy {

  def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    if (conf.get("spark.aiq.sql.enable_jdbc_pushdown", "false").toBoolean) {
      try {
        buildQuery(plan.transform({
          case Project(Nil, child) => child
          case SubqueryAlias(_, child) => child
        })).getOrElse(Nil)
      } catch {
        case e: Exception =>
          log.warn(s"Pushdown failed: ${e.getMessage}")
          Nil
      }
    } else {
      Nil
    }
  }

  private def buildQuery(plan: LogicalPlan): Option[Seq[RedshiftPushDownPlan]] = {
    QueryBuilder.getRDDFromPlan(plan).map {
      case (output: Seq[Attribute], rdd: RDD[InternalRow]) =>
        Seq(RedshiftPushDownPlan(output, rdd))
    }
  }
}
