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

import io.github.spark_redshift_community.spark.redshift.RedshiftRelation
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.SparkPlan
import io.github.spark_redshift_community.spark.redshift.pushdowns.querygeneration.QueryBuilder
import org.apache.spark.{DataSourceTelemetryHelpers, SparkContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.LogicalRelation

// Entry point of the pushdowns, taking in a logical plan and returns a spark physical plan.
class RedshiftPushDownStrategy(sparkContext: SparkContext)
  extends Strategy
    with DataSourceTelemetryHelpers {

  def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    if (sparkContext.getConf.get("spark.aiq.sql.enable_jdbc_pushdown", "false").toBoolean) {
      try {
        buildQuery(plan.transform({
          case Project(Nil, child) => child
          case SubqueryAlias(_, child) => child
        })).getOrElse(Nil)
      } catch {
        case e: Exception =>
          if (foundRedshiftRelation(plan)) {
            log.warn(
              logEventNameTagger(
                s"PushDown failed: ${e.getMessage}\n${e.getStackTrace.mkString("\n")}"
              )
            )
          }
          Nil
      }
    } else {
      Nil
    }
  }

  private def buildQuery(plan: LogicalPlan): Option[Seq[RedshiftPushDownPlan]] = {
    QueryBuilder.getRDDFromPlan(plan).map {
      case (output: Seq[Attribute], rdd: RDD[InternalRow], statement: String) =>
        Seq(RedshiftPushDownPlan(output, rdd, statement))
    }.orElse {
      // Set `dataSourceTelemetry.checkForPushDownFailures` to `true` for when QueryBuilder fails
      // ONLY when Redshift tables are involved in a query plan otherwise it's false signal
      if (foundRedshiftRelation(plan)) {
        sparkContext.dataSourceTelemetry.checkForPushDownFailures.set(true)
      }
      None
    }
  }

  private def foundRedshiftRelation(plan: LogicalPlan): Boolean = {
    plan.collectFirst {
      case LogicalRelation(_: RedshiftRelation, _, _, _) => true
    }.getOrElse(false)
  }
}
