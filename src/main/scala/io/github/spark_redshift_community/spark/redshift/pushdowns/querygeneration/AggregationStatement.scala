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

package io.github.spark_redshift_community.spark.redshift.pushdowns.querygeneration

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._

import io.github.spark_redshift_community.spark.redshift._

private[querygeneration] object AggregationStatement {
  def unapply(
    expAttr: (Expression, Seq[Attribute])
  ): Option[RedshiftPushDownSqlStatement] = {
    val expr = expAttr._1
    val fields = expAttr._2

    expr match {
      case _: AggregateExpression =>
        // Take only the first child, as all of the functions below have only one.
        expr.children.headOption.flatMap(agg_fun => {
          Option(agg_fun match {
            case _: Average | _: Corr | _: CovPopulation | _: CovSample | _: Count |
                 _: Max | _: Min | _: Sum | _: StddevPop | _: StddevSamp |
                 _: VariancePop | _: VarianceSamp =>
              val distinct: RedshiftPushDownSqlStatement =
                if (expr.sql contains "(DISTINCT ") ConstantString("DISTINCT") !
                else new RedshiftPushDownSqlStatement()

              ConstantString(agg_fun.prettyName.toUpperCase) +
                blockStatement(
                  distinct + convertStatements(fields, agg_fun.children: _*)
                )
            case _ =>
              // This exception is not a real issue. It will be caught in
              // QueryBuilder.
              throw new IllegalArgumentException(
                "not supported"
              )
          })
        })
      case _ => None
    }
  }
}
