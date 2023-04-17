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
// https://github.com/ActionIQ-OSS/spark-snowflake/blob/spark_2.4/src/main/scala/net/snowflake/spark/snowflake/pushdowns/querygeneration/QueryBuilder.scala
// scalastyle:on line.size.limit
package io.github.spark_redshift_community.spark.redshift.pushdowns.querygeneration

import scala.reflect.ClassTag
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.types.{StructField, StructType}
import io.github.spark_redshift_community.spark.redshift.{RedshiftPushDownSqlStatement, RedshiftRelation}
import org.apache.spark.sql.catalyst.InternalRow

/** This class takes a Spark LogicalPlan and attempts to generate
 * a query for Snowflake using tryBuild(). Here we use lazy instantiation
 * to avoid building the query from the get-go without tryBuild().
 */

private[querygeneration] class QueryBuilder(plan: LogicalPlan) extends Logging {

  import QueryBuilder.convertProjections

  private final val alias = Iterator.from(0).map(n => s"SUBQUERY_$n")

  lazy val rdd: RDD[InternalRow] = toRDD[InternalRow]
  lazy val tryBuild: Option[QueryBuilder] =
    if (treeRoot == null) None else Some(this)

  lazy val statement: RedshiftPushDownSqlStatement = {
    checkTree()
    treeRoot.getStatement()
  }

  lazy val getOutput: Seq[Attribute] = {
    checkTree()
    treeRoot.output
  }
  /** Finds the SourceQueryRedshift in this given tree. */
  private lazy val source = {
    checkTree()
    treeRoot
      .find {
        case q: SourceQueryRedshift => q
      }
      .getOrElse(
        throw new IllegalArgumentException(
          "Something went wrong: a query tree was generated with no " +
            "SourceQueryRedshift found."
        )
      )
  }
  private[redshift] lazy val treeRoot: RedshiftPushDownQuery = {
    try {
      generateQueries(plan).get
    } catch {
      case e: Exception =>
        log.warn(s"Not able to generate queries: ${e.getMessage}")
        null
    }
  }

  private def toRDD[T: ClassTag]: RDD[InternalRow] = {

    val schema = StructType(
      getOutput
        .map(attr => StructField(attr.name, attr.dataType, attr.nullable))
    )

    source.relation.buildRDDFromSQL(statement, schema)
  }

  private def checkTree(): Unit = {
    if (treeRoot == null) {
      throw new IllegalArgumentException(
        "QueryBuilder's tree accessed without generation."
      )
    }
  }

  private def generateQueries(plan: LogicalPlan): Option[RedshiftPushDownQuery] = {
    plan match {
      case l@LogicalRelation(redshiftRelation: RedshiftRelation, _, _, _) =>
        Some(SourceQueryRedshift(redshiftRelation, l.output, alias.next))

      case UnaryOp(child) =>
        generateQueries(child) map { subQuery =>
          plan match {
            case Filter(condition, _) =>
              FilterQueryRedshift(Seq(condition), subQuery, alias.next)
            case Project(fields, _) =>
              ProjectQueryRedshift(fields, subQuery, alias.next)
            case Aggregate(groups, fields, _) =>
              AggregateQueryRedshift(fields, groups, subQuery, alias.next)
            case Limit(limitExpr, Sort(orderExpr, true, _)) =>
              SortLimitQueryRedshift(Some(limitExpr), orderExpr, subQuery, alias.next)
            case Limit(limitExpr, _) =>
              SortLimitQueryRedshift(Some(limitExpr), Seq.empty, subQuery, alias.next)

            case Sort(orderExpr, true, Limit(limitExpr, _)) =>
              SortLimitQueryRedshift(Some(limitExpr), orderExpr, subQuery, alias.next)
            case Sort(orderExpr, true, _) =>
              SortLimitQueryRedshift(None, orderExpr, subQuery, alias.next)

            case Window(windowExpressions, _, _, _) =>
              WindowQueryRedshift(
                windowExpressions,
                subQuery,
                alias.next,
                if (plan.output.isEmpty) None else Some(plan.output)
              )

            case _ => subQuery
          }
        }

      case BinaryOp(left, right) =>
        generateQueries(left).flatMap { l =>
          generateQueries(right) map { r =>
            plan match {
              // The 5th parameter is new from spark 3.0
              case Join(_, _, joinType, condition) =>
                joinType match {
                  case Inner | LeftOuter | RightOuter | FullOuter =>
                    JoinQueryRedshift(l, r, condition, joinType, alias.next)
                  case LeftSemi =>
                    LeftSemiJoinQueryRedshift(l, r, condition, isAntiJoin = false, alias)
                  case LeftAnti =>
                    LeftSemiJoinQueryRedshift(l, r, condition, isAntiJoin = true, alias)
                  case _ => throw new IllegalArgumentException("can't resolve this join")
                }
            }
          }
        }

      // On Spark 2.4, Union() has 1 parameter only.
      case Union(children) =>
        Some(UnionQueryRedshift(children, alias.next))

      case Expand(projections, output, child) =>
        val children = projections.map { p =>
          val proj = convertProjections(p, output)
          Project(proj, child)
        }
        Some(UnionQueryRedshift(children, alias.next, Some(output)))

      case _ =>
        // This exception is not a real issue. It will be caught in
        // QueryBuilder.
        None
    }
  }

}

private[redshift] object QueryBuilder {

  final def convertProjections(projections: Seq[Expression],
                               output: Seq[Attribute]): Seq[NamedExpression] = {
    projections zip output map { expr =>
      expr._1 match {
        case e: NamedExpression => e
        case _ => Alias(expr._1, expr._2.name)(expr._2.exprId)
      }
    }
  }

  def getRDDFromPlan(
                      plan: LogicalPlan
                    ): Option[(Seq[Attribute], RDD[InternalRow])] = {
    val qb = new QueryBuilder(plan)

    qb.tryBuild.map { executedBuilder =>
      (executedBuilder.getOutput, executedBuilder.rdd)
    }
  }

}
