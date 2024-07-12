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
import org.apache.spark.DataSourceTelemetryHelpers
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.jdbc.pushdowns.querygeneration.QueryPushdownUnsupportedException

import java.util.concurrent.atomic.AtomicBoolean

/** This class takes a Spark LogicalPlan and attempts to generate
 * a query for Snowflake using tryBuild(). Here we use lazy instantiation
 * to avoid building the query from the get-go without tryBuild().
 */

private[querygeneration] class QueryBuilder(plan: LogicalPlan)
  extends Logging with DataSourceTelemetryHelpers {

  import QueryBuilder.convertProjections

  /**
   * Indicate whether any redshift tables are involved in a query plan.
   */
  private val foundRedshiftRelation: AtomicBoolean = new AtomicBoolean(false)

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
      generateQueries(plan).orNull
    } catch {
      case e: QueryPushdownUnsupportedException =>
        // protects us against noisy logs unrelated to PushDown (can be too noisy)
        if (foundRedshiftRelation.get()) {
          log.info(e.detailedErrorMsg)
        }
        null
      case e: Exception =>
        if (foundRedshiftRelation.get()) {
          log.warn(logEventNameTagger(s"Unable to generate PushDown queries"), e)
        }
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
    val query = plan match {
      case l@LogicalRelation(redshiftRelation: RedshiftRelation, _, _, _) =>
        foundRedshiftRelation.set(true)
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
              case Join(_, _, joinType, condition, _) =>
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

      case Union(children, byName, allowMissingCol, _, _) =>
        if (byName || allowMissingCol) {
          throw new IllegalArgumentException("Union by name not supported by Redshift")
        } else { Some(UnionQueryRedshift(children, alias.next)) }


      case Expand(projections, output, child) =>
        val children = projections.map { p =>
          val proj = convertProjections(p, output)
          Project(proj, child)
        }
        Some(UnionQueryRedshift(children, alias.next, Some(output)))

      case n =>
        // This exception is not a real issue. It will be caught in
        // QueryBuilder.
        throw new QueryPushdownUnsupportedException(
          s"PushDown failed to generate queries for treeNode=${n.nodeName} and tree=${n.toString}",
          "GenerateQueries",
          n.nodeName,
          n.toString
        )
    }
    log.debug(s"Generated PushDown query '$query' from logical plan:\n$plan")
    query
  }

}

private[redshift] object QueryBuilder {

  final def convertProjections(
    projections: Seq[Expression],
    output: Seq[Attribute]
  ): Seq[NamedExpression] = {
    projections zip output map { expr =>
      expr._1 match {
        case e: NamedExpression => e
        case _ => Alias(expr._1, expr._2.name)(expr._2.exprId)
      }
    }
  }

  def getRDDFromPlan(
    plan: LogicalPlan
  ): Option[(Seq[Attribute], RDD[InternalRow], String)] = {
    val qb = new QueryBuilder(plan)

    qb.tryBuild.map { executedBuilder =>
      (executedBuilder.getOutput, executedBuilder.rdd, executedBuilder.statement.toString)
    }
  }

}
