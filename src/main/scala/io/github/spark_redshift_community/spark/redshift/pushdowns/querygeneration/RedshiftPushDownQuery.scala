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
// https://github.com/ActionIQ-OSS/spark-snowflake/blob/spark_2.4/src/main/scala/net/snowflake/spark/snowflake/pushdowns/querygeneration/SnowflakeQuery.scala
// scalastyle:on line.size.limit
package io.github.spark_redshift_community.spark.redshift.pushdowns.querygeneration

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, Cast, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import io.github.spark_redshift_community.spark.redshift._

abstract sealed class RedshiftPushDownQuery {

  /** Output columns. */
  lazy val output: Seq[Attribute] =
    if (helper == null) Seq.empty
    else {
      helper.output
        .map { col =>
          val orig_name = col.name

          Alias(Cast(col, col.dataType), orig_name)(
            col.exprId,
            Seq.empty[String],
            Some(col.metadata)
          )
        }
        .map(_.toAttribute)
    }

  val helper: QueryHelper

  /** What comes after the FROM clause. */
  val suffixStatement: RedshiftPushDownSqlStatement = new RedshiftPushDownSqlStatement()

  def expressionToStatement(expr: Expression): RedshiftPushDownSqlStatement =
    convertStatement(expr, helper.colSet)

  /** Converts this query into a String representing the SQL.
   *
   * @param useAlias Whether or not to alias this translated block of SQL.
   * @return SQL statement for this query.
   */
  def getStatement(useAlias: Boolean = false): RedshiftPushDownSqlStatement = {
    val stmt =
      ConstantString("SELECT") + helper.columns.getOrElse(ConstantString("*") !) + "FROM" +
        helper.sourceStatement + suffixStatement

    if (useAlias) {
      blockStatement(stmt, helper.alias)
    } else {
      stmt
    }
  }

  /** Finds a particular query type in the overall tree.
   *
   * @param query PartialFunction defining a positive result.
   * @tparam T RedshiftPushDownQuery type
   * @return Option[T] for one positive match, or None if nothing found.
   */
  def find[T](query: PartialFunction[RedshiftPushDownQuery, T]): Option[T] =
    query
      .lift(this)
      .orElse(
        if (helper.children.isEmpty) None
        else helper.children.head.find(query)
      )

  /** Determines if two subtrees can be joined together.
   * Should determine if two sources are in same db?
   * set to true temporarily
   *
   * @param otherTree The other tree, can it be joined with this one?
   * @return True if can be joined, or False if not.
   */
  def canJoin(otherTree: RedshiftPushDownQuery): Boolean = {
    true
  }
}

/** The query for a base type (representing a table or view).
 *
 * @constructor
 * @param relation   The base PushDownRelation representing the basic table, view,
 *                   or subquery defined by the user.
 * @param refColumns Columns used to override the output generation for the QueryHelper.
 *                   These are the columns resolved by PushDownRelation.
 * @param alias      Query alias.
 */
case class SourceQueryRedshift(relation: RedshiftRelation,
  refColumns: Seq[Attribute],
  alias: String)
  extends RedshiftPushDownQuery {

  override val helper: QueryHelper = QueryHelper(
    children = Seq.empty,
    projections = None,
    outputAttributes = Some(refColumns),
    alias = alias,
    conjunctionStatement = blockStatementForSourceQuery(
      ConstantString(relation.jdbcOptions.tableOrQuery) + "",
      "redshift_query_alias"
    )
  )

  override def find[T](query: PartialFunction[RedshiftPushDownQuery, T]): Option[T] =
    query.lift(this)
}

/** The query for a filter operation.
 *
 * @constructor
 * @param conditions The filter condition.
 * @param child      The child node.
 * @param alias      Query alias.
 */
case class FilterQueryRedshift(conditions: Seq[Expression],
  child: RedshiftPushDownQuery,
  alias: String,
  fields: Option[Seq[Attribute]] = None)
  extends RedshiftPushDownQuery {

  override val helper: QueryHelper =
    QueryHelper(
      children = Seq(child),
      projections = None,
      outputAttributes = None,
      alias = alias,
      fields = fields
    )

  override val suffixStatement: RedshiftPushDownSqlStatement =
    ConstantString("WHERE") + mkStatement(
      conditions.map(expressionToStatement),
      "AND"
    )
}

/** The query for a projection operation.
 *
 * @constructor
 * @param columns The projection columns.
 * @param child   The child node.
 * @param alias   Query alias.
 */
case class ProjectQueryRedshift(columns: Seq[NamedExpression],
  child: RedshiftPushDownQuery,
  alias: String)
  extends RedshiftPushDownQuery {

  override val helper: QueryHelper =
    QueryHelper(
      children = Seq(child),
      projections = Some(columns),
      outputAttributes = None,
      alias = alias
    )
}

/** The query for a aggregation operation.
 *
 * @constructor
 * @param columns The projection columns, containing also the aggregate expressions.
 * @param groups  The grouping columns.
 * @param child   The child node.
 * @param alias   Query alias.
 */
case class AggregateQueryRedshift(columns: Seq[NamedExpression],
  groups: Seq[Expression],
  child: RedshiftPushDownQuery,
  alias: String)
  extends RedshiftPushDownQuery {

  override val helper: QueryHelper =
    QueryHelper(
      children = Seq(child),
      projections = if (columns.isEmpty) None else Some(columns),
      outputAttributes = None,
      alias = alias
    )

  override val suffixStatement: RedshiftPushDownSqlStatement =
    if (groups.nonEmpty) {
      ConstantString("GROUP BY") +
        mkStatement(groups.map(expressionToStatement), ",")
    } else {
      // SNOW-201486: Insert a limit 1 to ensure that only one row returns if there
      // are no grouping expressions
      ConstantString("LIMIT 1").toStatement
    }

  override def getStatement(useAlias: Boolean): RedshiftPushDownSqlStatement = {
    if (groups.nonEmpty) { super.getStatement(useAlias) } else {
      ConstantString("SELECT * FROM") + blockStatement(super.getStatement(useAlias))
    }
  }
}

/** The query for Sort and Limit operations.
 *
 * @constructor
 * @param limit   Limit expression.
 * @param orderBy Order By expressions.
 * @param child   The child node.
 * @param alias   Query alias.
 */
case class SortLimitQueryRedshift(limit: Option[Expression],
  orderBy: Seq[Expression],
  child: RedshiftPushDownQuery,
  alias: String)
  extends RedshiftPushDownQuery {

  override val helper: QueryHelper =
    QueryHelper(
      children = Seq(child),
      projections = None,
      outputAttributes = None,
      alias = alias
    )

  override val suffixStatement: RedshiftPushDownSqlStatement = {
    val statementFirstPart =
      if (orderBy.nonEmpty) {
        ConstantString("ORDER BY") + mkStatement(
          orderBy.map(expressionToStatement),
          ","
        )
      } else {
        new RedshiftPushDownSqlStatement()
      }

    statementFirstPart + limit
      .map(ConstantString("LIMIT") + expressionToStatement(_))
      .getOrElse(new RedshiftPushDownSqlStatement())
  }
}

/** The query for join operations.
 *
 * @constructor
 * @param left       The left query subtree.
 * @param right      The right query subtree.
 * @param conditions The join conditions.
 * @param joinType   The join type.
 * @param alias      Query alias.
 */
case class JoinQueryRedshift(left: RedshiftPushDownQuery,
  right: RedshiftPushDownQuery,
  conditions: Option[Expression],
  joinType: JoinType,
  alias: String)
  extends RedshiftPushDownQuery {

  val conj: String = joinType match {
    case Inner =>
      // keep the nullability for both projections
      "INNER JOIN"
    case LeftOuter =>
      // Update the column's nullability of right table as true
      right.helper.outputWithQualifier =
        right.helper.nullableOutputWithQualifier
      "LEFT OUTER JOIN"
    case RightOuter =>
      // Update the column's nullability of left table as true
      left.helper.outputWithQualifier =
        left.helper.nullableOutputWithQualifier
      "RIGHT OUTER JOIN"
    case FullOuter =>
      // Update the column's nullability of both tables as true
      left.helper.outputWithQualifier =
        left.helper.nullableOutputWithQualifier
      right.helper.outputWithQualifier =
        right.helper.nullableOutputWithQualifier
      "FULL OUTER JOIN"
    case _ => throw new IllegalArgumentException("cant resolve this join")
  }

  override val helper: QueryHelper =
    QueryHelper(
      children = Seq(left, right),
      projections = Some(
        left.helper.outputWithQualifier ++ right.helper.outputWithQualifier
      ),
      outputAttributes = None,
      alias = alias,
      conjunctionStatement = ConstantString(conj) !
    )

  override val suffixStatement: RedshiftPushDownSqlStatement =
    conditions
      .map(ConstantString("ON") + expressionToStatement(_))
      .getOrElse(new RedshiftPushDownSqlStatement())

  override def find[T](query: PartialFunction[RedshiftPushDownQuery, T]): Option[T] =
    query.lift(this).orElse(left.find(query)).orElse(right.find(query))
}

case class LeftSemiJoinQueryRedshift(left: RedshiftPushDownQuery,
  right: RedshiftPushDownQuery,
  conditions: Option[Expression],
  isAntiJoin: Boolean = false,
  alias: Iterator[String])
  extends RedshiftPushDownQuery {

  override val helper: QueryHelper =
    QueryHelper(
      children = Seq(left),
      projections = Some(left.helper.outputWithQualifier),
      outputAttributes = None,
      alias = alias.next
    )

  val cond: Seq[Expression] =
    if (conditions.isEmpty) Seq.empty else Seq(conditions.get)

  val anti: String = if (isAntiJoin) " NOT " else " "

  override val suffixStatement: RedshiftPushDownSqlStatement =
    ConstantString("WHERE") + anti + "EXISTS" + blockStatement(
      FilterQueryRedshift(
        conditions = cond,
        child = right,
        alias = alias.next,
        fields = Some(
          left.helper.outputWithQualifier ++ right.helper.outputWithQualifier
        )
      ).getStatement()
    )

  override def find[T](query: PartialFunction[RedshiftPushDownQuery, T]): Option[T] =
    query.lift(this).orElse(left.find(query)).orElse(right.find(query))
}

/** The query for union.
 *
 * @constructor
 * @param children Children of the union expression.
 */
case class UnionQueryRedshift(children: Seq[LogicalPlan],
  alias: String,
  outputCols: Option[Seq[Attribute]] = None)
  extends RedshiftPushDownQuery {

  val queries: Seq[RedshiftPushDownQuery] = children.map { child =>
    new QueryBuilder(child).treeRoot
  }

  override val helper: QueryHelper =
    QueryHelper(
      children = queries,
      outputAttributes = Some(queries.head.helper.output),
      alias = alias,
      visibleAttributeOverride =
        Some(queries.foldLeft(Seq.empty[Attribute])((x, y) => x ++ y.helper.output).map(
          a =>
            AttributeReference(a.name, a.dataType, a.nullable, a.metadata)(
              a.exprId,
              Seq[String](alias)
            )))
    )

  override def getStatement(useAlias: Boolean): RedshiftPushDownSqlStatement = {
    val query =
      if (queries.nonEmpty) {
        mkStatement(
          queries.map(c => blockStatement(c.getStatement())),
          "UNION ALL"
        )
      } else {
        new RedshiftPushDownSqlStatement()
      }

    if (useAlias) {
      blockStatement(query, alias)
    } else {
      query
    }
  }

  override def find[T](query: PartialFunction[RedshiftPushDownQuery, T]): Option[T] =
    query
      .lift(this)
      .orElse(
        queries
          .map(q => q.find(query))
          .view
          .foldLeft[Option[T]](None)(_ orElse _)
      )
}

/** Query including a windowing clause.
 *
 * @constructor
 * @param windowExpressions The windowing expressions.
 * @param child             The child query.
 * @param alias             Query alias.
 * @param fields            The fields generated by this query referenceable by the parent.
 */
case class WindowQueryRedshift(windowExpressions: Seq[NamedExpression],
  child: RedshiftPushDownQuery,
  alias: String,
  fields: Option[Seq[Attribute]])
  extends RedshiftPushDownQuery {

  val projectionVector: Seq[NamedExpression] =
    windowExpressions ++ child.helper.outputWithQualifier

  // We need to reorder the projections based on the output vector
  val orderedProjections: Option[Seq[NamedExpression]] =
    fields.map(_.map(reference => {
      val origPos = projectionVector.map(_.exprId).indexOf(reference.exprId)
      projectionVector(origPos)
    }))

  override val helper: QueryHelper =
    QueryHelper(
      children = Seq(child),
      projections = orderedProjections,
      outputAttributes = None,
      alias = alias
    )
}
