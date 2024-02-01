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
// https://github.com/ActionIQ-OSS/spark-snowflake/blob/spark_2.4/src/main/scala/net/snowflake/spark/snowflake/pushdowns/querygeneration/package.scala
// scalastyle:on line.size.limit
package io.github.spark_redshift_community.spark.redshift.pushdowns

import org.apache.spark.sql.catalyst.expressions.{
  Alias,
  Attribute,
  Expression,
  NamedExpression
}

import io.github.spark_redshift_community.spark.redshift._

package object querygeneration {
  private[querygeneration] final def blockStatement(
    stmt: RedshiftPushDownSqlStatement
  ): RedshiftPushDownSqlStatement =
    ConstantString("(") + stmt + ")"

  private[querygeneration] final def blockStatement(
    stmt: RedshiftPushDownSqlStatement,
    alias: String
  ): RedshiftPushDownSqlStatement =
    blockStatement(stmt) + "AS" + wrapStatement(alias)

  private[querygeneration] final def blockStatementForSourceQuery(
    stmt: RedshiftPushDownSqlStatement,
    alias: String
  ): RedshiftPushDownSqlStatement =
    stmt + "AS" + wrapStatement(alias)

  /** This adds an attribute as part of a SQL expression, searching in the provided
   * fields for a match, so the subquery qualifiers are correct.
   *
   * @param attr   The Spark Attribute object to be added.
   * @param fields The Seq[Attribute] containing the valid fields to be used in the expression,
   *               usually derived from the output of a subquery.
   * @return A RedshiftPushDownSqlStatement representing the attribute expression.
   */
  private[querygeneration] final def addAttributeStatement(
    attr: Attribute,
    fields: Seq[Attribute]
  ): RedshiftPushDownSqlStatement = {
    fields.find(e => e.exprId == attr.exprId) match {
      case Some(resolved) =>
        qualifiedAttributeStatement(resolved.qualifier, resolved.name)
      case None => qualifiedAttributeStatement(attr.qualifier, attr.name)
    }
  }

  /** Qualifies identifiers with that of the subquery to which it belongs */
  private[querygeneration] final def qualifiedAttribute(
    alias: Seq[String],
    name: String
  ): String = {
    val str =
      if (alias.isEmpty) ""
      else alias.map(wrap).mkString(".") + "."

    str + name
  }

  private[querygeneration] final def qualifiedAttributeStatement(
    alias: Seq[String],
    name: String
  ): RedshiftPushDownSqlStatement =
    ConstantString(qualifiedAttribute(alias, name)) !

  private[querygeneration] final def wrapStatement(
    name: String
  ): RedshiftPushDownSqlStatement =
    ConstantString(name.toUpperCase) !

  private[querygeneration] def renameColumns(
    origOutput: Seq[NamedExpression],
    alias: String
  ): Seq[NamedExpression] = {

    val col_names = Iterator.from(0).map(n => s"COL_$n")

    origOutput.map { expr =>
      val metadata = expr.metadata

      val altName = s"""${alias}_${col_names.next()}"""

      expr match {
        case a@Alias(child: Expression, name: String) =>
          Alias(child, altName)(a.exprId, Seq.empty[String], Some(metadata))
        case _ =>
          Alias(expr, altName)(expr.exprId, Seq.empty[String], Some(metadata))
      }
    }
  }

  private[querygeneration] final def wrap(name: String): String = {
    name.toUpperCase
  }

  private[querygeneration] final def convertStatement(
    expression: Expression,
    fields: Seq[Attribute]
  ): RedshiftPushDownSqlStatement = {
    (expression, fields) match {
      case AggregationStatement(stmt) => stmt
      case BasicStatement(stmt) => stmt
      case BooleanStatement(stmt) => stmt
      case DateStatement(stmt) => stmt
      case MiscStatement(stmt) => stmt
      case NumericStatement(stmt) => stmt
      case StringStatement(stmt) => stmt
      case WindowStatement(stmt) => stmt
      case UnsupportedStatement(stmt) => stmt
      // UnsupportedStatement must be the last CASE
    }
  }

  private[querygeneration] final def convertStatements(
    fields: Seq[Attribute],
    expressions: Expression*
  ): RedshiftPushDownSqlStatement =
    mkStatement(expressions.map(convertStatement(_, fields)), ",")

  final def mkStatement(
    seq: Seq[RedshiftPushDownSqlStatement],
    delimiter: RedshiftPushDownSqlStatement
  ): RedshiftPushDownSqlStatement =
    seq.foldLeft(new RedshiftPushDownSqlStatement()) {
      case (left, stmt) =>
        if (left.isEmpty) stmt else left + delimiter + stmt
    }

  final def mkStatement(
    seq: Seq[RedshiftPushDownSqlStatement],
    delimiter: String = ","
  ): RedshiftPushDownSqlStatement =
    mkStatement(seq, ConstantString(delimiter) !)
}
