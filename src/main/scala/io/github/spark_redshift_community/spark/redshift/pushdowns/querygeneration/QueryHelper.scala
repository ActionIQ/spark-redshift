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
// https://github.com/ActionIQ-OSS/spark-snowflake/blob/spark_2.4/src/main/scala/net/snowflake/spark/snowflake/pushdowns/querygeneration/QueryHelper.scala

package io.github.spark_redshift_community.spark.redshift.pushdowns.querygeneration

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, NamedExpression}

import io.github.spark_redshift_community.spark.redshift.RedshiftPushDownSqlStatement

private[querygeneration] case class QueryHelper(
  children: Seq[RedshiftPushDownQuery],
  projections: Option[Seq[NamedExpression]] = None,
  outputAttributes: Option[Seq[Attribute]],
  alias: String,
  conjunctionStatement: RedshiftPushDownSqlStatement = new RedshiftPushDownSqlStatement(),
  fields: Option[Seq[Attribute]] = None,
  visibleAttributeOverride: Option[Seq[Attribute]] = None
) {
  val colSet: Seq[Attribute] =
    if (fields.isEmpty) {
      children.foldLeft(Seq.empty[Attribute])(
        (x, y) => {
          val attrs =
            if (y.helper.visibleAttributeOverride.isEmpty) {
              y.helper.outputWithQualifier
            } else {
              y.helper.visibleAttributeOverride.get
            }

          x ++ attrs
        }
      )
    } else {
      fields.get
    }

  val pureColSet: Seq[Attribute] =
    children.foldLeft(Seq.empty[Attribute])((x, y) => x ++ y.helper.output)

  val processedProjections: Option[Seq[NamedExpression]] = projections
    .map(
      p =>
        p.map(
          e =>
            colSet.find(c => c.exprId == e.exprId) match {
              case Some(a) if e.isInstanceOf[AttributeReference] =>
                AttributeReference(a.name, a.dataType, a.nullable, a.metadata)(
                  a.exprId
                )
              case _ => e
            }
        )
    )
    .map(p => renameColumns(p, alias))

  val columns: Option[RedshiftPushDownSqlStatement] =
    processedProjections.map(
      p => mkStatement(p.map(convertStatement(_, colSet)), ",")
    )

  lazy val output: Seq[Attribute] = {
    outputAttributes.getOrElse(
      processedProjections.map(p => p.map(_.toAttribute)).getOrElse {
        if (children.isEmpty) {
          throw new IllegalArgumentException(
            "Query output attributes must not be empty when it has no children."
          )
        } else {
          children
            .foldLeft(Seq.empty[Attribute])((x, y) => x ++ y.helper.output)
        }
      }
    )
  }

  var outputWithQualifier: Seq[AttributeReference] = output.map(
    a =>
      AttributeReference(a.name, a.dataType, a.nullable, a.metadata)(
        a.exprId,
        Seq[String](alias)
      )
  )

  // For OUTER JOIN, the column's nullability may need to be modified as true
  def nullableOutputWithQualifier: Seq[AttributeReference] = output.map(
    a =>
      AttributeReference(a.name, a.dataType, nullable = true, a.metadata)(
        a.exprId,
        Seq[String](alias)
      )
  )

  val sourceStatement: RedshiftPushDownSqlStatement = {
    if (children.nonEmpty) {
      mkStatement(children.map(_.getStatement(true)), conjunctionStatement)
    } else {
      conjunctionStatement
    }
  }
}
