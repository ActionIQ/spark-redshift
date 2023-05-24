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

import org.apache.spark.sql.catalyst.expressions.{Ascii, Attribute, Concat, Expression, Length, Like, Lower, StringLPad, StringRPad, StringTranslate, StringTrim, StringTrimLeft, StringTrimRight, Substring, Upper}

import io.github.spark_redshift_community.spark.redshift._

/** Extractor for boolean expressions (return true or false). */
private[querygeneration] object StringStatement {

  /** Used mainly by QueryGeneration.convertExpression. This matches
  * a tuple of (Expression, Seq[Attribute]) representing the expression to
  * be matched and the fields that define the valid fields in the current expression
  * scope, respectively.
  *
  * @param expAttr A pair-tuple representing the expression to be matched and the
  *                attribute fields.
  * @return An option containing the translated SQL, if there is a match, or None if there
  *         is no match.
  */
  def unapply(
    expAttr: (Expression, Seq[Attribute])
  ): Option[RedshiftPushDownSqlStatement] = {
    val expr = expAttr._1
    val fields = expAttr._2

    Option(expr match {
      case _: Ascii | _: Lower | _: Substring | _: StringLPad | _: StringRPad |
          _: StringTranslate | _: StringTrim | _: StringTrimLeft |
          _: StringTrimRight | _: Substring | _: Upper | _: Length =>
        ConstantString(expr.prettyName.toUpperCase) +
          blockStatement(convertStatements(fields, expr.children: _*))

      case Concat(children) =>
        val rightSide =
          if (children.length > 2) Concat(children.drop(1)) else children(1)
        ConstantString("CONCAT") + blockStatement(
          convertStatement(children.head, fields) + "," +
            convertStatement(rightSide, fields)
        )

      case Like(left, right, escapeChar) =>
        convertStatement(left, fields) + "LIKE" + convertStatement(
          right,
          fields
        ) + s"ESCAPE '$escapeChar'"

      case _ => null
    })
  }
}
