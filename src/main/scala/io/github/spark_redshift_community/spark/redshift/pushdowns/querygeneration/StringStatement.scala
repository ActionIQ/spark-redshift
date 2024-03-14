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

import org.apache.spark.sql.catalyst.expressions.{Ascii, Attribute, CaseWhen, Coalesce, Concat, ConcatWs, Expression, IsNotNull, Length, Like, Literal, Lower, Nvl2, Reverse, StringInstr, StringLPad, StringRPad, StringTranslate, StringTrim, StringTrimLeft, StringTrimRight, Substring, Upper}
import io.github.spark_redshift_community.spark.redshift._
import org.apache.spark.sql.types.StringType

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
        val emptyStmt = convertStatement(Literal.default(expr.dataType), fields)
        val defaultFuncStmt = functionStatement(
          expr.prettyName.toUpperCase, children.map(convertStatement(_, fields)))
        children.size match {
          // redshift doesn't allow 0 or 1 argument
          case 0 => emptyStmt
          case 1 => convertStatement(children.head, fields)
          case 2 => defaultFuncStmt
          case _ =>
            // https://docs.aws.amazon.com/redshift/latest/dg/r_CONCAT.html
            // using nested concat instead of `||` b/c it's easier to manage parentheses
            convertStatement(
              Concat(Seq(children.head, Concat(children.tail))), fields
            )
        }

      case ConcatWs(children) =>
        def coalesceIfNullable(e: Expression): Expression = {
          if (e.nullable) Coalesce(Seq(e, Literal.default(expr.dataType))) else e
        }
        val separator = children.head
        val resExpr = if (
        // if separator is null, result is always null
        // e.g concat_ws(null, 'a', 'b') => null
          separator.foldable && separator.nullable && Option(separator.eval()).isEmpty
        ) { Option(Literal(null)) } else {
          Option(children.size match {
            case 1 =>
              if (separator.nullable) {
                // if separator is null, result is always null, else empty string
                // e.g concat_ws(null) => null; concat_ws('a') => ''
                Nvl2(
                  separator,
                  Literal.default(expr.dataType),
                  Literal(null),
                  Literal(null)
                )
              } else { Literal.default(expr.dataType) }

            case 2 =>
              if (separator.nullable) {
                // if separator is null, result is always null
                // e.g concat_ws(null, 'a') => null
                Nvl2(
                  separator,
                  // need Coalesce b/c ConcatWs ignores null inputs and takes not-null inputs
                  // when separator is not null.
                  // e.g concat_ws('-', null) => ''
                  coalesceIfNullable(children(1)),
                  Literal(null),
                  Literal(null)
                )
              } else { coalesceIfNullable(children(1)) }

            // too complicated to deal with trailing separator, not pushing down
            case _ if (children.last.nullable) => null

            case _ =>
              if (children.tail.forall(e => !e.nullable)) {
                // simple case, just concat recursively
                Concat(Seq(
                  children.tail.head, separator, ConcatWs(separator +: children.tail.tail))
                )
              } else {
                // need to deal with possible nulls
                // e.g. concat_ws('-', 'blah', null, 'abc') =>
                // concat(concat('blah', '-'), '', 'abc')) => 'blah-abc'
                val tailSize = children.tail.size
                val cnctTail = children.tail.zipWithIndex.map {
                  case (c, id) if !c.nullable && id != tailSize - 1 =>
                    Concat(Seq(c, separator))
                  case (c, id) if c.nullable && id != tailSize - 1 =>
                    CaseWhen(
                      branches = Seq((IsNotNull(c), Concat(Seq(c, separator)))),
                      elseValue = Literal.default(expr.dataType)
                    )
                  // last element being nullable is matched above
                  case (c, id) if id == tailSize - 1 => c
                }
                Concat(cnctTail)
              }
          }
          )
        }
        resExpr.map(convertStatement(_, fields)).orNull

      case Like(left, right, escapeChar) =>
        convertStatement(left, fields) + "LIKE" + convertStatement(
          right,
          fields
        ) + s"ESCAPE '$escapeChar'"

      case StringInstr(left, right) =>
        // https://docs.aws.amazon.com/redshift/latest/dg/r_POSITION.html
        ConstantString("POSITION") + blockStatement(
          mkStatement(Seq(right, left).map(convertStatement(_, fields)), "IN")
        )

      case Reverse(child) if child.dataType == StringType =>
        functionStatement(
          expr.prettyName.toUpperCase, Seq(convertStatement(child, fields)))

      case _ => null
    })
  }
}
