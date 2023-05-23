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

import org.apache.spark.sql.catalyst.expressions.{Abs, Acos, Asin, Atan, Attribute, Ceil, CheckOverflow, Cos, Cosh, Exp, Expression, Floor, Greatest, Least, Log, Pi, Pow, PromotePrecision, Rand, Round, Sin, Sinh, Sqrt, Tan, Tanh, UnaryMinus}

import io.github.spark_redshift_community.spark.redshift._

/** Extractor for boolean expressions (return true or false). */
private[querygeneration] object NumericStatement {

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
      case _: Abs | _: Acos | _: Cos | _: Tan | _: Tanh | _: Cosh | _: Atan |
          _: Floor | _: Sin | _: Log | _: Asin | _: Sqrt | _: Ceil | _: Sqrt |
          _: Sinh | _: Greatest | _: Least | _: Exp =>
        ConstantString(expr.prettyName.toUpperCase) +
          blockStatement(convertStatements(fields, expr.children: _*))

      case UnaryMinus(child, _) =>
        // Redshift is ANSI-compliant
        ConstantString("-") +
          blockStatement(convertStatement(child, fields))

      case Pow(left, right) =>
        ConstantString("POWER") +
          blockStatement(
            convertStatement(left, fields) + "," + convertStatement(
              right,
              fields
            )
          )

      case PromotePrecision(child) => convertStatement(child, fields)

      case CheckOverflow(child, t, _) =>
        MiscStatement.getCastType(t) match {
          case Some(cast) =>
            ConstantString("CAST") +
              blockStatement(convertStatement(child, fields) + "AS" + cast)
          case _ => convertStatement(child, fields)
        }

      // Spark has resolved PI() as 3.141592653589793
      // Suppose connector can't see Pi().
      case Pi() => ConstantString("PI()") !

      case Rand(seed, hideSeed) =>
        val seedVar = LongVariable(Option(seed).map(_.asInstanceOf[Long]))
        val blockStmt = if (hideSeed) blockStatement(seedVar !, "RANDOM()") else blockStatement(seedVar !)
        ConstantString("RANDOM") + blockStmt

      case Round(child, scale) =>
        ConstantString("ROUND") + blockStatement(
          convertStatements(fields, child, scale)
        )

      case _ => null
    })
  }
}
