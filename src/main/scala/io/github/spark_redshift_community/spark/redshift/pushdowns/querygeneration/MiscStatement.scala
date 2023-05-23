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

import org.apache.spark.sql.catalyst.expressions.{Alias, Ascending, Attribute, CaseWhen, Cast, Coalesce, Descending, Expression, If, In, InSet, Literal, MakeDecimal, ScalarSubquery, ShiftLeft, ShiftRight, SortOrder, UnscaledValue}
import org.apache.spark.sql.types.{Decimal, _}
import org.apache.spark.unsafe.types.UTF8String

import io.github.spark_redshift_community.spark.redshift._

/** Extractors for everything else. */
private[querygeneration] object MiscStatement {

  def unapply(
    expAttr: (Expression, Seq[Attribute])
  ): Option[RedshiftPushDownSqlStatement] = {
    val expr = expAttr._1
    val fields = expAttr._2

    Option(expr match {
      case Alias(child: Expression, name: String) =>
        blockStatement(convertStatement(child, fields), name)
      // redshift is ANSI-compliant
      case Cast(child, t, _, _) =>
        getCastType(t) match {
          case Some(cast) =>
            (child.dataType, t) match {
              case (_: DateType | _: TimestampType,
              _: IntegerType | _: LongType | _: FloatType | _: DoubleType | _: DecimalType) => {
                // This exception will not break the connector. It will be caught in
                // QueryBuilder.
                throw new IllegalArgumentException(
                  s"Don't support to convert ${child.dataType} column to $t type"
                  )
              }
              case _ =>
            }
            child.dataType match {
              case (_: BooleanType) =>
                blockStatement(ConstantString("case when")
                  + convertStatement(child, fields) + "then 'True' else 'False' End")
              case _ =>
                ConstantString("CAST") +
                  blockStatement(convertStatement(child, fields) + "AS" + cast)
            }
          case _ => convertStatement(child, fields)
        }
      case If(child, trueValue, falseValue) =>
        ConstantString("IFF") +
          blockStatement(
            convertStatements(fields, child, trueValue, falseValue)
          )

      case In(child, list) =>
        blockStatement(
          convertStatement(child, fields) + "IN" +
            blockStatement(convertStatements(fields, list: _*))
        )

      case InSet(child, hset) =>
        convertStatement(In(child, setToExpr(hset)), fields)

      case MakeDecimal(child, precision, scale, _) =>
        ConstantString("TO_DECIMAL") + blockStatement(
          blockStatement(
            convertStatement(child, fields) + "/ POW(10," +
              IntVariable(Some(scale)) + ")"
          ) + "," + IntVariable(Some(precision)) + "," +
            IntVariable(Some(scale))
        )
      case ShiftLeft(col, num) =>
        ConstantString("BITSHIFTLEFT") +
          blockStatement(convertStatements(fields, col, num))

      case ShiftRight(col, num) =>
        ConstantString("BITSHIFTRIGHT") +
          blockStatement(convertStatements(fields, col, num))

      case SortOrder(child, Ascending, _, _) =>
        blockStatement(convertStatement(child, fields)) + "ASC"
      case SortOrder(child, Descending, _, _) =>
        blockStatement(convertStatement(child, fields)) + "DESC"

      case ScalarSubquery(subquery, _, _, joinCond) if joinCond.isEmpty =>
        blockStatement(new QueryBuilder(subquery).statement)

      case UnscaledValue(child) =>
        child.dataType match {
          case d: DecimalType =>
            blockStatement(
              convertStatement(child, fields) + "* POW(10," + IntVariable(
                Some(d.scale)
              ) + ")"
            )
          case _ => null
        }

      case CaseWhen(branches, elseValue) =>
        ConstantString("CASE") +
          mkStatement(branches.map(conditionValue => {
            ConstantString("WHEN") + convertStatement(conditionValue._1, fields) +
              ConstantString("THEN") + convertStatement(conditionValue._2, fields)
          }
          ), " ") + { elseValue match {
          case Some(value) => ConstantString("ELSE") + convertStatement(value, fields)
          case None => new RedshiftPushDownSqlStatement()
        }} + ConstantString("END")

      case Coalesce(columns) =>
        ConstantString(expr.prettyName.toUpperCase) +
          blockStatement(
            mkStatement(
              columns.map(convertStatement(_, fields)),
              ", "
            )
          )

      case _ => null
    })
  }

  private final def setToExpr(set: Set[Any]): Seq[Expression] = {
    set.map {
      case d: Decimal => Literal(d, DecimalType(d.precision, d.scale))
      case s @ (_: String | _: UTF8String) => Literal(s, StringType)
      case d: Double => Literal(d, DoubleType)
      case e: Expression => e
      case default =>
        // This exception will not break the connector. It will be caught in
        // QueryBuilder.
        throw new IllegalArgumentException(
          s"${default.getClass.getSimpleName} @ MiscStatement.setToExpr"
        )
    }.toSeq
  }

  /** Attempts a best effort conversion from a SparkType
  * to a connector type to be used in a Cast.
  * https://docs.aws.amazon.com/redshift/latest/dg/c_Supported_data_types.html
  */
  private[querygeneration] final def getCastType(t: DataType): Option[String] =
    Option(t match {
      case StringType => "VARCHAR"
      case BinaryType => "BINARY"
      case DateType => "DATE"
      case TimestampType => "TIMESTAMP"
      case d: DecimalType =>
        "DECIMAL(" + d.precision + ", " + d.scale + ")"
      case IntegerType | LongType => "NUMERIC"
      case FloatType | DoubleType => "FLOAT"
      case _ => null
    })

}
