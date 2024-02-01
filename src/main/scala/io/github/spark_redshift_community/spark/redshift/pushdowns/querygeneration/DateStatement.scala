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

import org.apache.spark.sql.catalyst.expressions.{AddMonths, Attribute, DateAdd, DateSub, Expression, Month, Quarter, TruncDate, TruncTimestamp, Year}

import io.github.spark_redshift_community.spark.redshift._

/** Extractor for boolean expressions (return true or false). */
private[querygeneration] object DateStatement {
  // DateAdd's pretty name in Spark is "date_add",
  // the counterpart's name in SF is "DATEADD".
  // And the syntax is some different.
  val DATEADD = "DATEADD"

  def unapply(
    expAttr: (Expression, Seq[Attribute])
  ): Option[RedshiftPushDownSqlStatement] = {
    val expr = expAttr._1
    val fields = expAttr._2

    Option(expr match {
      case DateAdd(startDate, days) =>
        ConstantString(DATEADD) +
          blockStatement(
            ConstantString("day,") +
              convertStatement(days, fields) + "," +
              convertStatement(startDate, fields)
          )

      // To be tested
      case DateSub(startDate, days) =>
        ConstantString(DATEADD) +
          blockStatement(
            ConstantString("day, (0 - (") +
              convertStatement(days, fields) + ") )," +
              convertStatement(startDate, fields)
          )

      // Add AddMonths() support here
      // But, Spark 3.0, it doesn't. For example,
      // On spark 2.3/2.4, "2015-02-28" +1 month -> "2015-03-31"
      // On spark 3.0,     "2015-02-28" +1 month -> "2015-03-28"
      case AddMonths(_, _) => null

      case _: Month | _: Quarter | _: Year |
           _: TruncDate | _: TruncTimestamp =>
        ConstantString(expr.prettyName.toUpperCase) +
          blockStatement(convertStatements(fields, expr.children: _*))

      case _ => null
    })
  }
}
