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

import org.apache.spark.sql.catalyst.expressions.{AddMonths, AiqDateToString, AiqStringToDate, Attribute, DateAdd, DateSub, Expression, Literal, Month, Quarter, TruncDate, TruncTimestamp, Year}
import io.github.spark_redshift_community.spark.redshift._

/** Extractor for datetime expressions */
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

      case AiqDateToString(ts, fmt, tz) if fmt.foldable =>
        val unixMsStmt = convertStatement(ts, fields)
        val fmtStmt = convertStatement(validFmtExpr(fmt), fields)
        val tzStmt = convertStatement(tz, fields)
        val utcTsStmt = fromUnixTimeMs(unixMsStmt)
        val tzTsStmt = convertTimezone(utcTsStmt, None, tzStmt)
        formatDatetime(tzTsStmt, fmtStmt)

      case AiqStringToDate(tsStr, fmt, tz) if fmt.foldable =>
        val fmtStmt = convertStatement(validFmtExpr(fmt), fields)
        val utcTzStmt = convertStatement(Literal("UTC"), fields)
        val tzStmt = convertStatement(tz, fields)

        val localTsStmt = strToTimestamp(convertStatement(tsStr, fields), fmtStmt)
        val utcTsStmt = convertTimezone(localTsStmt, Option(tzStmt), utcTzStmt)
        toUnixTimeMs(utcTsStmt)

      case _ => null
    })
  }

  private def validFmtExpr(fmt: Expression): Expression = {
    // https://docs.aws.amazon.com/redshift/latest/dg/r_FORMAT_strings.html
    val validFmt = fmt.eval().toString
      .replaceAll("([^'])T([^'])", "$1\"T\"$2")
      .replaceAll("HH", "HH24")
      .replaceAll("mm", "MI")
      .replaceAll("a", "AM")
      .replaceAll(".sss|.SSS", ".MS")
    Literal(validFmt)
  }

  private def fromUnixTimeMs(
    unixMsStmt: RedshiftPushDownSqlStatement
  ): RedshiftPushDownSqlStatement = {
    // https://stackoverflow.com/a/64656770
    // TIMESTAMP'epoch' + unixMs * INTERVAL'0.001 SECOND'
    ConstantString("TIMESTAMP'epoch' +") + unixMsStmt + "* INTERVAL'0.001 SECOND'"
  }

  private def toUnixTimeMs(tsStmt: RedshiftPushDownSqlStatement): RedshiftPushDownSqlStatement = {
    // CAST(EXTRACT('epoch' from ts) AS BIGINT) * 1000 + EXTRACT(ms from ts)
    castStmt(extractStmt(tsStmt, "'epoch'")) + "* 1000" + "+" + extractStmt(tsStmt, "ms")
  }

  private def convertTimezone(
    tsStmt: RedshiftPushDownSqlStatement,
    fromTz: Option[RedshiftPushDownSqlStatement],
    toTz: RedshiftPushDownSqlStatement
  ): RedshiftPushDownSqlStatement = {
    // https://docs.aws.amazon.com/redshift/latest/dg/CONVERT_TIMEZONE.html
    functionStatement(
      "CONVERT_TIMEZONE",
      fromTz.fold(Seq(toTz, tsStmt))(Seq(_, toTz, tsStmt))
    )
  }

  private def formatDatetime(
    tsStmt: RedshiftPushDownSqlStatement, fmtStmt: RedshiftPushDownSqlStatement
  ): RedshiftPushDownSqlStatement = {
    ConstantString("TO_CHAR") + blockStatement(mkStatement(Seq(tsStmt, fmtStmt)))
  }

  private def strToTimestamp(
    tsStrStmt: RedshiftPushDownSqlStatement, fmtStmt: RedshiftPushDownSqlStatement
  ): RedshiftPushDownSqlStatement = {
    castStmt(functionStatement("TO_TIMESTAMP", Seq(tsStrStmt, fmtStmt)), "TIMESTAMP")
  }

  private def castStmt(
    stmt: RedshiftPushDownSqlStatement, asType: String = "BIGINT"
  ): RedshiftPushDownSqlStatement = {
    functionStatement("CAST", Seq(stmt + "AS" + asType))
  }

  private def extractStmt(
    stmt: RedshiftPushDownSqlStatement, dtPart: String
  ): RedshiftPushDownSqlStatement = {
    functionStatement("EXTRACT", Seq(ConstantString(dtPart) + "FROM" + stmt))
  }
}
