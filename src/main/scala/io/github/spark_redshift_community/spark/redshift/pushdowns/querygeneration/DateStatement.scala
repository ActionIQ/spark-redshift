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

import org.apache.spark.sql.catalyst.expressions.{Add, AddMonths, AiqDateToString, AiqDayDiff, AiqStringToDate, Attribute, Cast, ConvertTimezone, DateAdd, DateDiff, DateFormatClass, DateSub, Divide, Expression, Extract, FromUnixTime, Literal, MakeInterval, Month, Multiply, ParseToTimestamp, Quarter, TruncDate, TruncTimestamp, UnixMillis, Year}
import io.github.spark_redshift_community.spark.redshift._
import org.apache.spark.sql.types.{DoubleType, LongType, NullType, StringType, TimestampType}

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

      case ParseToTimestamp(left, Some(fmt), _, None) if fmt.foldable =>
        val fmtExpr = validFmtExpr(fmt)
        functionStatement("TO_TIMESTAMP", Seq(left, fmtExpr).map(convertStatement(_, fields)))

      case ConvertTimezone(sourceTz, targetTz, sourceTs) =>
        functionStatement(
          "CONVERT_TIMEZONE", Seq(sourceTz, targetTz, sourceTs).map(convertStatement(_, fields))
        )
        
      case Extract(field, source, _) if field.foldable =>
        val fieldStr = field.eval().toString
        val sourceStmt = convertStatement(source, fields)
        functionStatement(
          "EXTRACT",
          Seq(ConstantString(fieldStr) + "FROM" + sourceStmt)
        )

      case MakeInterval(years, months, weeks, days, hours, mins, secs, _) =>
        def helper(expr: Expression, dtPart: String): Option[RedshiftPushDownSqlStatement] = {
          val stmt = if (expr.foldable) {
            // handles null
            Option(expr.eval()).map { v => ConstantString(v.toString).toStatement }
          } else { Option(convertStatement(expr, fields)) }

          // e.g. SELECT 2 * INTERVAL '1 SECOND'
          stmt.map(s => s + "*" + s"INTERVAL '1 $dtPart'")
        }
        val totalDays = Add(Multiply(weeks, Literal(7)), days)
        val stmtParts = Seq(
          helper(years, "YEAR"),
          helper(months, "MONTH"),
          helper(totalDays, "DAY"),
          helper(hours, "HOUR"),
          helper(mins, "MINUTE"),
          helper(secs, "SECOND")
        ).collect { case Some(stmt) => stmt }
        blockStatement(mkStatement(stmtParts, "+"))

      case FromUnixTime(sec, _, _) =>
        val intervalExpr = MakeInterval(
          years = Literal.default(NullType),
          months = Literal.default(NullType),
          weeks = Literal.default(NullType),
          days = Literal.default(NullType),
          hours = Literal.default(NullType),
          mins = Literal.default(NullType),
          secs = sec
        )
        ConstantString("TIMESTAMP'epoch' +") + convertStatement(intervalExpr, fields)

      case UnixMillis(child) =>
        // CAST ( EXTRACT ('epoch' from ts) AS BIGINT) * 1000 +
        // CAST ( EXTRACT (ms from ts) AS BIGINT)
        val secsExpr = Multiply(
          Cast(Extract(Literal("'epoch'"), child, Literal.default(LongType)), LongType),
          Literal(1000L)
        )
        val msExpr = Cast(Extract(Literal("ms"), child, Literal.default(LongType)), LongType)
        convertStatement(Add(secsExpr, msExpr), fields)

      case AiqDateToString(ts, fmt, tz) if fmt.foldable =>
        convertStatement(
          DateFormatClass(
            ConvertTimezone(
              Literal("UTC"), tz, FromUnixTime(Divide(Cast(ts, DoubleType), Literal(1000.0)), fmt)
            ),
            validFmtExpr(fmt)
          ),
          fields
        )

      case DateDiff(endDate, startDate) =>
        // https://docs.aws.amazon.com/redshift/latest/dg/r_DATEDIFF_function.html
        functionStatement(
          "DATEDIFF",
          ConstantString("days").toStatement
            +: Seq(startDate, endDate).map(convertStatement(_, fields))
        )

      case AiqStringToDate(tsStr, fmt, tz) if fmt.foldable =>
        val localTsExpr = Cast(
          ParseToTimestamp(tsStr, Option(fmt), StringType),
          TimestampType
        )
        val msExpr = UnixMillis(
          ConvertTimezone(tz, Literal("UTC"), localTsExpr)
        )
        convertStatement(msExpr, fields)

      case DateFormatClass(timestamp, fmt, None) =>
        functionStatement(
          "TO_CHAR",
          Seq(timestamp, fmt).map(convertStatement(_, fields))
        )

      case AiqDayDiff(startTs, endTs, timezoneId) =>
        val Seq(startDt, endDt) = Seq(startTs, endTs).map { ts =>
          ConvertTimezone(
            Literal("UTC"),
            timezoneId,
            FromUnixTime(
              Divide(Cast(ts, DoubleType), Literal(1000.0)),
              Literal.default(NullType)
            )
          )
        }
        convertStatement(DateDiff(endDt, startDt), fields)

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
}
